SE.Hybrid — design notes
===

> Status: pre-implementation research, 2026-11-10. Everything here is a proposal; the open
> questions at the end are genuinely open.

Goal
---

A `HybridCache` implementation built directly on StackExchange.Redis v4's server-assisted
client-side caching (CSC), replacing the ASP.NET concrete implementation
(`Microsoft.Extensions.Caching.Hybrid.DefaultHybridCache`) rather than sitting on top of it.

The stock pairing — `DefaultHybridCache` over `IDistributedCache` over SE.Redis — has two
structural problems this project removes:

1. **Double in-memory tier.** With CSC enabled, the serialized payload is cached in the
   multiplexer's client cache *and* the deserialized object (or a second copy of the bytes) is
   cached in `DefaultHybridCache`'s `MemoryCache` L1. Two memory budgets, two eviction policies,
   two staleness windows, no coordination.
2. **No first-class invalidation.** `IDistributedCache` has no change notification, so the
   Microsoft implementation documents the gap outright: *"the in-memory cache in other servers
   isn't affected"* by `RemoveAsync`/`RemoveByTagAsync`. `LocalCacheExpiration` is the only bound
   on cross-server staleness. Redis has had exactly the missing mechanism since 6.0
   (`CLIENT TRACKING`), and SE.Redis v4 implements it end-to-end.

What we keep from `DefaultHybridCache` is the part that is orthogonal to the backend and hard to
get right: **stampede protection** (query combination), the **serialization pipeline**, and the
**public contract** (`HybridCache` abstract class, entry options/flags, tags). What we replace is
everything from the L1/L2 split down.

The two halves, as found
---

### The abstract contract (`Microsoft.Extensions.Caching.Abstractions`)

The abstract `HybridCache` class ships in `Microsoft.Extensions.Caching.Abstractions` (not in the
`Microsoft.Extensions.Caching.Hybrid` implementation package), together with
`HybridCacheEntryOptions`, `HybridCacheEntryFlags`, `IHybridCacheSerializer<T>` and
`IHybridCacheSerializerFactory`. So SE.Hybrid references only the abstractions package; consumers
resolving `HybridCache` from DI switch implementations by changing one registration, with no code
changes. Third parties (FusionCache) already do this; it is the intended seam.

Abstract/virtual surface to implement:

- `GetOrCreateAsync<TState, T>(key, state, factory, options?, tags?, ct)` (the `<T>` overload is a
  base-class convenience over it)
- `SetAsync<T>(key, value, options?, tags?, ct)`
- `RemoveAsync(key, ct)` + virtual collection overload
- `RemoveByTagAsync(tag, ct)` + virtual collection overload

### `DefaultHybridCache` internals (dotnet/extensions, MIT)

Source: `src/Libraries/Microsoft.Extensions.Caching.Hybrid/Internal/` in
[dotnet/extensions](https://github.com/dotnet/extensions). Partial-class layout, roughly:

| file(s) | what it does | fate here |
|---|---|---|
| `DefaultHybridCache.Stampede*.cs` (~1050 lines) | In-flight operations keyed by `StampedeKey` (key + partition of the flags); later callers join an existing flow via interlocked ref-count (join only if count can be incremented from non-zero; zero means every caller cancelled and the shared token with them). Factory cancellation fires only when *all* joined callers cancel. L2 write may complete after callers are released. | **Fork, largely intact.** This is the heart of what we keep. |
| `DefaultHybridCache.CacheItem.cs` / `ImmutableCacheItem` / `MutableCacheItem` | What goes in L1: immutable types store the deserialized instance; mutable types store the serialized buffer and deserialize per read (defensive copies), with buffer ref-counting tied to eviction callbacks. | **Fork the concept, redistribute the roles** — see "the deserialized layer" below. |
| `DefaultHybridCache.L2.cs` | `IDistributedCache`/`IBufferDistributedCache` read/write. | **Drop**; replaced by direct SE.Redis calls. |
| `DefaultHybridCache.TagInvalidation.cs`, `TagSet.cs` | Per-tag invalidation timestamps (`ConcurrentDictionary<string, Task<long>>` locally, special keys in L2); an entry is valid iff its creation timestamp is newer than every one of its tags' invalidation timestamps, and the `*` wildcard timestamp. | **Fork `TagSet`; rebuild the timestamp store on redis + CSC** — this is where the design gets strictly better, below. |
| `DefaultHybridCache.Serialization.cs`, `DefaultJsonSerializerFactory`, `InbuiltTypeSerializer`, `ImmutableTypeCache*` | Serializer resolution (per-type, factories, inbuilt `string`/`byte[]`, STJ fallback), immutability detection (`sealed` + `[ImmutableObject(true)]`). | **Fork as-is.** |
| `HybridCachePayload.cs` | L2 envelope: key echo, tags + creation time, flags, checksum — protects against case-insensitive backends, truncation, and carries what tag validation needs. | **Fork byte-for-byte; wire compatibility is a goal** (decided — see interop section). |
| `BufferChunk`, `RecyclableArrayBufferWriter` | Pooled buffer plumbing. | Fork what the stampede/serialization code needs; prefer SE.Redis v4's own lease types (`Lease<byte>`, `ReadOnlyLease<T>`) at the boundary. |
| `HybridCacheEventSource.cs`, `Log.cs` | Metrics/logging. | Fork shape, rename sources. |

### SE.Redis v4 client-side caching (`docs/ClientSideCaching.md` on the v4 branch)

The capabilities that matter for this design:

- **Push invalidation** over RESP3 (`CLIENT TRACKING`); per-multiplexer cache below the API
  surface, so any read path we use is covered. Entries are retired by server push, by local
  writes (*before* the write is sent — read-your-writes is guaranteed through one multiplexer),
  by connection loss (full flush), by `FLUSHDB`/`FLUSHALL`, and by `TimeToLive` as the backstop
  (default 1 minute; always finite — covers unannounced expiry and undetected dead connections).
- **Tracking modes**: `Tracking` (OPTOUT), `TrackingByPrefix` (OPTIN + prefix policy, stored on
  first miss inside the prefixes), `TrackingOptIn` (per-call `CommandFlags.CacheOptIn`, stored on
  first miss), `Broadcast`, `KeyspaceNotifications` (RESP2-capable fallback), `None`; `Default`
  falls back to `KeyspaceNotifications` when tracking is unavailable.
- **Per-call / per-context control**: `CommandFlags.CacheOptIn` / `CacheOptOut`,
  `Context.WithMaxCacheAge(TimeSpan)`, `WithoutCache()`.
- **Admission** (TinyLFU doorkeeper, store on second miss) — bypassed by `CacheOptIn`/prefix
  opt-in, which is what we want: SE.Hybrid's reads are deliberate, so they should always store
  on first miss.
- **Policies**: `TimeToLive`, `RefreshAfter` (read-while-ageing; background re-send of the
  original request), `InvalidationGracePeriod` (read-while-stale after a remote change, one
  background refresh, never applies to own writes).
- **Manual invalidation**: `multiplexer.GetClientCache()` → `Invalidate(key)`, `InvalidateAll()`,
  plus counters (`Count`, `Bytes`, `Stored`, `Evicted`, `Promoted`, `Expired`, `Refreshes`,
  `ServedStale`).
- **Memory**: `MaxBytes`/`MaxEntries` with Clock (approx-LRU) eviction over slabs,
  `MaxPayloadBytes` (1 MiB default) per reply.
- **Connection groups**: one cache per member; own writes stamp every member.

Not currently exposed (candidate SE.Redis API additions, below): an *outbound* invalidation
notification (event/callback when the server invalidates a key), and any notion of entry
identity/version visible to a caller.

Proposed architecture
---

```
caller
  │  GetOrCreateAsync(key, factory, options, tags)
  ▼
stampede layer (forked from DefaultHybridCache; per-process query combination)
  │  miss / join
  ▼
redis read path:  GET <prefix><key> (plain string entry; tracked, CacheOptIn)
  │         ▲
  │         └── SE.Redis client-side cache = the ONLY near cache (serialized payload,
  │             invalidated by server push; RefreshAfter / grace windows apply)
  ▼
redis / factory:  miss ⇒ run factory ⇒ serialize ⇒ SET <prefix><key> <envelope> PX <ms> ⇒ return
```

- **One near cache, not two.** The CSC holds the serialized payload. There is no `IMemoryCache`.
  A local hit costs one dictionary probe in the multiplexer plus a deserialize. Note the honest
  accounting: `DefaultHybridCache` *also* deserializes per read for mutable types (defensive
  copies), so for mutable types this is parity with strictly less memory; only for
  sealed-immutable types does the stock implementation avoid per-hit deserialization — see next
  point.
- **The deserialized layer (immutable types).** Three options, in increasing order of SE.Redis
  coupling:
  1. **None** (phase 1): deserialize per hit, always. Correct, simple, parity-or-better with
     stock for mutable types; a regression only for hot immutable types.
  2. **Identity-keyed memo**: cache `(payload identity) → deserialized instance`, where identity
     comes from the CSC entry (same entry served again ⇒ reuse the instance). Needs a small
     SE.Redis API: expose an entry version/identity (even just the lease exposing "this is the
     same stored buffer") on cached reads. No separate invalidation needed — identity changes
     when the entry does, and the memo entry dies with it (weak table or size-bounded).
  3. **Invalidation-event-driven object cache**: SE.Redis raises per-key invalidation
     notifications; SE.Hybrid maintains its own object map evicted by those events. Most capable
     (also enables "L1 of objects, skip storing the serialized reply" via `CacheOptOut` +
     tracking), most new API, most re-created machinery. Not phase 1.
  Recommendation: ship phase 1; design the SE.Redis hook for option 2 in parallel (it is small
  and generally useful); keep option 3 in the back pocket.
- **Stampede protection** is forked unchanged in spirit: per-process, keyed by
  `(key, flags-partition)`, interlocked join, shared cancellation. The CSC's own
  `RefreshAfter`/`InvalidationGracePeriod` solve a *different* stampede (the read-herd against
  redis for an already-cached entry, refreshed by re-sending the GET); the forked layer solves
  the factory stampede (the herd against the underlying database). They compose: the grace
  window keeps serving while exactly one GET refreshes; if that GET misses, exactly one factory
  runs per process.
- **`RemoveAsync` = `DEL`.** The server pushes invalidation to every tracking client; every
  process's near cache drops the entry. Cross-server invalidation becomes a guarantee instead of
  a documented gap. (Local read-your-writes is immediate — the multiplexer invalidates before
  the DEL is sent; remote processes see it "shortly after", per CSC consistency semantics.)

### Tags: the stock timestamp scheme, made coherent by the client cache

The tag design keeps the stock *scheme* — per-tag invalidation timestamps, entries valid iff
created after every relevant tag's timestamp — because the payload envelope (kept
byte-compatible, below) carries tag names and a creation time and has no room for anything
else. What changes is where the near copy of those timestamps lives and how it is kept fresh:

- Each tag's invalidation time lives at `__SER_HCT__<tag>` (our sentinel — we're not
  Microsoft; stock uses `__MSFT_HCT__`): a plain string holding 8-byte little-endian UTC
  ticks, same value format as stock. `DefaultHybridCache` memoizes its tag timestamps locally
  with no cross-server refresh signal; here the keys fall inside the tracked prefix, so every
  process holds them in its client cache, and `RemoveByTagAsync` writes the key and the
  tracking push drops every cached copy — *tag invalidation is coherent across the whole fleet
  within the push latency*, not "this server plus whoever's L1 expires". Steady-state tag
  validation is a local-memory read; the first read after an invalidation is the only round
  trip, once per process.
- Cost on a miss: N tag-key reads (pipelined individual GETs — tag keys won't share a cluster
  hash slot, so no MGET); all locally cached afterwards.
- **Clock skew is accepted**, as it is in the stock implementation (entry created on server A,
  tag invalidated from B, compared on C). A monotonic per-tag *counter* scheme would remove
  clocks from the correctness argument entirely, but the entry side of the comparison lives in
  the envelope, and envelope compatibility pins us to creation-time-vs-timestamp. (Revisit only
  if envelope compatibility is ever dropped.) Race-safety is the stock behaviour: timestamps
  are captured such that a `RemoveByTag` landing while a factory runs invalidates the new entry.
- Tag-key lifecycle mirrors stock intent: a long absolute TTL (stock uses its max-cache-days
  bound via `_tagInvalidationEntryOptions`), refreshed on write; safe because an expired
  timestamp only ever *under*-invalidates entries that have themselves long expired.

### Representation: plain strings with expiry (sliding expiration deliberately dropped)

The native representation is the simplest thing redis offers: **the entry is a string**
(`SET <prefix><key> <envelope> PX <ms>`, read with `GET`, removed with `UNLINK`). No hash, no
`absexp`/`sldexp` fields. Two findings make this safe and right:

- **HybridCache never speaks sliding expiration.** `HybridCacheEntryOptions` is `Expiration` +
  `LocalCacheExpiration`; every `DistributedCacheEntryOptions` the stock implementation
  constructs (entries and tag keys alike) is `AbsoluteExpirationRelativeToNow`, and it never
  calls `IDistributedCache.Refresh`. The hash layout's sliding machinery is dead weight carried
  for `IDistributedCache`'s general contract.
- **Sliding would be actively broken under client-side caching anyway.** A sliding read is a
  write in disguise (`HMGET` + `EXPIRE`): served from the client cache, the server never sees
  the read and the window never slides (and expiry is not announced — stale until the CSC
  `TimeToLive` backstop); sent to the server, each refresh's `EXPIRE` is a modification that
  push-invalidates every other client's copy of that key — a read-herd becomes an invalidation
  storm, and the near cache can never settle on a hot key. So absolute-only is not a
  compromise; it is the only shape that makes sense here.

Absolute expiry needs no metadata fields: the TTL itself (`PTTL`) carries it, and the envelope
already records creation time + duration internally.

### Interop: envelope-level only (revised from "full wire compat")

Full representation-level compat was considered and dropped along with sliding expiration.
The deciding observation: **mixed formats don't degrade gracefully** — stock `RedisCache` reads
with `HMGET`, which against a native string entry is a `WRONGTYPE` *error*, not a miss, and its
writes (`HSET`) error over a string likewise. A truly mixed fleet on one prefix is therefore
ugly regardless of what we write, and the realistic migration was always "flip the fleet,
separate prefix if cautious, let TTLs age out". What survives, because it is free and useful:

1. **Payload envelope**: fork `HybridCachePayload` byte-for-byte. Format v1/v2: sentinel
   (`0x03`) + version, 2 bytes entropy, 8 bytes creation time (UTC ticks, LE); then varint
   flags, payload size, duration (ticks relative to creation), optional local-size (v2), tag
   count, varint+utf8 key echo, varint+utf8 tags, payload bytes, trailing sentinel+version;
   all bytes must be exhausted. Write v1 unless a local-size override is persisted, like the
   stock writer. This keeps serializer, tag and creation-time semantics aligned with stock and
   enables the migration mode below.
2. **Serialization config surface**: mirror the stock builder's registration shape
   (`AddSerializer<T, TImpl>()`, `AddSerializerFactory<TFactory>()`) over the same abstractions
   interfaces (`IHybridCacheSerializer<T>`, `IHybridCacheSerializerFactory`), so moving an app
   across is a copy-paste of its existing registrations. The builder interface itself
   (`IHybridCacheBuilder`) lives in the implementation package, not abstractions, so SE.Hybrid
   defines its own equivalent.
3. *(optional, phase 4)* **Migration read-fallback**: a mode that, on a native miss, attempts
   the stock hash layout (`HMGET` of `InstanceName`-prefixed keys, envelope in `data`) so a
   freshly flipped fleet starts warm from the old fleet's entries; writes are always native.
   One-directional by design — the old fleet is on its way out.

### Mixed-mode behaviour (validated empirically, not just from source)

Validated 2026-11-10 against the real shipped packages (`Microsoft.Extensions.Caching.Hybrid`
10.11.0 + `Microsoft.Extensions.Caching.StackExchangeRedis` 10.0.13) and a live redis 8, in
`tests/StackExchange.Redis.HybridCache.MixedMode.Tests` (that project deliberately references
only the stock stack). Findings, all asserted by tests:

- **Stock reading a native string entry**: the `HMGET` fails with `WRONGTYPE`;
  `DefaultHybridCache` logs it (`CacheUnderlyingDataQueryFailure`, Error) and **treats it as a
  miss** — the factory runs, nothing is surfaced to the caller. (Source: the L2 fetch's generic
  `catch` sets `result = default` with the comment `// treat as "miss"`; only
  `OperationCanceledException` propagates.)
- **Stock writing back over a native string entry**: the `HSET` fails with `WRONGTYPE`, is
  logged (`CacheBackendWriteFailure`, Error) and swallowed; **the native entry survives
  byte-for-byte**. Consequence, also asserted: the stock side then re-runs the factory on
  *every* call for that key — a degraded old fleet, not a broken one, but factory-per-call plus
  an error log per read is loud and slow. Migration guidance: don't leave stock writers running
  against a prefix the new fleet has taken over; flip the fleet (or change prefix) and let TTLs
  age the rest out.
- **Native `SET` over a stock hash entry succeeds** (`SET` is type-agnostic): our writes always
  win, after which the stock side sees the logged-miss behaviour above.
- **SE.Hybrid must mirror the same contract** (phase 2, to be asserted in the main test
  project once the backend exists): meeting a stock hash entry — or any unparseable/foreign
  value — is a **logged miss, never a surfaced error**, and our `UNLINK`/`SET` paths are
  type-agnostic so removal and overwrite work regardless of what holds the key.

### Entry options and flags, mapped

| contract | mapping |
|---|---|
| `Expiration` | redis TTL on the stored entry (`SET ... PX`). The source of truth; shared by the fleet. |
| `LocalCacheExpiration` | CSC bound: per-read `WithMaxCacheAge(localExpiration)` context (can only tighten the cache-wide `TimeToLive`). Cache-wide default comes from `CachePolicy.TimeToLive`. Note semantics shift: stock = "how long L1 keeps it"; here = "how stale a near-copy I will accept", with push invalidation making the practical staleness far smaller. |
| `DisableLocalCacheRead` | `CommandFlags.CacheOptOut` on the GET (note: also skips storing; CSC cannot currently express "don't read the cache but do store the reply" — acceptable, flag as SE.Redis nice-to-have). |
| `DisableLocalCacheWrite` | `CacheOptOut` likewise (same caveat, mirrored). |
| `DisableDistributedCacheRead/Write` | Skip the redis GET / SET; with both disabled SE.Hybrid degrades to stampede-protection-only, same as stock. |
| `DisableUnderlyingData` | Skip the factory; return default on miss. |
| `DisableCompression` | Honour if/when payload compression is forked. |

### Configuration and DI

```csharp
services.AddRedisHybridCache(options =>
{
    options.KeyPrefix = "hc:";          // also the recommended CSC tracking prefix
    // DefaultEntryOptions, MaximumPayloadBytes, MaximumKeyLength, serializers...
});
```

- Resolves `IConnectionMultiplexer` from DI (or accepts one / a factory on the options).
- **Requires** (validated at startup, loudly): RESP3 + `ClientCache` configured on the
  multiplexer, with the `KeyPrefix` inside `CacheOptions.Prefixes` (⇒ `TrackingByPrefix`:
  first-miss store, opt-in frames, server tracks only what we hold) — or any tracking mode plus
  our per-call `CacheOptIn`. Degraded modes (no CSC ⇒ behave like stock-without-L1;
  `KeyspaceNotifications` fallback) work but log their consequences.
- Serializer registration mirrors the stock builder (`AddSerializer<T, TImpl>()`,
  `AddSerializerFactory<TFactory>()`) against the same abstractions interfaces, so existing
  registrations carry over.

What this needs from SE.Redis v4 (candidate API additions)
---

v4 is **unreleased and ours**, so gaps found here should be surfaced as v4 API work *now*, while
the surface is still soft — this project is partly a deliberate stress-test of the client-side
caching API. Current list:

1. *(phase 2)* **Entry identity on cached reads** — enough to key a deserialized-object memo.
   Smallest viable shape: lease/result exposes an opaque version token stable for the lifetime of
   the stored entry.
2. *(phase 3, maybe never)* **Outbound invalidation notification** on `ClientCache` — per-key
   callback when the server (or a local write) invalidates. Deliberately absent today; would need
   care re: callback cost on the reader path.
3. *(nice-to-have)* Split `CacheOptOut` into read-suppression vs store-suppression for the
   `DisableLocalCacheRead/Write` flags' exact semantics.
4. *(check, probably exists)* Per-call read returning a lease over the payload without copy
   (`ReadOnlyLease<T>` is documented for cache hits — confirm the exact string/GET shape).

None of these block phase 1.

Phasing
---

1. **Skeleton + contract** (this repo, now): project layout, abstractions reference, stub
   `RedisHybridCache`, DI, options; CI; port of the stampede machinery with the redis backend
   stubbed; unit tests against a fake backend.
2. **Redis backend**: native read/write/delete path (plain strings + `PX`), payload envelope,
   serializers, options/flags mapping; tag timestamps; integration tests against real redis
   (mirror SE.Redis test infra), including envelope round-trips against the real
   `HybridCachePayload`. Raise any SE.Redis v4 API gaps found here immediately — v4 is still
   soft.
3. **Perf + the deserialized layer**: benchmarks vs stock `DefaultHybridCache`+SE.Redis CSC;
   entry-identity memo (the SE.Redis hook should be prototyped on the v4 branch during phase 2,
   not after).
4. **Hardening**: cluster, connection groups, `KeyspaceNotifications`/degraded modes, metrics
   parity, docs; the optional migration read-fallback (needs the stock redis hash layout pinned
   against the real Microsoft packages, if and when built).

Decisions so far
---

- **Naming**: `StackExchange.Redis.HybridCache` for package, assembly *and* namespace (the
  namespace segment shadows the `HybridCache` type name inside our own namespace; handled with a
  file-level alias, a nuisance worth the consistency).
- **Sliding expiration: dropped; representation: plain strings with `PX` expiry.** HybridCache
  never expresses sliding, and sliding is actively broken under client-side caching (see the
  representation section). Entries are `GET`/`SET ... PX`/`UNLINK` on plain string keys.
- **Interop**: envelope-level only (fork `HybridCachePayload` byte-for-byte + mirror the
  serializer config surface), not representation-level — mixed formats fail with `WRONGTYPE`
  rather than degrading, so live mixed fleets were never realistic. An optional one-directional
  migration read-fallback (native miss ⇒ try the stock hash layout) is parked in phase 4.
  Consequence of envelope compat: timestamp-based tag validation, clock skew accepted as in
  stock (the envelope has no room for per-tag epochs). Mixed-mode behaviour in both directions
  is validated against the real packages — see the mixed-mode section; headline: each side
  treats the other's entries as a logged miss, never an error, and native writes always win.
- **Tag keys**: `__SER_HCT__<tag>` — our sentinel, not Microsoft's `__MSFT_HCT__` — as plain
  strings, 8-byte LE UTC ticks (stock's value format).
- **TFMs**: `netstandard2.0;net8.0;net10.0`; no explicit net472 for now — revisit if a netfx
  audience shows up (ns2.0 keeps the door open).
- **Licensing/attribution**: dotnet/extensions is MIT; forked files keep their copyright header
  plus a provenance note, and `THIRD-PARTY-NOTICES.md` carries the notice (same pattern SE.Redis
  uses). (Trivially fine in this case — same original author — but kept for hygiene.)

Open questions
---

- **Strong-name signing**: SE.Redis signs; new key vs none for this package.
- **Migration read-fallback** (phase 4, optional): build it at all? If so, pin the exact stock
  `Microsoft.Extensions.Caching.StackExchangeRedis` hash layout (it has shifted across
  versions, including the `IBufferDistributedCache` path) against the real packages.
- **`IDistributedCache`/`IBufferDistributedCache` bridge**: also implement them (cheap, from the
  same backend) so SE.Hybrid can serve code that still asks for `IDistributedCache`? Lean: no,
  out of scope; stay focused. (Note: if ever built, it must be absolute-expiry-only or use its
  own representation — sliding is incompatible with the client cache, per the representation
  section.)

References
---

- ASP.NET HybridCache guide: https://learn.microsoft.com/aspnet/core/performance/caching/hybrid
- `DefaultHybridCache` source (MIT): https://github.com/dotnet/extensions/tree/main/src/Libraries/Microsoft.Extensions.Caching.Hybrid
- SE.Redis v4 client-side caching: `docs/ClientSideCaching.md` on the `v4` branch
- Redis server-assisted client-side caching: https://redis.io/docs/latest/develop/reference/client-side-caching/
