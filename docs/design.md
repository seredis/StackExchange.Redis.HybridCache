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
redis read path:  tracked read of the entry (compat representation, below), CacheOptIn
  │         ▲
  │         └── SE.Redis client-side cache = the ONLY near cache (serialized payload,
  │             invalidated by server push; RefreshAfter / grace windows apply)
  ▼
redis / factory:  miss ⇒ run factory ⇒ serialize ⇒ store + TTL (compat representation) ⇒ return
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

**Decision: wire compatibility with the default implementation is a goal** (same serializer
configured ⇒ mixed fleets can share entries during migration). That pins the tag design to the
stock scheme — per-tag invalidation *timestamps*, entries valid iff created after every relevant
tag's timestamp — rather than anything bespoke. What changes is where the near copy of those
timestamps lives and how it is kept fresh:

- `DefaultHybridCache` stores each tag's invalidation time in L2 at `__MSFT_HCT__<tag>`
  (8-byte little-endian UTC ticks) and memoizes it locally with no cross-server refresh signal.
- Here, those same keys fall inside the tracked prefix, so every process holds the timestamp in
  its client cache; `RemoveByTagAsync` writes the key and the tracking push drops every cached
  copy — *tag invalidation is coherent across the whole fleet within the push latency*, not
  "this server plus whoever's L1 expires". Steady-state tag validation is a local-memory read;
  the first read after an invalidation is the only round trip, once per process.
- Cost on a miss: N tag-key reads (pipelined individual GETs — tag keys won't share a cluster
  hash slot, so no MGET); all locally cached afterwards.
- **Clock skew is accepted**, as it is in the stock implementation (entry created on server A,
  tag invalidated from B, compared on C). A monotonic per-tag *counter* scheme would remove
  clocks from the correctness argument entirely and was the original sketch here, but it cannot
  be read or honoured by `DefaultHybridCache` instances sharing the data, so interop wins.
  (Revisit only if interop is ever dropped.) Race-safety is the stock behaviour: timestamps are
  captured such that a `RemoveByTag` landing while a factory runs invalidates the new entry.
- Tag-key lifecycle follows the stock implementation (it writes them with its
  `_tagInvalidationEntryOptions`; match whatever TTL that implies so neither side resurrects the
  other's tags).

### Interop with the default implementation (decided: yes)

Mixed fleets — some servers on `DefaultHybridCache` + redis `IDistributedCache`, some on
SE.Hybrid — should read and write each other's entries, given the same serializer
configuration. Three layers must match:

1. **Payload envelope**: fork `HybridCachePayload` byte-for-byte. Format v1/v2: sentinel
   (`0x03`) + version, 2 bytes entropy, 8 bytes creation time (UTC ticks, LE); then varint
   flags, payload size, duration (ticks relative to creation), optional local-size (v2), tag
   count, varint+utf8 key echo, varint+utf8 tags, payload bytes, trailing sentinel+version;
   all bytes must be exhausted. Write v1 unless a local-size override is persisted, like the
   stock writer.
2. **Tag timestamp keys**: `__MSFT_HCT__<tag>`, 8-byte LE UTC ticks — see the tags section.
3. **The redis representation `Microsoft.Extensions.Caching.StackExchangeRedis` uses**: keys
   prefixed with `InstanceName`, and each entry stored as a *hash* (`absexp`/`sldexp`/`data`
   fields, with the envelope in `data`) plus a key TTL — verify the exact current layout
   (it has shifted across versions, including the `IBufferDistributedCache` path) and pin it
   with round-trip tests against the real packages. `RedisHybridCacheOptions.KeyPrefix`
   therefore plays the role of, and must match, `InstanceName` (default: empty, matching
   `RedisCacheOptions`). Hash reads (`HMGET`) are tracked and invalidated like any other keyed
   read, so none of this costs the client cache anything.
4. **Serialization config surface**: mirror the stock builder's registration shape
   (`AddSerializer<T, TImpl>()`, `AddSerializerFactory<TFactory>()`) over the same abstractions
   interfaces (`IHybridCacheSerializer<T>`, `IHybridCacheSerializerFactory`), so the "same
   serializer configured" precondition is a copy-paste of the existing registrations. The
   builder interface itself (`IHybridCacheBuilder`) lives in the implementation package, not
   abstractions, so SE.Hybrid defines its own equivalent.

Interop test matrix (phase 2): stock-writes/SE.Hybrid-reads and the reverse, for plain entries,
tagged entries, `RemoveAsync`, `RemoveByTagAsync` from each side, and the `*` wildcard.

### Entry options and flags, mapped

| contract | mapping |
|---|---|
| `Expiration` | redis TTL on the stored entry (and `absexp` in the compat representation). The source of truth; shared by the fleet. |
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
    options.KeyPrefix = "myapp:";       // = the IDistributedCache InstanceName for interop;
                                        // also the recommended CSC tracking prefix
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
2. **Redis backend**: read/write/delete path in the compat representation, payload envelope,
   serializers, options/flags mapping; tag timestamps; integration tests against real redis
   (mirror SE.Redis test infra) including the interop matrix against the real Microsoft
   packages. Raise any SE.Redis v4 API gaps found here immediately — v4 is still soft.
3. **Perf + the deserialized layer**: benchmarks vs stock `DefaultHybridCache`+SE.Redis CSC;
   entry-identity memo (the SE.Redis hook should be prototyped on the v4 branch during phase 2,
   not after).
4. **Hardening**: cluster, connection groups, `KeyspaceNotifications`/degraded modes, metrics
   parity, docs.

Decisions so far
---

- **Naming**: `StackExchange.Redis.HybridCache` for package, assembly *and* namespace (the
  namespace segment shadows the `HybridCache` type name inside our own namespace; handled with a
  file-level alias, a nuisance worth the consistency).
- **Interop**: wire-compatible with the default implementation (payload envelope, tag timestamp
  keys, redis representation, serializer config surface) — see the interop section. Consequence:
  timestamp-based tag validation, clock skew accepted as in stock.
- **TFMs**: `netstandard2.0;net8.0;net10.0`; no explicit net472 for now — revisit if a netfx
  audience shows up (ns2.0 keeps the door open).
- **Licensing/attribution**: dotnet/extensions is MIT; forked files keep their copyright header
  plus a provenance note, and `THIRD-PARTY-NOTICES.md` carries the notice (same pattern SE.Redis
  uses). (Trivially fine in this case — same original author — but kept for hygiene.)

Open questions
---

- **Strong-name signing**: SE.Redis signs; new key vs none for this package.
- **Exact current `Microsoft.Extensions.Caching.StackExchangeRedis` layout** (hash fields,
  `IBufferDistributedCache` path, sliding-expiration touch behaviour) — pin with round-trip
  tests against the real packages before freezing the backend.
- **`IDistributedCache`/`IBufferDistributedCache` bridge**: also implement them (cheap, from the
  same backend) so SE.Hybrid can serve code that still asks for `IDistributedCache`? Lean: no,
  out of scope; stay focused.

References
---

- ASP.NET HybridCache guide: https://learn.microsoft.com/aspnet/core/performance/caching/hybrid
- `DefaultHybridCache` source (MIT): https://github.com/dotnet/extensions/tree/main/src/Libraries/Microsoft.Extensions.Caching.Hybrid
- SE.Redis v4 client-side caching: `docs/ClientSideCaching.md` on the `v4` branch
- Redis server-assisted client-side caching: https://redis.io/docs/latest/develop/reference/client-side-caching/
