Feature-parity plan: SE.Hybrid vs the in-box `DefaultHybridCache`
===

Reference: `dotnet/extensions` `main`, `src/Libraries/Microsoft.Extensions.Caching.Hybrid`
(MIT; tests in `test/Libraries/Microsoft.Extensions.Caching.Hybrid.Tests` — inheritable,
covered by `THIRD-PARTY-NOTICES.md`; ported files keep the .NET Foundation header plus a
provenance note, as the envelope fork already does).

> **Status: implemented (2026-11-10).** All thirteen gap rows and implementation-order steps
> 1–4 are done; #13 remains track-only by design, and the two items explicitly deferred to the
> v4 outbound-invalidation hook (tag-timestamp memoization in #7, the deserialized-object memo
> behind the first divergence) stay deferred. The three documented divergences are asserted in
> the ported tests. Suite at completion: 202 tests in the main project (net8.0 + net10.0,
> against live redis) plus 6 mixed-mode byte-compat tests, all green; Release builds clean on
> all four library TFMs.

Gap inventory
---

| # | Area | Stock behaviour | SE.Hybrid today | Action |
|---|------|-----------------|-----------------|--------|
| 1 | **Stampede combining** | `StampedeKey(key, flags)` → shared `StampedeState<TState,T>`; double-checked join; 8-way partitioned sync locks; last-caller-out shared cancellation (`CancelCaller`/ref-count); `QueueUserWorkItem` vs `ExecuteDirectAsync` split on cancellability | absent (headline gap) | Port, adapted (see architecture below) |
| 2 | **Value semantics** | immutable `T` (string / value types / sealed + `[ImmutableObject(true)]`) share one instance; mutable `T` gets a defensive copy per caller by re-deserializing the retained buffer | every read deserializes (mutable-equivalent; fine, but no immutable sharing, no buffer reuse within a stampede) | Port `CacheItem` / `ImmutableCacheItem` / `MutableCacheItem` / `ImmutableTypeCache` + ref-count buffer recycling |
| 3 | **Key/tag validation** | empty/whitespace, max length, control chars (`<= 0x1F`), full unicode encode/decode round-trip of key *and* tags before L2 use | empty + length only | Port `ContainsReservedCharacters` + `ValidateUnicodeCorrectness` |
| 4 | **Flags** | all six honoured; flags participate in stampede key; `SetAsync` forces `DisableLocalCacheRead\|DisableDistributedCacheRead`; post-set hard flags | `DisableDistributedCacheRead/Write`, `DisableUnderlyingData` only | Port via stampede; map `DisableLocalCacheRead`/`Write` → `CommandFlags.CacheOptOut` on the GET (either flag ⇒ opt out; the read/store split is v4 candidate API #3 — until then opting out of both is the conservative mapping). `DisableCompression` accepted as reserved no-op (stock has no compression either) |
| 5 | **Options** | `DefaultEntryOptions`, `LocalCacheExpiration`, `ReportTagMetrics`, `DisableCompression`, `DistributedCacheServiceKey` | subset | Add `ReportTagMetrics`. `LocalCacheExpiration`: **documented divergence** — local lifetime is governed by the server TTL + connection `CachePolicy`; push invalidation removes the staleness-bounding reason for short L1 TTLs (per-call max-age is a v4 candidate if demanded). Keyed-muxer convention replaces `DistributedCacheServiceKey` |
| 6 | **Batch removes** | base-class virtuals (sequential loop) | same | Override: `KeyDeleteAsync(RedisKey[])` for keys; pipelined tag-timestamp SETs for tags — *better* than stock |
| 7 | **Tags** | `TagSet` (0/1/N compact), validation at create, local memo + L2 read-through, `PrefetchTags` overlapping the L2 read | `string[]`, sequential GETs after parse | Port `TagSet`; prefetch wildcard+caller tags concurrently with the entry GET; reuse prefetched results for payload tags. (Memoized timestamps still gated on the v4 outbound-invalidation hook — separate work) |
| 8 | **Post-factory tag race** | if an invalidation lands while the factory runs, bump the entry's creation timestamp (with 1-tick delay if the clock hasn't moved) so the new write isn't ambiguously invalidated | absent | Port (tag reads are CSC-local, so the check is cheap) |
| 9 | **Logging** | `LoggerMessage`-generated, stable event IDs 1–11 | ad-hoc `ILogger` calls | Port `Log.cs` verbatim (same IDs/messages — ops familiarity) |
| 10 | **EventSource** | `Microsoft-Extensions-HybridCache`: 16 events, polling counters | none | Port as `StackExchange-Redis-HybridCache`, same counter names (dashboards port). Mapping note: L1 hit/miss are not observable through `IDatabase` (a locally-served GET looks like any GET) — the outer GET maps to the distributed-cache events; CSC's own counters live on `GetClientCache()` |
| 11 | **Serializer factory** | `DefaultJsonSerializerFactory` honours DI `JsonSerializerOptions` + field-serialization fallback | basic STJ serializer | Port factory behaviour |
| 12 | **TimeProvider** | injected `TimeProvider` (`_clock`) everywhere | static `DateTimeOffset.UtcNow` | Inject `TimeProvider` (envelope-level expiry becomes testable; server TTL obviously stays on the server's clock) |
| 13 | **.NET 11 surface** (`HybridCacheEntryContext` factory overload, `LocalSize`) | gated `RUNTIME_PACKAGES_11_OR_GREATER` | n/a — we reference 10.x abstractions | Track; not parity-blocking. Envelope v2 (`LocalCacheSize`) already parses; we keep writing v1 |

Architecture: where the stampede sits
---

Stock shape: sync L1 check → join/create stampede → stampede reads L2 → factory → L1+L2 write.
Our "L1" is the client cache *inside* the GET, so:

- **Fast path (unchanged from phase 1)**: direct `GET` (`CacheOptIn`) + tag check outside any
  stampede — a CSC-local hit costs no stampede allocation, mirroring stock's alloc-free L1 hit.
- **Miss** → join/create `StampedeState` keyed on `(key, flags)`. The creator passes
  "already missed" evidence so the stampede skips re-reading the backend (stock re-reads L2
  because its outer check was L1-only; our outer check *was* the backend read — re-reading
  would double the round trips on every cold read). The race this widens (a foreign write
  landing between our miss and the factory) is microseconds on top of the inherent
  factory-duration race that stock accepts too: the factory result wins and overwrites.
  Joiners never re-read; they await the shared result.
- **`SetAsync`** = stock pattern: standalone (never registered for joining) `ExecuteDirectAsync`
  with a pass-through factory and forced read-disable flags — reusing validation, tag-race
  handling, serialization, and the write path.
- Buffer lifetime: ref-count port as-is; without an L1 holding a reservation, the serialized
  buffer lives until the last joiner unwraps *and* the backend write completes, then recycles.

Divergences to assert in tests (deliberate, documented)
---

- Immutable types are *not* reference-shared across separate `GetOrCreateAsync` calls (no
  deserialized-object memo until the v4 entry-identity hook); they *are* shared among callers
  joined to one stampede. Semantically invisible for genuinely immutable types.
- `LocalCacheExpiration` is not honoured locally (see #5).
- L1-specific events (`LocalCacheHit`/`Miss`/`Write`) are not emitted (see #10).

Test inheritance (from `Microsoft.Extensions.Caching.Hybrid.Tests`)
---

- **Port**: `StampedeTests` (with the `DebugGetCallerCount` hook), `FunctionalTests`,
  `TypeTests` (immutability semantics; adapt reference-identity asserts per divergence note),
  10-era parts of `FactoryOptionsTests` (batch removes), `TagSetTests`, `SerializerTests`
  (merge with ours), `HybridCacheEventSourceTests` + `TestEventListener`, buffer-release
  assertions folded into stampede tests where the debug hooks allow.
- **Adapt concept only**: `UnreliableL2Tests` (ours: fault-injecting `IDatabase` wrapper),
  `ExpirationTests` (envelope-level expiry via `TimeProvider`; server TTL covered by existing
  live-redis tests), `PayloadTests` (our envelope fork already has targeted tests; port the
  edge cases we lack).
- **Not applicable**: `DistributedCacheTests`/`SqlServerTests`/`RedisTests` (stock L2
  plumbing), `SizeTests` (MemoryCache `SizeLimit`), `LocalInvalidationTests` (L1-specific),
  `L2Tests`/`BufferReleaseTests` internals that inspect `IMemoryCache`.

Implementation order
---

1. Infrastructure port: `BufferChunk`, `RecyclableArrayBufferWriter` (replaces
   `PooledBufferWriter`), `TagSet`, `ImmutableTypeCache(.T)`, `CacheItem` trio, `Log`,
   `HybridCacheEventSource`.
2. Stampede port: `StampedeKey`, `StampedeState(.T)`, sync-lock partitions, dictionary;
   `BackgroundFetchAsync` rewritten for the redis backend (GET/parse/tag-check, factory,
   tag-race fix, serialize-once, SET).
3. `RedisHybridCache` rewire: `GetOrCreateAsync`/`SetAsync` over the stampede; validation
   parity; flags mapping; `TimeProvider`; batch remove overrides; serializer factory parity.
4. Inherited tests, adapted; run full suite against live redis; mixed-mode suite re-run.
5. Docs: design.md status, README.
