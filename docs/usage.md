Using StackExchange.Redis.HybridCache
===

A [`HybridCache`](https://learn.microsoft.com/aspnet/core/performance/caching/hybrid)
implementation built directly on StackExchange.Redis v4 server-assisted client-side caching.
There is one near cache — the multiplexer's client cache — kept coherent by redis push
invalidation (`CLIENT TRACKING`), so key *and tag* invalidations reach every server's near
cache. Consumers use the standard `HybridCache` abstraction; swapping implementations is a
DI change, not a code change.

Requirements
---

- **StackExchange.Redis 4.x** (currently alpha; client-side caching is v4-only).
- **Redis 6.0+** for `CLIENT TRACKING`; RESP3 (redis 6+, enabled per connection) for push
  invalidation on the same connection. Redis 7.4+ recommended.
- .NET Framework 4.7.2+, .NET Standard 2.0, .NET 8, or .NET 10.

Quick start
---

```bash
dotnet add package StackExchange.Redis.HybridCache --prerelease
```

```csharp
using Microsoft.Extensions.Caching.Hybrid;
using StackExchange.Redis;

var config = ConfigurationOptions.Parse("localhost:6379");
config.Protocol = RedisProtocol.Resp3;
config.ClientCache = new CacheOptions
{
    Prefixes = ["hc:"], // match RedisHybridCacheOptions.KeyPrefix
};
var muxer = await ConnectionMultiplexer.ConnectAsync(config);

services.AddSingleton<IConnectionMultiplexer>(muxer);
services.AddRedisHybridCache(); // KeyPrefix defaults to "hc:"
```

```csharp
public class SomeService(HybridCache cache)
{
    public async Task<Customer> GetCustomerAsync(int id, CancellationToken ct = default)
        => await cache.GetOrCreateAsync(
            $"customer/{id}",
            async token => await LoadFromDatabaseAsync(id, token),
            tags: ["customers"],
            cancellationToken: ct);
}
```

- A read served from the client cache is a local, synchronous, zero-allocation hit — no
  round trip, including the tag checks.
- `RemoveAsync` / `RemoveByTagAsync` invalidate **every** server's near cache via redis push
  invalidation; the special tag `"*"` invalidates everything.
- Stampede protection (query combination) is built in: concurrent callers for the same key
  share one factory execution.

A dedicated connection (recommended)
---

Client-side caching is a *connection-level* feature: one client cache per multiplexer,
cache-wide tracking configuration, and a flush on disconnect. A dedicated multiplexer for the
cache keeps that coupling away from your application's general-purpose connection. Register it
keyed with `typeof(HybridCache)` and it is preferred automatically:

```csharp
services.AddKeyedSingleton<IConnectionMultiplexer>(typeof(HybridCache), (_, _) => cacheMuxer);
services.AddRedisHybridCache(options => options.KeyPrefix = "hc:");
```

An unkeyed `IConnectionMultiplexer` registration is the fallback when no keyed one exists.

Configuring tracking
---

`CacheOptions.Prefixes` should cover `RedisHybridCacheOptions.KeyPrefix` (entry keys and tag
keys both live under it). With prefixes configured, the default tracking mode resolves to
`TrackingByPrefix`: reads under the prefix are tracked and the value is stored on the first
miss. The library additionally passes `CacheOptIn` on its reads, so first-miss admission
holds in every tracking mode.

`CacheOptions.DefaultPolicy` (a `CachePolicy`) governs the near cache's local behaviour:

- `TimeToLive` (default **1 minute**): how long a locally-cached copy may be used before
  being refetched, *independent of invalidation* — with push invalidation active this is a
  safety net, not the coherence mechanism.
- `RefreshAfter` / `InvalidationGracePeriod`: background refresh of hot entries and
  serve-stale-briefly-on-invalidation; both off by default.

The multiplexer's cache is observable via `muxer.GetClientCache()`: effective tracking mode
(after any runtime fallback), stored/evicted/served counters, and manual
`Invalidate`/`InvalidateAll`.

### First-use validation

On first use, the cache inspects the connection and logs (once) what it found, so a
misconfigured deployment is loud rather than silently slow or silently stale:

| Event id | Level | Meaning |
|---|---|---|
| 1001 `ClientCacheNotAvailable` | Warning | No client cache configured/enabled: the cache works, but every read is a round trip (distributed-only). |
| 1002 `ClientCacheNoServerInvalidation` | Warning | Tracking mode `None`: local copies (including tag timestamps) can be stale for up to `CachePolicy.TimeToLive`. |
| 1003 `ClientCacheKeyspaceNotifications` | Information | Invalidation is via keyspace notifications (e.g. RESP2 fallback): at-most-once delivery, flush on reconnect. |
| 1004 `ClientCacheKeyPrefixNotCovered` | Warning | `KeyPrefix` is outside `CacheOptions.Prefixes` under a mode where prefixes are a hard boundary (`Broadcast`/`KeyspaceNotifications`/`None`): nothing is near-cached. |
| 1005 `ClientCacheValidated` | Debug | Tracking is active and the configuration checks out. |

Options
---

```csharp
services.AddRedisHybridCache(options =>
{
    options.KeyPrefix = "hc:";              // prefix for entry AND tag keys (default "hc:")
    options.MaximumPayloadBytes = 1 << 20;  // reject larger serialized payloads (default 1 MiB)
    options.MaximumKeyLength = 1024;        // characters (default 1024)
    options.DefaultEntryOptions = new HybridCacheEntryOptions
    {
        Expiration = TimeSpan.FromMinutes(5),
        LocalCacheExpiration = TimeSpan.FromMinutes(1),
    };
});
```

Per-entry `HybridCacheEntryOptions`:

- `Expiration`: absolute lifetime of the entry in redis (default 5 minutes, mirroring the
  stock implementation). Expiry is always absolute — **sliding expiration is deliberately
  unsupported** (it is incompatible with client-side caching; `docs/design.md` explains why).
- `LocalCacheExpiration`: the maximum age of a *locally-served* copy for that read (resolved
  as `LocalCacheExpiration ?? Expiration` within the options object). With push invalidation,
  coherence does not depend on this; it matters as a staleness bound when invalidation is
  degraded. Applied only when tighter than the backend expiration; bounded local hits remain
  zero-allocation. Note the semantics are read-time ("how stale a near copy I will accept"),
  not store-time ("how long the near cache keeps it").
- `Flags`: all standard flags are honoured. `DisableLocalCacheRead`/`Write` map to opting the
  read out of the client cache (the read/store split awaits a finer-grained SE.Redis API);
  `DisableCompression` is accepted as a reserved no-op (as in the stock implementation).

Serialization
---

Identical surface to the stock implementation: `string` and `byte[]` are handled inbuilt;
everything else defaults to `System.Text.Json`, honouring a DI-registered
`JsonSerializerOptions`. Override per type or globally:

```csharp
services.AddSingleton<IHybridCacheSerializer<Customer>>(new MyCustomerSerializer());
services.AddSingleton<IHybridCacheSerializerFactory, MySerializerFactory>();
```

Existing `HybridCache` serializer packages (e.g. `protobuf-net.HybridCache`) work unmodified.
The payload envelope is byte-compatible with the stock implementation's, given the same
serializer configuration — see the interop notes in `docs/design.md`.

Telemetry
---

- `ILogger` events use the stock event ids 1–11 (SE-specific events from 1000, table above).
- `EventSource` **`StackExchange-Redis-HybridCache`** mirrors the stock
  `Microsoft-Extensions-HybridCache` counters (same counter names, so dashboards port):
  local/distributed hits and misses, data queries, writes, stampede joins, tag invalidations.
  `LocalCacheWrite` is not emitted — admission into the client cache happens inside
  StackExchange.Redis; use `GetClientCache()`'s counters for that side.
- `muxer.GetClientCache()` exposes the near cache itself: entries stored/evicted, stale
  serves, and the effective tracking mode.

Behavioural notes
---

- Entries are plain redis strings (`SET`/`PX`, `GET`, `UNLINK`) under `KeyPrefix`; tag
  timestamps are strings under `{KeyPrefix}__SER_HCT__{tag}`. No hash layout, no `SCAN`.
- Tag invalidation writes a timestamp key; reads validate entries against their tags from the
  client cache, so validation is local and the invalidation is push-coherent fleet-wide.
- Values that stock `HybridCache` would share by reference across calls (immutable types) are
  shared among callers joined to one stampede, but re-deserialized across separate calls
  (reference identity across calls awaits an SE.Redis entry-identity hook; semantically
  invisible for genuinely immutable types).
- Running alongside the stock `HybridCache`+`IDistributedCache` pairing on the same keyspace:
  a foreign-format entry is treated as a logged miss and overwritten on write — the same
  behaviour the stock implementation exhibits toward ours.
