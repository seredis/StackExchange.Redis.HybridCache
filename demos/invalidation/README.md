# Active vs passive cache invalidation

Two `HybridCache` implementations, one demo: the in-box
[`HybridCache`](https://learn.microsoft.com/aspnet/core/performance/caching/hybrid) over
`IDistributedCache`-on-redis, and `StackExchange.Redis.HybridCache` built on
[client-side caching](https://seredis.dev/ClientSideCaching). Both run the **same application
code** — two "web servers", each with its own near cache, sharing one redis and one database —
and the same scripted scenario. The only file that differs between the two projects is
`Setup.cs`:

| | |
|---|---|
| `StockHybrid/Setup.cs` | `AddHybridCache` + `AddStackExchangeRedisCache`: a `MemoryCache` near cache per server, **no invalidation channel** — a foreign change is seen when the local copy *expires* (passive) |
| `RedisHybrid/Setup.cs` | `AddRedisHybridCache`: the multiplexer's client cache is the near cache, and redis **pushes invalidations** into it (`CLIENT TRACKING`, RESP3) — a foreign change evicts the local copy immediately (active) |
| `Shared/Usage.cs` | the application's cache consumption — identical, verbatim, for both |
| `Shared/Program.cs`, `Scenario.cs`, `Database.cs` | demo machinery, also shared verbatim |

Both setups use the same entry options (`Expiration` 5 min, `LocalCacheExpiration` 10 s); what
changes is the invalidation transport, not the policy.

## Run

Requires Docker (compose plugin) and the .NET 10 SDK.

```bash
docker compose up -d --wait     # one redis 8.2 on host port 16412
dotnet run --project StockHybrid
dotnet run --project RedisHybrid
```

## What you see

Three scenarios; each measures how long **server B** keeps serving a value that server A has
already changed:

1. **update** — server A writes a new price through the cache (`SetAsync`);
2. **tag invalidation** — the price list changes; server A calls `RemoveByTagAsync("catalog")`
   and keeps reading (so its next read re-populates redis);
3. **tag invalidation, invalidator quiet** — the same, but server A goes silent afterwards:
   nothing re-populates redis, so the invalidation itself is server B's only way to find out.

Measured output (one run, this machine):

```
                                           stock (passive)      SE.Redis (active)
                                          server A  server B    server A  server B
1. update (SetAsync)                        0.00 s    9.03 s      0.00 s    0.00 s
2. tag invalidation                         0.02 s   10.00 s      0.02 s    0.02 s
3. tag invalidation, invalidator quiet     (quiet)    > 15 s     (quiet)    0.02 s
```

## Why

- **Stock, scenarios 1–2**: nothing tells server B's `MemoryCache` anything. B serves its local
  copy until that copy's `LocalCacheExpiration` passes, then refetches from redis. The ~9–10 s
  is exactly that setting: staleness is a knob you trade against backend traffic (10 s stale
  ⇔ a round trip per entry per 10 s, per server).
- **Stock, scenario 3** is worse than the knob suggests: server B's copy *did* expire during
  the wait, and B *did* refetch from redis — and still served the old value. `RemoveByTagAsync`
  doesn't delete entries; it writes a tag timestamp that readers compare against, and each
  stock process memoizes a tag's timestamp the first time it needs it, permanently
  (`DefaultHybridCache.TagInvalidation.cs`: `_tagInvalidationTimes` is only updated by a
  *local* `RemoveByTagAsync`). A tag invalidated elsewhere is invisible until something writes
  a *fresh entry* for B to fetch — in scenario 2 that was server A's next read; here, nothing —
  so B stays stale until the entry itself leaves redis (`Expiration`, 5 minutes).
- **SE.Redis, all three**: both the entries and the tag-timestamp keys live in the client
  cache, and redis pushes an invalidation for any of them the moment they change — a foreign
  `SetAsync` evicts B's entry copy, a foreign `RemoveByTagAsync` evicts B's tag copy. B's next
  read notices within its poll interval (~20 ms of it is the round trip to refetch).

The same `LocalCacheExpiration` is configured in both setups: in the stock setup it is the
*mechanism*; in the SE.Redis setup it is a safety net behind the push channel (see the
[usage docs](../../docs/usage.md) for the degraded modes where it matters).

> This demo lives in the SE.Hybrid repo while `StackExchange.Redis.HybridCache` has no
> published package (`RedisHybrid` uses a project reference); it is destined for the
> [demos](https://github.com/seredis/demos) repo once the package exists. The two flavors are
> separate projects out of necessity as well as neatness: the stock L2 provider pins
> StackExchange.Redis 2.x, which cannot share a process with the v4 client.
