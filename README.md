SE.Hybrid
===

A [`HybridCache`](https://learn.microsoft.com/aspnet/core/performance/caching/hybrid)
implementation built natively on
[StackExchange.Redis v4 server-assisted client-side caching](https://github.com/StackExchange/StackExchange.Redis/blob/v4/docs/ClientSideCaching.md),
instead of layering the stock implementation over `IDistributedCache`.

Why: the stock pairing keeps **two** uncoordinated in-memory tiers (the multiplexer's client
cache *and* a `MemoryCache` L1), and `IDistributedCache` has no change notification, so key and
tag invalidation never reach other servers' L1. Redis has had the missing mechanism since 6.0
(`CLIENT TRACKING`); building directly on it gives one near cache and fleet-coherent
invalidation, while keeping the parts of the stock implementation worth keeping — the stampede
protection (query combination), the serialization pipeline, and the `HybridCache` contract
itself (from `Microsoft.Extensions.Caching.Abstractions`; swapping in this implementation is a
one-line DI change). Entries are plain redis strings with absolute expiry — sliding expiration
is deliberately unsupported (it is incompatible with client-side caching; the design notes
explain why) — and the payload envelope stays byte-compatible with the default implementation's,
given the same serializer configuration.

> **Status: skeleton.** The public shape and build exist; the implementation does not yet.
> Start with [docs/design.md](docs/design.md) — architecture, the fork map from
> `Microsoft.Extensions.Caching.Hybrid` internals, tag-epoch design, phasing, open questions.

Layout
---

- `src/StackExchange.Redis.HybridCache/` — the library (`RedisHybridCache : HybridCache`,
  options, DI registration)
- `tests/StackExchange.Redis.HybridCache.Tests/` — xunit.v3 on Microsoft.Testing.Platform
- `docs/design.md` — design notes

Building
---

```bash
dotnet build
dotnet test
```

Requires the .NET 10 SDK (see `global.json`). Versioning is Nerdbank.GitVersioning
(`version.json`); packages are produced from `src/`.

License
---

MIT. Portions are derived from
[dotnet/extensions](https://github.com/dotnet/extensions) (`Microsoft.Extensions.Caching.Hybrid`),
also MIT — see [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
