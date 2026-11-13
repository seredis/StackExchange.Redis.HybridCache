// Exact DisableLocalCacheRead/Write semantics (issue #5, via SE.Redis CacheNoLocalServe /
// CacheNoLocalStore, 4.0.165-alpha). The round-trip detector is instance identity through the
// result memo (issue #6): a local hit serves the resident envelope's memoized string instance,
// while a genuine round trip parses a fresh envelope and deserializes a fresh string. So Same
// means "served locally" and NotSame means "went to the server" - no counters, no internals.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class LocalCacheFlagsTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public LocalCacheFlagsTests(RedisFixture fx) => _fx = fx;

    private static readonly HybridCacheEntryOptions NoLocalRead = new() { Flags = HybridCacheEntryFlags.DisableLocalCacheRead };
    private static readonly HybridCacheEntryOptions NoLocalWrite = new() { Flags = HybridCacheEntryFlags.DisableLocalCacheWrite };
    private static readonly HybridCacheEntryOptions NoLocal = new() { Flags = HybridCacheEntryFlags.DisableLocalCache };

    private static ServiceProvider CreateProvider(IConnectionMultiplexer muxer, string prefix)
        => new ServiceCollection()
            .AddSingleton(muxer)
            .AddRedisHybridCache(o => o.KeyPrefix = prefix)
            .BuildServiceProvider();

    private static async Task<string> HeatAsync(HybridCacheBase cache, string key)
    {
        // write, remote fetch that populates the local cache, then local hits; returns the
        // memoized instance that proves subsequent serves are local (Same) or not (NotSame)
        var ct = TestContext.Current.CancellationToken;
        _ = await cache.GetOrCreateAsync(key, _ => new ValueTask<string>("payload"), cancellationToken: ct);
        _ = await ReadAsync(cache, key);
        string first = await ReadAsync(cache, key);
        string second = await ReadAsync(cache, key);
        Assert.Equal("payload", first);
        Assert.Same(first, second); // baseline: the memo is engaged and serving locally
        return first;
    }

    private static ValueTask<string> ReadAsync(HybridCacheBase cache, string key, HybridCacheEntryOptions? options = null)
        => cache.GetOrCreateAsync(key, _ => new ValueTask<string>("wrong"), options, cancellationToken: TestContext.Current.CancellationToken);

    [Fact]
    public async Task DisableLocalCacheRead_ForcesRoundTrip_CacheStaysWarm()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string resident = await HeatAsync(cache, "nlr");

        // the flagged read must hit the server even though a fresh local copy exists...
        string forced = await ReadAsync(cache, "nlr", NoLocalRead);
        Assert.Equal("payload", forced);
        Assert.NotSame(resident, forced);

        // ...without costing anyone else the local cache: the resident entry still serves
        string after = await ReadAsync(cache, "nlr");
        Assert.Same(resident, after);
    }

    [Fact]
    public async Task DisableLocalCacheWrite_RoundTripsNeverAdmit()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        var ct = TestContext.Current.CancellationToken;

        // write only; every flagged read on the cold local cache is its own round trip,
        // because the previous one was forbidden from storing
        _ = await cache.GetOrCreateAsync("nlw", _ => new ValueTask<string>("payload"), cancellationToken: ct);
        string first = await ReadAsync(cache, "nlw", NoLocalWrite);
        string second = await ReadAsync(cache, "nlw", NoLocalWrite);
        Assert.Equal("payload", first);
        Assert.Equal("payload", second);
        Assert.NotSame(first, second);

        // plain reads still populate and serve as usual afterwards
        _ = await ReadAsync(cache, "nlw");
        string a = await ReadAsync(cache, "nlw");
        string b = await ReadAsync(cache, "nlw");
        Assert.Same(a, b);
    }

    [Fact]
    public async Task DisableLocalCacheWrite_ServesAnExistingEntry()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string resident = await HeatAsync(cache, "nlw-warm");

        // serving is not storing: a store-disabled read of a warm entry is still a local hit
        string served = await ReadAsync(cache, "nlw-warm", NoLocalWrite);
        Assert.Same(resident, served);
    }

    [Fact]
    public async Task DisableLocalCache_BothFlags_FullBypass()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string resident = await HeatAsync(cache, "nl");

        // both flags: never served locally, never stored - each read is its own round trip
        string first = await ReadAsync(cache, "nl", NoLocal);
        string second = await ReadAsync(cache, "nl", NoLocal);
        Assert.Equal("payload", first);
        Assert.NotSame(resident, first);
        Assert.NotSame(first, second);

        // and the resident entry was left alone throughout
        string after = await ReadAsync(cache, "nl");
        Assert.Same(resident, after);
    }
}
