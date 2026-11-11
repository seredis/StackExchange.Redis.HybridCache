// LocalCacheExpiration, honoured via memoized WithMaxCacheAge contexts (issue #2): a locally
// cached copy older than the effective local expiration is refetched rather than served. The
// observable case needs an invalidation the client cache cannot see, so these tests use a
// TrackingMode.None connection (no server-assisted invalidation) and a foreign write.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis.Caching;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class LocalCacheExpirationTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public LocalCacheExpirationTests(RedisFixture fx) => _fx = fx;

    private static ServiceProvider CreateProvider(IConnectionMultiplexer muxer, string prefix)
        => new ServiceCollection()
            .AddSingleton(muxer)
            .AddRedisHybridCache(o => o.KeyPrefix = prefix)
            .BuildServiceProvider();

    [Fact]
    public async Task LocalCacheExpiration_BoundsStaleness_WhereInvalidationCannot()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        // a cache whose local copies are never invalidated by the server...
        var config = ConfigurationOptions.Parse($"{_fx.Host},connectTimeout=2000,abortConnect=true");
        config.Protocol = RedisProtocol.Resp3;
        config.ClientCache = new CacheOptions { TrackingMode = CacheTrackingMode.None };
        using var muxer = ConnectionMultiplexer.Connect(config);

        string prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(muxer, prefix);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        // ...and a second, unrelated writer
        await using var foreignProvider = CreateProvider(_fx.Connection!, prefix);
        var foreign = foreignProvider.GetRequiredService<HybridCacheBase>();

        var ct = TestContext.Current.CancellationToken;
        string key = "lce";
        Assert.Equal(1, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(1), cancellationToken: ct));

        // populate the local copy (writes don't; reads do), then overwrite from outside
        Assert.Equal(1, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(-1), cancellationToken: ct));
        await foreign.SetAsync(key, 2, cancellationToken: ct);

        // without a local bound the stale copy is served (mode None: nothing invalidated it)
        Assert.Equal(1, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(-1), cancellationToken: ct));

        // with a tighter local bound, a copy older than the bound is refetched from the server
        var bounded = new HybridCacheEntryOptions
        {
            Expiration = TimeSpan.FromMinutes(5),
            LocalCacheExpiration = TimeSpan.FromMilliseconds(100),
        };
        await Task.Delay(500, ct); // comfortably past the bound (the cache age clock is ~16ms coarse)
        Assert.Equal(2, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(-1), bounded, cancellationToken: ct));
    }

    [Fact]
    public async Task LocalCacheExpiration_FreshCopyStillServedLocally()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        var ct = TestContext.Current.CancellationToken;
        var bounded = new HybridCacheEntryOptions
        {
            Expiration = TimeSpan.FromMinutes(5),
            LocalCacheExpiration = TimeSpan.FromSeconds(30),
        };

        string key = "lce-fresh";
        int calls = 0;
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(42, await cache.GetOrCreateAsync(
                key,
                _ =>
                {
                    calls++;
                    return new ValueTask<int>(42);
                },
                bounded,
                cancellationToken: ct));
        }

        Assert.Equal(1, calls); // a fresh bounded copy is still a hit
    }
}
