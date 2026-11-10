using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class RedisHybridCacheTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public RedisHybridCacheTests(RedisFixture fx) => _fx = fx;

    private static ServiceProvider CreateProvider(IConnectionMultiplexer muxer, string prefix)
        => new ServiceCollection()
            .AddSingleton(muxer)
            .AddRedisHybridCache(o => o.KeyPrefix = prefix)
            .BuildServiceProvider();

    private sealed record Poco(string Name, int Value);

    [Fact]
    public async Task StringRoundTrip_FactoryRunsOnce()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(_fx.Connection!, prefix);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        int calls = 0;
        Func<CancellationToken, ValueTask<string>> factory = _ =>
        {
            Interlocked.Increment(ref calls);
            return new("expensive-value");
        };

        Assert.Equal("expensive-value", await cache.GetOrCreateAsync("k1", factory, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("expensive-value", await cache.GetOrCreateAsync("k1", factory, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(1, calls);

        // and it really is a plain string at the prefixed key, with a TTL
        Assert.Equal(RedisType.String, await _fx.Db.KeyTypeAsync(prefix + "k1"));
        var ttl = await _fx.Db.KeyTimeToLiveAsync(prefix + "k1");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));
    }

    [Fact]
    public async Task PocoRoundTrip_ViaJson()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        var stored = await cache.GetOrCreateAsync("p1", _ => new ValueTask<Poco>(new Poco("abc", 42)), cancellationToken: TestContext.Current.CancellationToken);
        var fetched = await cache.GetOrCreateAsync("p1", _ => new ValueTask<Poco>(new Poco("should-not-run", -1)), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(stored, fetched);
        Assert.NotSame(stored, fetched); // defensive: a separate instance per retrieval
    }

    [Fact]
    public async Task Expiration_IsHonoured()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        var options = new HybridCacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(500) };

        int calls = 0;
        Func<CancellationToken, ValueTask<string>> factory = _ => new($"v{Interlocked.Increment(ref calls)}");

        Assert.Equal("v1", await cache.GetOrCreateAsync("e1", factory, options, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("v1", await cache.GetOrCreateAsync("e1", factory, options, cancellationToken: TestContext.Current.CancellationToken));
        await Task.Delay(750, TestContext.Current.CancellationToken);
        Assert.Equal("v2", await cache.GetOrCreateAsync("e1", factory, options, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Remove_InvalidatesEntry()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        int calls = 0;
        Func<CancellationToken, ValueTask<string>> factory = _ => new($"v{Interlocked.Increment(ref calls)}");

        Assert.Equal("v1", await cache.GetOrCreateAsync("r1", factory, cancellationToken: TestContext.Current.CancellationToken));
        await cache.RemoveAsync("r1", TestContext.Current.CancellationToken);
        Assert.Equal("v2", await cache.GetOrCreateAsync("r1", factory, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task RemoveByTag_InvalidatesTaggedEntriesOnly()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        int tagged = 0, untagged = 0;
        Func<CancellationToken, ValueTask<string>> taggedFactory = _ => new($"t{Interlocked.Increment(ref tagged)}");
        Func<CancellationToken, ValueTask<string>> untaggedFactory = _ => new($"u{Interlocked.Increment(ref untagged)}");

        Assert.Equal("t1", await cache.GetOrCreateAsync("tag1", taggedFactory, tags: ["region-a"], cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("u1", await cache.GetOrCreateAsync("tag2", untaggedFactory, cancellationToken: TestContext.Current.CancellationToken));

        await cache.RemoveByTagAsync("region-a", TestContext.Current.CancellationToken);

        Assert.Equal("t2", await cache.GetOrCreateAsync("tag1", taggedFactory, tags: ["region-a"], cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("u1", await cache.GetOrCreateAsync("tag2", untaggedFactory, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task RemoveByWildcard_InvalidatesEverything_IncludingUntagged()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        int calls = 0;
        Func<CancellationToken, ValueTask<string>> factory = _ => new($"v{Interlocked.Increment(ref calls)}");

        Assert.Equal("v1", await cache.GetOrCreateAsync("w1", factory, cancellationToken: TestContext.Current.CancellationToken));
        await cache.RemoveByTagAsync("*", TestContext.Current.CancellationToken);
        Assert.Equal("v2", await cache.GetOrCreateAsync("w1", factory, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ForeignHashEntry_IsLoggedMiss_AndOurWriteReplacesIt()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(_fx.Connection!, prefix);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        // a stock-style (or any foreign) hash at our entry key
        await _fx.Db.HashSetAsync(prefix + "f1", [new HashEntry("data", "whatever")]);

        // mirror of the validated stock contract: logged miss, never a surfaced error
        var value = await cache.GetOrCreateAsync("f1", _ => new ValueTask<string>("from-factory"), cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("from-factory", value);

        // and our SET is type-agnostic: the write-back replaced the hash
        Assert.Equal(RedisType.String, await _fx.Db.KeyTypeAsync(prefix + "f1"));
        Assert.Equal("from-factory", await cache.GetOrCreateAsync("f1", _ => new ValueTask<string>("not-run"), cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task WorksWithClientSideCachingEnabled()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        int calls = 0;
        Func<CancellationToken, ValueTask<string>> factory = _ => new($"v{Interlocked.Increment(ref calls)}");

        // round-trip through the tracked prefix: reads are served (and later cached) below us
        Assert.Equal("v1", await cache.GetOrCreateAsync("c1", factory, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("v1", await cache.GetOrCreateAsync("c1", factory, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("v1", await cache.GetOrCreateAsync("c1", factory, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(1, calls);

        // own-write invalidation (read-your-writes): the delete through the same multiplexer
        // must be observed by the very next read, cached or not
        await cache.RemoveAsync("c1", TestContext.Current.CancellationToken);
        Assert.Equal("v2", await cache.GetOrCreateAsync("c1", factory, cancellationToken: TestContext.Current.CancellationToken));

        // same for tag invalidation: the tag-key write goes through this multiplexer
        Assert.Equal("v3", await cache.GetOrCreateAsync("c2", factory, tags: ["ct"], cancellationToken: TestContext.Current.CancellationToken));
        await cache.RemoveByTagAsync("ct", TestContext.Current.CancellationToken);
        Assert.Equal("v4", await cache.GetOrCreateAsync("c2", factory, tags: ["ct"], cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Reads_ForceAdmission_OnFirstMiss()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);

        // deliberately NOT our recommended shape: plain Tracking (opt-out), no prefixes, default
        // admission — so SE.Redis's doorkeeper holds an unflagged read back until its second miss
        // (cache-second-time). Our reads carry CacheOptIn, which is documented (and here asserted)
        // to force first-miss admission: being a HybridCache entry IS the admission decision.
        var options = ConfigurationOptions.Parse($"{_fx.Host},connectTimeout=2000,abortConnect=true");
        options.Protocol = RedisProtocol.Resp3;
        options.ClientCache = new StackExchange.Redis.Caching.CacheOptions
        {
            TrackingMode = StackExchange.Redis.Caching.CacheTrackingMode.Tracking,
        };
        await using var muxer = await ConnectionMultiplexer.ConnectAsync(options);
        var clientCache = muxer.GetClientCache()!;

        await using var provider = CreateProvider(muxer, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        await cache.SetAsync("adm", "value", cancellationToken: TestContext.Current.CancellationToken);
        long before = clientCache.Stored;
        Assert.Equal("value", await cache.GetOrCreateAsync(
            "adm", _ => new ValueTask<string>("factory-must-not-run"), cancellationToken: TestContext.Current.CancellationToken));

        // the fill is applied as the reply is processed; allow a beat for the counter
        long stored = 0;
        for (int i = 0; i < 20 && (stored = clientCache.Stored - before) == 0; i++)
        {
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }

        Assert.True(stored >= 1, $"expected the single read to be admitted to the client cache; stored delta: {stored}");
    }
}
