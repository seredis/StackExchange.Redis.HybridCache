// First-use configuration checks: degraded client-cache configurations must work, but log what
// they cost (docs/design.md; issue #1). The check runs once, on the first backend use - never in
// the constructor (DI placeholder pattern).

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis.Caching;
using StackExchange.Redis.HybridCache.Internal;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class ConfigurationCheckTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public ConfigurationCheckTests(RedisFixture fx) => _fx = fx;

    private static ServiceProvider CreateProvider(IConnectionMultiplexer muxer, string prefix, LogCollector logs)
    {
        var services = new ServiceCollection()
            .AddSingleton(muxer)
            .AddRedisHybridCache(o => o.KeyPrefix = prefix);
        services.AddLogging(b => b.AddProvider(logs).SetMinimumLevel(LogLevel.Debug));
        return services.BuildServiceProvider();
    }

    private ConnectionMultiplexer Connect(Action<ConfigurationOptions>? configure = null)
    {
        var options = ConfigurationOptions.Parse($"{_fx.Host},connectTimeout=2000,abortConnect=true");
        configure?.Invoke(options);
        return ConnectionMultiplexer.Connect(options);
    }

    [Fact]
    public async Task NoClientCache_WarnsDistributedOnly_Once()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var logs = new LogCollector();
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix(), logs);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        Assert.Equal(1, await cache.GetOrCreateAsync("first", _ => new ValueTask<int>(1)));
        Assert.Equal(2, await cache.GetOrCreateAsync("second", _ => new ValueTask<int>(2)));

        Assert.Single(logs.Entries, e => e.EventId.Id == Log.IdClientCacheNotAvailable && e.Level == LogLevel.Warning);
    }

    [Fact]
    public async Task TrackingByPrefix_Covered_NoWarnings()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        var logs = new LogCollector();
        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix(), logs);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        Assert.Equal(1, await cache.GetOrCreateAsync("first", _ => new ValueTask<int>(1)));

        Assert.DoesNotContain(logs.Entries, e => e.Level >= LogLevel.Warning);
        Assert.Single(logs.Entries, e => e.EventId.Id == Log.IdClientCacheValidated);
    }

    [Fact]
    public async Task TrackingModeNone_WarnsNoInvalidation()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        using var muxer = Connect(o =>
        {
            o.Protocol = RedisProtocol.Resp3;
            o.ClientCache = new CacheOptions { TrackingMode = CacheTrackingMode.None };
        });
        var logs = new LogCollector();
        await using var provider = CreateProvider(muxer, RedisFixture.NewPrefix(), logs);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        Assert.Equal(1, await cache.GetOrCreateAsync("first", _ => new ValueTask<int>(1)));

        Assert.Single(logs.Entries, e => e.EventId.Id == Log.IdClientCacheNoServerInvalidation && e.Level == LogLevel.Warning);
    }

    [Fact]
    public async Task HardBoundaryMode_PrefixNotCovered_Warns()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        using var muxer = Connect(o =>
        {
            o.Protocol = RedisProtocol.Resp3;
            o.ClientCache = new CacheOptions
            {
                TrackingMode = CacheTrackingMode.Broadcast,
                Prefixes = ["somewhere-else:"],
            };
        });
        var logs = new LogCollector();
        await using var provider = CreateProvider(muxer, RedisFixture.NewPrefix(), logs);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        Assert.Equal(1, await cache.GetOrCreateAsync("first", _ => new ValueTask<int>(1)));

        Assert.Single(logs.Entries, e => e.EventId.Id == Log.IdClientCacheKeyPrefixNotCovered && e.Level == LogLevel.Warning);
    }

    [Fact]
    public async Task HardBoundaryMode_PrefixCovered_NoCoverageWarning()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        using var muxer = Connect(o =>
        {
            o.Protocol = RedisProtocol.Resp3;
            o.ClientCache = new CacheOptions
            {
                TrackingMode = CacheTrackingMode.Broadcast,
                Prefixes = [RedisFixture.TrackedPrefix],
            };
        });
        var logs = new LogCollector();
        await using var provider = CreateProvider(muxer, RedisFixture.NewPrefix(), logs);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        Assert.Equal(1, await cache.GetOrCreateAsync("first", _ => new ValueTask<int>(1)));

        Assert.DoesNotContain(logs.Entries, e => e.EventId.Id == Log.IdClientCacheKeyPrefixNotCovered);
    }

    [Fact]
    public void Construction_DoesNotTriggerCheck()
    {
        // the check must wait for first backend use, like the context itself (DI placeholders)
        var logs = new LogCollector();
        using var provider = CreateProvider(PlaceholderMultiplexer.Instance, RedisFixture.NewPrefix(), logs);
        _ = provider.GetRequiredService<HybridCacheBase>(); // resolving must not touch the muxer

        Assert.DoesNotContain(logs.Entries, e => e.EventId.Id >= 1001 && e.EventId.Id <= 1005);
    }
}
