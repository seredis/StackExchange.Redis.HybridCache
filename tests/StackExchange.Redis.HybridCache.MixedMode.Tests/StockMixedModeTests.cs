using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.MixedMode.Tests;

/// <summary>
/// What does the stock stack (DefaultHybridCache over Microsoft.Extensions.Caching.StackExchangeRedis)
/// actually do when it meets our native representation (a plain string at the entry key)?
/// Expected, per source-reading: the HMGET fails with WRONGTYPE, DefaultHybridCache logs it and
/// treats it as a miss (factory runs, nothing thrown); its write-back HSET also fails, is logged
/// and swallowed; so the stock side degrades to factory-per-call for such keys. These tests
/// validate that empirically. The reverse direction (RedisHybridCache meeting a stock hash entry)
/// is asserted in the main test project once the backend exists (phase 2): same contract —
/// logged miss, never a surfaced error — plus "our SET replaces their hash".
/// </summary>
public class StockMixedModeTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public StockMixedModeTests(RedisFixture fx) => _fx = fx;

    private (ServiceProvider Provider, HybridCacheBase Cache, LogCollector Logs) CreateStock(string prefix)
    {
        var logs = new LogCollector();
        var services = new ServiceCollection();
        services.AddLogging(b =>
        {
            b.SetMinimumLevel(LogLevel.Trace);
            b.AddProvider(logs);
        });
        services.AddStackExchangeRedisCache(o =>
        {
            o.Configuration = _fx.Host;
            o.InstanceName = prefix;
        });
        services.AddHybridCache(o => o.DefaultEntryOptions = new HybridCacheEntryOptions
        {
            // force every read/write through L2, so L1 can't mask what we're probing
            Flags = HybridCacheEntryFlags.DisableLocalCacheRead | HybridCacheEntryFlags.DisableLocalCacheWrite,
        });
        var provider = services.BuildServiceProvider();
        return (provider, provider.GetRequiredService<HybridCacheBase>(), logs);
    }

    private static bool MentionsWrongType(LogEntry e)
        => (e.Exception?.ToString() ?? e.Message).Contains("WRONGTYPE", StringComparison.OrdinalIgnoreCase);

    [Fact]
    public async Task StockRead_OfNativeStringEntry_IsLoggedMiss_NotSurfacedError()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = $"mix:{Guid.NewGuid():N}:";
        const string key = "entry";
        var planted = Encoding.UTF8.GetBytes("native-representation string payload");
        await _fx.Db.StringSetAsync(prefix + key, planted);

        var (provider, cache, logs) = CreateStock(prefix);
        await using (provider)
        {
            var value = await cache.GetOrCreateAsync(
                key, _ => new ValueTask<string>("from-factory"),
                cancellationToken: TestContext.Current.CancellationToken);

            // treated as a miss: the factory ran, and nothing was thrown at the caller
            Assert.Equal("from-factory", value);

            // ... and the WRONGTYPE failure was logged, not swallowed silently
            Assert.Contains(logs.Entries, MentionsWrongType);
        }
    }

    [Fact]
    public async Task StockWriteBack_OverNativeStringEntry_FailsLogged_EntrySurvives_SoStockMissesForever()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = $"mix:{Guid.NewGuid():N}:";
        const string key = "entry";
        var planted = Encoding.UTF8.GetBytes("native-representation string payload");
        await _fx.Db.StringSetAsync(prefix + key, planted);

        var (provider, cache, logs) = CreateStock(prefix);
        await using (provider)
        {
            var first = await cache.GetOrCreateAsync(
                key, _ => new ValueTask<string>("v1"),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal("v1", first);

            // the L2 write-back may complete after callers are released; give it a moment
            var deadline = DateTime.UtcNow.AddSeconds(3);
            while (DateTime.UtcNow < deadline
                && !logs.Entries.Any(e => e.Level >= LogLevel.Error && MentionsWrongType(e) && e.Message.Contains("write", StringComparison.OrdinalIgnoreCase))
                && await _fx.Db.KeyTypeAsync(prefix + key) == RedisType.String)
            {
                await Task.Delay(100, TestContext.Current.CancellationToken);
            }

            // our entry was NOT replaced: the stock write-back could not overwrite a string
            Assert.Equal(RedisType.String, await _fx.Db.KeyTypeAsync(prefix + key));
            Assert.Equal(planted, (byte[]?)await _fx.Db.StringGetAsync(prefix + key));

            // consequence: the stock side misses (and re-runs the factory) every single time
            var second = await cache.GetOrCreateAsync(
                key, _ => new ValueTask<string>("v2"),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal("v2", second);
        }
    }

    [Fact]
    public async Task NativeSet_ReplacesStockHashEntry_AndStockThenMisses()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = $"mix:{Guid.NewGuid():N}:";
        const string key = "entry";

        var (provider, cache, logs) = CreateStock(prefix);
        await using (provider)
        {
            // sanity: a normal stock round-trip through L2 works
            await cache.SetAsync(key, "stock-value", cancellationToken: TestContext.Current.CancellationToken);
            var roundTrip = await cache.GetOrCreateAsync(
                key, _ => new ValueTask<string>("unexpected-factory"),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal("stock-value", roundTrip);
            Assert.Equal(RedisType.Hash, await _fx.Db.KeyTypeAsync(prefix + key));

            // a native-style SET replaces the hash outright (SET is type-agnostic): our writes win
            await _fx.Db.StringSetAsync(prefix + key, Encoding.UTF8.GetBytes("native overwrite"));
            Assert.Equal(RedisType.String, await _fx.Db.KeyTypeAsync(prefix + key));

            // ... after which the stock side sees a logged miss, not an error
            var afterOverwrite = await cache.GetOrCreateAsync(
                key, _ => new ValueTask<string>("from-factory"),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal("from-factory", afterOverwrite);
            Assert.Contains(logs.Entries, MentionsWrongType);
        }
    }
}
