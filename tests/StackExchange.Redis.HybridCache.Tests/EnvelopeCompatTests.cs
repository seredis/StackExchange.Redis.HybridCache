using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

/// <summary>
/// Validates envelope byte-compatibility against the REAL default implementation
/// (Microsoft.Extensions.Caching.Hybrid): what stock writes, we read, and vice versa.
/// See <see cref="StockHarness"/> for how the stock side is observed.
/// </summary>
public class EnvelopeCompatTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public EnvelopeCompatTests(RedisFixture fx) => _fx = fx;

    private sealed record Poco(string Name, int Value);

    private ServiceProvider CreateOurs(string prefix)
        => new ServiceCollection()
            .AddSingleton<IConnectionMultiplexer>(_fx.Connection!)
            .AddRedisHybridCache(o => o.KeyPrefix = prefix)
            .BuildServiceProvider();

    [Fact]
    public async Task StockWrites_WeRead()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = RedisFixture.NewPrefix();
        var (stockProvider, stock, stub) = StockHarness.Create();
        await using var ours = CreateOurs(prefix);
        await using (stockProvider)
        {
            await stock.SetAsync("s1", "hello-from-stock", cancellationToken: TestContext.Current.CancellationToken);
            await stock.SetAsync("s2", new Poco("stock", 42), tags: ["shared-tag"], cancellationToken: TestContext.Current.CancellationToken);

            // plant the stock envelope bytes as native string entries
            await _fx.Db.StringSetAsync(prefix + "s1", stub.Store["s1"]);
            await _fx.Db.StringSetAsync(prefix + "s2", stub.Store["s2"]);

            var cache = ours.GetRequiredService<HybridCacheBase>();
            var s1 = await cache.GetOrCreateAsync("s1", _ => new ValueTask<string>("factory-must-not-run"), cancellationToken: TestContext.Current.CancellationToken);
            var s2 = await cache.GetOrCreateAsync("s2", _ => new ValueTask<Poco>(new Poco("factory-must-not-run", -1)), cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal("hello-from-stock", s1);
            Assert.Equal(new Poco("stock", 42), s2);
        }
    }

    [Fact]
    public async Task WeWrite_StockReads()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = RedisFixture.NewPrefix();
        var (stockProvider, stock, stub) = StockHarness.Create();
        await using var ours = CreateOurs(prefix);
        await using (stockProvider)
        {
            var cache = ours.GetRequiredService<HybridCacheBase>();
            await cache.SetAsync("o1", "hello-from-us", cancellationToken: TestContext.Current.CancellationToken);
            await cache.SetAsync("o2", new Poco("ours", 7), tags: ["shared-tag"], cancellationToken: TestContext.Current.CancellationToken);

            // plant our envelope bytes where stock's L2 will serve them
            stub.Store["o1"] = (byte[])(await _fx.Db.StringGetAsync(prefix + "o1"))!;
            stub.Store["o2"] = (byte[])(await _fx.Db.StringGetAsync(prefix + "o2"))!;

            var o1 = await stock.GetOrCreateAsync("o1", _ => new ValueTask<string>("factory-must-not-run"), cancellationToken: TestContext.Current.CancellationToken);
            var o2 = await stock.GetOrCreateAsync("o2", _ => new ValueTask<Poco>(new Poco("factory-must-not-run", -1)), cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal("hello-from-us", o1);
            Assert.Equal(new Poco("ours", 7), o2);
        }
    }
}
