// The direct tag-timestamp memo (issue #7, net10 + client-side caching): tag currency on a hot
// entry is answered from a local tag → ticks map kept coherent by SE.Redis per-key removal
// notifications. These tests assert the SEMANTICS, which must hold identically on every TFM
// (downlevel has no memo and answers from the client cache): our own RemoveByTag is visible to
// the very next read (read-your-own-writes - the memo entry is dropped synchronously with the
// write), and a foreign RemoveByTag converges within push latency (the removal notification).
// The reads before each invalidation deliberately repeat, so on net10 the memo is hot and the
// invalidation genuinely has to go through its coherence machinery.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class TagMemoTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public TagMemoTests(RedisFixture fx) => _fx = fx;

    private static ServiceProvider CreateProvider(IConnectionMultiplexer muxer, string prefix)
        => new ServiceCollection()
            .AddSingleton(muxer)
            .AddRedisHybridCache(o => o.KeyPrefix = prefix)
            .BuildServiceProvider();

    private static async Task HeatAsync(HybridCacheBase cache, string key, string[] tags)
    {
        // several reads of the same tagged entry: the first populates the local caches, the
        // rest are served hot (and, on net10, from the tag memo)
        for (int i = 0; i < 4; i++)
        {
            Assert.Equal("v1", await cache.GetOrCreateAsync(key, _ => new ValueTask<string>("v1"), tags: tags, cancellationToken: TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    public async Task OwnRemoveByTag_VisibleToTheVeryNextRead()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        var ct = TestContext.Current.CancellationToken;

        await HeatAsync(cache, "own", ["t-own"]);

        // read-your-own-writes: no polling, no grace - the next read MUST miss
        await cache.RemoveByTagAsync("t-own", ct);
        Assert.Equal("v2", await cache.GetOrCreateAsync("own", _ => new ValueTask<string>("v2"), tags: ["t-own"], cancellationToken: ct));
    }

    [Fact]
    public async Task OwnWildcardRemove_VisibleToTheVeryNextRead()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        var ct = TestContext.Current.CancellationToken;

        await HeatAsync(cache, "ownwild", ["t-any"]);

        await cache.RemoveByTagAsync("*", ct);
        Assert.Equal("v2", await cache.GetOrCreateAsync("ownwild", _ => new ValueTask<string>("v2"), tags: ["t-any"], cancellationToken: ct));
    }

#if NET10_0_OR_GREATER
    [Fact]
    public async Task TagMemo_EngagesExactlyWhenClientCacheIsPresent()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        // with client-side caching: engaged (so the semantic tests above really do exercise it)
        await using var cachedProvider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cached = Assert.IsType<RedisHybridCache>(cachedProvider.GetRequiredService<HybridCacheBase>());
        Assert.True(cached.TagMemoEngaged);

        // without: no invalidation source to keep a memo honest, so there must be no memo
        await using var plainProvider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var plain = Assert.IsType<RedisHybridCache>(plainProvider.GetRequiredService<HybridCacheBase>());
        Assert.False(plain.TagMemoEngaged);
    }
#endif

    [Fact]
    public async Task ForeignRemoveByTag_ConvergesWithinPushLatency()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        string prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(_fx.CachedConnection!, prefix);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        // a second, unrelated client doing the invalidating - the case stock's local tag memo
        // structurally cannot see, and the reason the memo here rides removal notifications
        await using var foreignProvider = CreateProvider(_fx.Connection!, prefix);
        var foreign = foreignProvider.GetRequiredService<HybridCacheBase>();

        var ct = TestContext.Current.CancellationToken;
        await HeatAsync(cache, "foreign", ["t-foreign"]);

        await foreign.RemoveByTagAsync("t-foreign", ct);

        // the push → notification → memo-drop chain is asynchronous; converge, promptly
        var sw = Stopwatch.StartNew();
        while (true)
        {
            string value = await cache.GetOrCreateAsync("foreign", _ => new ValueTask<string>("v2"), tags: ["t-foreign"], cancellationToken: ct);
            if (value == "v2")
            {
                break;
            }

            Assert.True(sw.ElapsedMilliseconds < 5000, $"still serving the pre-invalidation value after {sw.ElapsedMilliseconds}ms");
            await Task.Delay(50, ct);
        }
    }
}
