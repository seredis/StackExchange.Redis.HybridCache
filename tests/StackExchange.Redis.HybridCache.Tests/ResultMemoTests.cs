// Result memoization (issue #6, via SE.Redis IRespResultReuse): with client-side caching, a
// repeat hit on the same entry is handed the already-parsed CachedEnvelope, and - for types the
// immutability conventions prove shareable - the already-deserialized value instance. These
// tests pin the observable contract: immutable values ARE shared across calls (the stock
// parity divergence this retires), mutable values are NEVER shared.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class ResultMemoTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public ResultMemoTests(RedisFixture fx) => _fx = fx;

    private static ServiceProvider CreateProvider(IConnectionMultiplexer muxer, string prefix)
        => new ServiceCollection()
            .AddSingleton(muxer)
            .AddRedisHybridCache(o => o.KeyPrefix = prefix)
            .BuildServiceProvider();

    [Fact]
    public async Task ImmutableValue_InstanceSharedAcrossLocalHits()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        var ct = TestContext.Current.CancellationToken;

        string key = "memo-imm";
        _ = await cache.GetOrCreateAsync(key, _ => new ValueTask<string>("payload"), cancellationToken: ct);

        // the first read after the write is the remote fetch that populates the local cache;
        // the reads after that are local hits served through the envelope + value memos
        _ = await cache.GetOrCreateAsync(key, _ => new ValueTask<string>("wrong"), cancellationToken: ct);
        string first = await cache.GetOrCreateAsync(key, _ => new ValueTask<string>("wrong"), cancellationToken: ct);
        string second = await cache.GetOrCreateAsync(key, _ => new ValueTask<string>("wrong"), cancellationToken: ct);

        Assert.Equal("payload", first);
        Assert.Same(first, second); // one deserialization, one instance: string is provably shareable
    }

    [Fact]
    public async Task MutableValue_FreshInstancePerCall()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        var ct = TestContext.Current.CancellationToken;

        string key = "memo-mut";
        _ = await cache.GetOrCreateAsync(key, _ => new ValueTask<MutableThing>(new MutableThing { Value = "payload" }), cancellationToken: ct);

        _ = await cache.GetOrCreateAsync(key, _ => new ValueTask<MutableThing>(new MutableThing { Value = "wrong" }), cancellationToken: ct);
        MutableThing first = await cache.GetOrCreateAsync(key, _ => new ValueTask<MutableThing>(new MutableThing { Value = "wrong" }), cancellationToken: ct);
        MutableThing second = await cache.GetOrCreateAsync(key, _ => new ValueTask<MutableThing>(new MutableThing { Value = "wrong" }), cancellationToken: ct);

        Assert.Equal("payload", first.Value);
        Assert.Equal("payload", second.Value);
        Assert.NotSame(first, second); // mutable: sharing would be corruption, not caching

        // and proof: a caller mutating its copy must not poison later reads
        first.Value = "mutated";
        MutableThing third = await cache.GetOrCreateAsync(key, _ => new ValueTask<MutableThing>(new MutableThing { Value = "wrong" }), cancellationToken: ct);
        Assert.Equal("payload", third.Value);
    }

    public sealed class MutableThing
    {
        public string? Value { get; set; }
    }
}
