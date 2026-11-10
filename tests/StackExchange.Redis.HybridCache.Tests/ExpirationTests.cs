// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// The TimeProvider-driven part of the stock ExpirationTests concept: entry expiry is enforced at
// the envelope level against the injected clock (the redis PX TTL is the server-side backstop,
// covered by the live-TTL tests in RedisHybridCacheTests).

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class ExpirationTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public ExpirationTests(RedisFixture fx) => _fx = fx;

    private sealed class ManualClock : TimeProvider
    {
        private long _utcTicks = DateTimeOffset.UtcNow.UtcTicks;

        public override DateTimeOffset GetUtcNow() => new(Volatile.Read(ref _utcTicks), TimeSpan.Zero);

        public void Advance(TimeSpan by) => Interlocked.Add(ref _utcTicks, by.Ticks);
    }

    [Fact]
    public async Task EnvelopeExpiry_HonoursInjectedClock()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var clock = new ManualClock();
        await using var provider = new ServiceCollection()
            .AddSingleton<IConnectionMultiplexer>(_fx.Connection!)
            .AddSingleton<TimeProvider>(clock)
            .AddRedisHybridCache(o => o.KeyPrefix = RedisFixture.NewPrefix())
            .BuildServiceProvider();
        var cache = provider.GetRequiredService<HybridCacheBase>();

        int calls = 0;
        Func<CancellationToken, ValueTask<int>> factory = _ => new(++calls);
        var options = new HybridCacheEntryOptions { Expiration = TimeSpan.FromMinutes(5) };

        Assert.Equal(1, await cache.GetOrCreateAsync("k", factory, options, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(1, await cache.GetOrCreateAsync("k", factory, options, cancellationToken: TestContext.Current.CancellationToken)); // cached

        clock.Advance(TimeSpan.FromMinutes(4));
        Assert.Equal(1, await cache.GetOrCreateAsync("k", factory, options, cancellationToken: TestContext.Current.CancellationToken)); // still cached

        clock.Advance(TimeSpan.FromMinutes(2)); // now 6 minutes after creation: past the 5-minute envelope duration
        Assert.Equal(2, await cache.GetOrCreateAsync("k", factory, options, cancellationToken: TestContext.Current.CancellationToken)); // expired by envelope, despite the live redis TTL
    }
}
