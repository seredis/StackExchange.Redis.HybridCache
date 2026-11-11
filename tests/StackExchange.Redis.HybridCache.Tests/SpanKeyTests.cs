// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// The allocation-free read path: the ReadOnlySpan<char> GetOrCreateAsync override (which the
// abstract base's DefaultInterpolatedStringHandler overloads route through, net10+), and the
// zero-allocation contract of locally-served hits - string-keyed and interpolated alike.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis.HybridCache.Internal;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class SpanKeyTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public SpanKeyTests(RedisFixture fx) => _fx = fx;

    private static ServiceProvider CreateProvider(IConnectionMultiplexer muxer, string prefix, bool rawIntSerializer = false)
    {
        IServiceCollection services = new ServiceCollection()
            .AddSingleton(muxer)
            .AddRedisHybridCache(o => o.KeyPrefix = prefix);
        if (rawIntSerializer)
        {
            // a fixed-size binary serializer, so the measured path is ours rather than STJ's
            services.AddSingleton<IHybridCacheSerializer<int>>(new RawIntSerializer());
        }

        return services.BuildServiceProvider();
    }

    private sealed class RawIntSerializer : IHybridCacheSerializer<int>
    {
        public int Deserialize(ReadOnlySequence<byte> source)
        {
            Span<byte> buffer = stackalloc byte[sizeof(int)];
            source.CopyTo(buffer);
            return BinaryPrimitives.ReadInt32LittleEndian(buffer);
        }

        public void Serialize(int value, IBufferWriter<byte> target)
        {
            Span<byte> buffer = target.GetSpan(sizeof(int));
            BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
            target.Advance(sizeof(int));
        }
    }

#if NET10_0_OR_GREATER
    [Fact]
    public async Task InterpolatedKey_HitsEntryCreatedViaString()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        int calls = 0;
        Func<CancellationToken, ValueTask<string>> factory = _ =>
        {
            Interlocked.Increment(ref calls);
            return new("payload");
        };

        Assert.Equal("payload", await cache.GetOrCreateAsync("spans/17", factory));
        int id = 17;
        Assert.Equal("payload", await cache.GetOrCreateAsync($"spans/{id}", factory));
        Assert.Equal(1, calls); // the interpolated spelling found the string-keyed entry
    }

    [Fact]
    public async Task InterpolatedKey_MissCreatesEntryReadableViaString()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        Guid id = Guid.NewGuid();
        Assert.Equal(42, await cache.GetOrCreateAsync($"spanmiss/{id}", _ => new ValueTask<int>(42)));
        Assert.Equal(42, await cache.GetOrCreateAsync("spanmiss/" + id, _ => new ValueTask<int>(-1)));
    }

    [Fact]
    public async Task InterpolatedKey_InvalidKeyBypassesCache()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        // whitespace key: the cache is bypassed and the factory serves every call (stock contract)
        string space = " ";
        Assert.Equal(1, await cache.GetOrCreateAsync($"{space}", _ => new ValueTask<int>(1)));
        Assert.Equal(2, await cache.GetOrCreateAsync($"{space}", _ => new ValueTask<int>(2)));
    }

    [Fact]
    public async Task InterpolatedKey_HonoursDisableDistributedCacheRead()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(_fx.Connection!, RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        var options = new HybridCacheEntryOptions { Flags = HybridCacheEntryFlags.DisableDistributedCacheRead };
        string key = Guid.NewGuid().ToString("N");
        Assert.Equal(1, await cache.GetOrCreateAsync($"ddr/{key}", _ => new ValueTask<int>(1), options));

        // the write still happened, so a normal read finds it
        Assert.Equal(1, await cache.GetOrCreateAsync("ddr/" + key, _ => new ValueTask<int>(-1)));
    }
#endif

    [Fact]
    public async Task StringKeyHits_DoNotAllocate()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        Assert.SkipWhen(HybridCacheEventSource.Log.IsEnabled(), "EventSource listener active (parallel EventSource tests); counters would attribute allocations to this thread");

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix(), rawIntSerializer: true);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string key = "zeroalloc/string";
        Func<CancellationToken, ValueTask<int>> factory = _ => new(42);

        // warm: miss -> factory -> SET; then reads until locally served
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(42, await cache.GetOrCreateAsync(key, factory, cancellationToken: TestContext.Current.CancellationToken));
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        int wrong = 0;
        for (int i = 0; i < 1_000; i++)
        {
            if (await cache.GetOrCreateAsync(key, factory, cancellationToken: TestContext.Current.CancellationToken) != 42)
            {
                wrong++;
            }
        }

        long delta = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.SkipWhen(HybridCacheEventSource.Log.IsEnabled(), "EventSource listener became active mid-measurement");
        Assert.Equal(0, wrong);
        Assert.Equal(0, delta);
    }

    [Fact]
    public async Task TaggedStringKeyHits_DoNotAllocate()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        Assert.SkipWhen(HybridCacheEventSource.Log.IsEnabled(), "EventSource listener active (parallel EventSource tests); counters would attribute allocations to this thread");

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix(), rawIntSerializer: true);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string key = "zeroalloc/tagged";
        string[] tags = ["zat1", "zat2"];
        Func<CancellationToken, ValueTask<int>> factory = _ => new(42);

        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(42, await cache.GetOrCreateAsync(key, factory, tags: tags, cancellationToken: TestContext.Current.CancellationToken));
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        int wrong = 0;
        for (int i = 0; i < 1_000; i++)
        {
            // the tag region is walked as UTF-8 and both tag timestamps consulted, every hit
            if (await cache.GetOrCreateAsync(key, factory, tags: tags, cancellationToken: TestContext.Current.CancellationToken) != 42)
            {
                wrong++;
            }
        }

        long delta = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.SkipWhen(HybridCacheEventSource.Log.IsEnabled(), "EventSource listener became active mid-measurement");
        Assert.Equal(0, wrong);
        Assert.Equal(0, delta);
    }

    [Fact]
    public async Task BoundedLocalExpirationHits_DoNotAllocate()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        Assert.SkipWhen(HybridCacheEventSource.Log.IsEnabled(), "EventSource listener active (parallel EventSource tests); counters would attribute allocations to this thread");

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix(), rawIntSerializer: true);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        // a tighter-than-backend local bound routes reads through the memoized WithMaxCacheAge
        // context; the derived context must be built once, not per call
        var options = new HybridCacheEntryOptions
        {
            Expiration = TimeSpan.FromMinutes(5),
            LocalCacheExpiration = TimeSpan.FromSeconds(30),
        };

        string key = "zeroalloc/bounded";
        Func<CancellationToken, ValueTask<int>> factory = _ => new(42);

        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(42, await cache.GetOrCreateAsync(key, factory, options, cancellationToken: TestContext.Current.CancellationToken));
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        int wrong = 0;
        for (int i = 0; i < 1_000; i++)
        {
            if (await cache.GetOrCreateAsync(key, factory, options, cancellationToken: TestContext.Current.CancellationToken) != 42)
            {
                wrong++;
            }
        }

        long delta = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.SkipWhen(HybridCacheEventSource.Log.IsEnabled(), "EventSource listener became active mid-measurement");
        Assert.Equal(0, wrong);
        Assert.Equal(0, delta);
    }

#if NET10_0_OR_GREATER
    [Fact]
    public async Task InterpolatedKeyHits_DoNotAllocate()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);
        Assert.SkipWhen(HybridCacheEventSource.Log.IsEnabled(), "EventSource listener active (parallel EventSource tests); counters would attribute allocations to this thread");

        await using var provider = CreateProvider(_fx.CachedConnection!, RedisFixture.NewPrefix(), rawIntSerializer: true);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        Func<CancellationToken, ValueTask<int>> factory = _ => new(42);

        for (int i = 0; i < 10; i++)
        {
            int id = i % 2;
            Assert.Equal(42, await cache.GetOrCreateAsync($"zeroalloc/dish/{id}", factory));
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        int wrong = 0;
        for (int i = 0; i < 1_000; i++)
        {
            // the headline: a composite interpolated key, never materialized as a string on a hit
            int id = i % 2;
            if (await cache.GetOrCreateAsync($"zeroalloc/dish/{id}", factory) != 42)
            {
                wrong++;
            }
        }

        long delta = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.SkipWhen(HybridCacheEventSource.Log.IsEnabled(), "EventSource listener became active mid-measurement");
        Assert.Equal(0, wrong);
        Assert.Equal(0, delta);
    }
#endif
}
