// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Behavioural parity with the in-box implementation: the Remove* shapes are ported from
// dotnet/extensions (Microsoft.Extensions.Caching.Hybrid.Tests/FunctionalTests.cs); the fault
// handling, validation and quota tests assert the same logged-event contract (same event IDs)
// as the stock UnreliableL2/L2 suites, rebuilt over a fault-injecting multiplexer.

using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis.HybridCache.Internal;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class ParityTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public ParityTests(RedisFixture fx) => _fx = fx;

    private ServiceProvider CreateProvider(string prefix, LogCollector? logs = null, IConnectionMultiplexer? muxer = null, Action<RedisHybridCacheOptions>? configure = null)
    {
        var services = new ServiceCollection()
            .AddSingleton(muxer ?? _fx.Connection!)
            .AddRedisHybridCache(o =>
            {
                o.KeyPrefix = prefix;
                configure?.Invoke(o);
            });
        if (logs is not null)
        {
            services.AddLogging(b => b.AddProvider(logs));
        }

        return services.BuildServiceProvider();
    }

    private static string Me([CallerMemberName] string caller = "") => caller;

    // ported FunctionalTests shapes (stock ran these over L1; here they run over live redis)

    [Fact]
    public async Task RemoveSingleKey()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        string key = Me();
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(42)));

        // unrelated remove; should use cached value
        await cache.RemoveAsync("unrelated");
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(96)));

        // now remove and repeat - should get updated value
        await cache.RemoveAsync(key);
        Assert.Equal(96, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(96)));
    }

    [Fact]
    public async Task RemoveNoKeyViaArray()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        string key = Me();
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(42)));

        await cache.RemoveAsync([]);
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(96)));
    }

    [Fact]
    public async Task RemoveSingleKeyViaArray()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        string key = Me();
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(42)));

        await cache.RemoveAsync(["unrelated"]);
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(96)));

        await cache.RemoveAsync([key]);
        Assert.Equal(96, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(96)));
    }

    [Fact]
    public async Task RemoveMultipleKeysViaArray()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        string key = Me();
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(42)));
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(96)));

        await cache.RemoveAsync([key, "unrelated"]);
        Assert.Equal(96, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(96)));
    }

    [Fact]
    public async Task RemoveMultipleTagsViaArray()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();
        int calls = 0;
        Func<System.Threading.CancellationToken, ValueTask<int>> factory = _ => new(++calls);

        Assert.Equal(1, await cache.GetOrCreateAsync("a", factory, tags: ["t1"]));
        Assert.Equal(2, await cache.GetOrCreateAsync("b", factory, tags: ["t2"]));
        Assert.Equal(3, await cache.GetOrCreateAsync("c", factory, tags: ["t3"]));

        await cache.RemoveByTagAsync(["t1", "t3"]);

        Assert.Equal(4, await cache.GetOrCreateAsync("a", factory, tags: ["t1"])); // invalidated
        Assert.Equal(2, await cache.GetOrCreateAsync("b", factory, tags: ["t2"])); // untouched
        Assert.Equal(5, await cache.GetOrCreateAsync("c", factory, tags: ["t3"])); // invalidated
    }

    // fault handling: same logged-event contract as stock (CacheBackendReadFailure = 6,
    // CacheBackendWriteFailure = 7), with the caller always served

    [Fact]
    public async Task BackendReadFailure_IsLoggedMiss_AndValueServed()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var logs = new LogCollector();
        var muxer = FaultInjectingMultiplexer.Create(_fx.Connection!, failReads: true, failWrites: false);
        await using var provider = CreateProvider(RedisFixture.NewPrefix(), logs, muxer);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        Assert.Equal("value", await cache.GetOrCreateAsync(Me(), _ => new ValueTask<string>("value")));
        Assert.Contains(logs.Entries, e => e.EventId.Id == Log.IdCacheBackendReadFailure && e.Exception is not null);
    }

    [Fact]
    public async Task BackendWriteFailure_IsLoggedAndSwallowed()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var logs = new LogCollector();
        var muxer = FaultInjectingMultiplexer.Create(_fx.Connection!, failReads: false, failWrites: true);
        string prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(prefix, logs, muxer);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string key = Me();
        Assert.Equal("value", await cache.GetOrCreateAsync(key, _ => new ValueTask<string>("value")));
        Assert.Contains(logs.Entries, e => e.EventId.Id == Log.IdCacheBackendWriteFailure && e.Exception is not null);

        // nothing was stored
        Assert.False(await _fx.Db.KeyExistsAsync(prefix + key));
    }

    // key/tag validation: same logged-event contract as stock, cache bypassed, caller served

    [Theory]
    [InlineData(" ", Log.IdKeyEmptyOrWhitespace)]
    [InlineData("key\u0000withnull", Log.IdKeyInvalidContent)]
    [InlineData("key\twithtab", Log.IdKeyInvalidContent)]
    public async Task InvalidKey_BypassesCache_AndServesValue(string key, int expectedEventId)
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var logs = new LogCollector();
        await using var provider = CreateProvider(RedisFixture.NewPrefix(), logs);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        int calls = 0;
        Func<System.Threading.CancellationToken, ValueTask<int>> factory = _ => new(++calls);
        Assert.Equal(1, await cache.GetOrCreateAsync(key, factory));
        Assert.Equal(2, await cache.GetOrCreateAsync(key, factory)); // never cached
        Assert.Contains(logs.Entries, e => e.EventId.Id == expectedEventId);
    }

    [Fact]
    public async Task OverlongKey_BypassesCache_AndServesValue()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var logs = new LogCollector();
        await using var provider = CreateProvider(RedisFixture.NewPrefix(), logs, configure: o => o.MaximumKeyLength = 16);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string key = new('x', 17);
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(42)));
        Assert.Contains(logs.Entries, e => e.EventId.Id == Log.IdMaximumKeyLengthExceeded);
    }

    [Fact]
    public async Task MalformedUnicodeKey_DisablesBackend_AndServesValue()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var logs = new LogCollector();
        string prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(prefix, logs);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string key = "lone-surrogate-\uD800"; // malformed: cannot round-trip utf-8
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(42)));
        Assert.Contains(logs.Entries, e => e.EventId.Id == Log.IdKeyInvalidUnicode);

        // nothing was stored under the (replacement-char) encoding of that key
        Assert.False(await _fx.Db.KeyExistsAsync(prefix + key));
    }

    [Fact]
    public async Task MalformedUnicodeTag_DisablesBackend_AndServesValue()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var logs = new LogCollector();
        string prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(prefix, logs);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string key = Me();
        Assert.Equal(42, await cache.GetOrCreateAsync(key, _ => new ValueTask<int>(42), tags: ["bad-\uD800"]));
        Assert.Contains(logs.Entries, e => e.EventId.Id == Log.IdTagInvalidUnicode);
        Assert.False(await _fx.Db.KeyExistsAsync(prefix + key));
    }

    // serialization quota: same logged-event contract as stock (MaximumPayloadBytesExceeded = 1);
    // the value is served but never stored

    [Fact]
    public async Task OversizedPayload_IsLoggedAndNotStored()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var logs = new LogCollector();
        string prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(prefix, logs, configure: o => o.MaximumPayloadBytes = 128);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string bigValue = new('x', 1024);
        string key = Me();
        Assert.Equal(bigValue, await cache.GetOrCreateAsync(key, _ => new ValueTask<string>(bigValue)));
        Assert.Contains(logs.Entries, e => e.EventId.Id == Log.IdMaximumPayloadBytesExceeded);
        Assert.False(await _fx.Db.KeyExistsAsync(prefix + key));
    }

    [Fact]
    public async Task DisableUnderlyingData_ReturnsDefaultOnMiss()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateProvider(RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        var options = new HybridCacheEntryOptions { Flags = HybridCacheEntryFlags.DisableUnderlyingData };
        string? result = await cache.GetOrCreateAsync(
            Me(), _ => new ValueTask<string?>("factory-must-not-run"), options);
        Assert.Null(result);
    }

    [Fact]
    public async Task SetAsync_RespectsDisableDistributedCacheWrite()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        string prefix = RedisFixture.NewPrefix();
        await using var provider = CreateProvider(prefix);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        string key = Me();
        var options = new HybridCacheEntryOptions { Flags = HybridCacheEntryFlags.DisableDistributedCacheWrite };
        await cache.SetAsync(key, "value", options);
        Assert.False(await _fx.Db.KeyExistsAsync(prefix + key));
    }
}

/// <summary>
/// Wraps a real multiplexer, failing string reads and/or writes on the databases it hands out -
/// the equivalent of the stock suite's unreliable-L2 harness.
/// </summary>
public class FaultInjectingMultiplexer : DispatchProxy
{
    private IConnectionMultiplexer? _target;
    private bool _failReads;
    private bool _failWrites;

    public static IConnectionMultiplexer Create(IConnectionMultiplexer target, bool failReads, bool failWrites)
    {
        object proxy = Create<IConnectionMultiplexer, FaultInjectingMultiplexer>()!;
        var typed = (FaultInjectingMultiplexer)proxy;
        typed._target = target;
        typed._failReads = failReads;
        typed._failWrites = failWrites;
        return (IConnectionMultiplexer)proxy;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        object? result = targetMethod!.Invoke(_target, args);
        if (targetMethod.Name == nameof(IConnectionMultiplexer.GetDatabase) && result is IDatabase db)
        {
            return FaultInjectingDatabase.Create(db, _failReads, _failWrites);
        }

        return result;
    }

    private class FaultInjectingDatabase : DispatchProxy
    {
        private IDatabase? _target;
        private bool _failReads;
        private bool _failWrites;

        public static IDatabase Create(IDatabase target, bool failReads, bool failWrites)
        {
            object proxy = Create<IDatabase, FaultInjectingDatabase>()!;
            var typed = (FaultInjectingDatabase)proxy;
            typed._target = target;
            typed._failReads = failReads;
            typed._failWrites = failWrites;
            return (IDatabase)proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod!.Name)
            {
                case nameof(IDatabase.StringGetAsync) when _failReads:
                    return Task.FromException<RedisValue>(new InvalidOperationException("deliberate read failure"));
                case nameof(IDatabase.StringSetAsync) when _failWrites:
                    return Task.FromException<bool>(new InvalidOperationException("deliberate write failure"));
                default:
                    try
                    {
                        return targetMethod.Invoke(_target, args);
                    }
                    catch (TargetInvocationException tie) when (tie.InnerException is not null)
                    {
                        throw tie.InnerException;
                    }
            }
        }
    }
}
