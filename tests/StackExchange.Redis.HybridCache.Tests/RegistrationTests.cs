using System;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class RegistrationTests
{
    [Fact]
    public void OptionsHaveDocumentedDefaults()
    {
        var options = new RedisHybridCacheOptions();
        Assert.Equal("hc:", options.KeyPrefix);
        Assert.Equal(1 << 20, options.MaximumPayloadBytes);
        Assert.Equal(1024, options.MaximumKeyLength);
    }

    [Fact]
    public void ResolvingWithoutMultiplexerThrowsMeaningfully()
    {
        using var provider = new ServiceCollection()
            .AddRedisHybridCache()
            .BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() => { _ = provider.GetRequiredService<HybridCacheBase>(); });
        Assert.Contains("IConnectionMultiplexer", ex.Message);
    }

    [Fact]
    public void KeyedMultiplexerRegistration_IsSufficient()
    {
        // a dedicated, cache-scoped connection keyed by typeof(HybridCache) is the encouraged
        // shape; resolution must find it without any unkeyed registration present
        using var provider = new ServiceCollection()
            .AddKeyedSingleton(typeof(HybridCacheBase), (_, _) => PlaceholderMultiplexer.Instance)
            .AddRedisHybridCache()
            .BuildServiceProvider();

        var cache = provider.GetRequiredService<HybridCacheBase>();
        Assert.IsType<RedisHybridCache>(cache);
    }
}

/// <summary>Never used for traffic: construction-only stand-in (the ctor null-checks, nothing more).</summary>
public class PlaceholderMultiplexer : System.Reflection.DispatchProxy
{
    public static IConnectionMultiplexer Instance { get; }
        = (IConnectionMultiplexer)(object)Create<IConnectionMultiplexer, PlaceholderMultiplexer>();

    protected override object? Invoke(System.Reflection.MethodInfo? targetMethod, object?[]? args)
        => targetMethod?.Name switch
        {
            "Dispose" => null, // the container disposes singletons; tolerate that
            "DisposeAsync" => default(System.Threading.Tasks.ValueTask),
            _ => throw new InvalidOperationException("This multiplexer is a placeholder and must not be used."),
        };
}
