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
}
