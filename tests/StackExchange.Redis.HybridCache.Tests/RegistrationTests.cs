using System;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis.Hybrid;
using Xunit;

namespace StackExchange.Redis.Hybrid.Tests;

public class RegistrationTests
{
    [Fact]
    public void OptionsHaveDocumentedDefaults()
    {
        var options = new RedisHybridCacheOptions();
        Assert.Equal("hc:", options.KeyPrefix);
        Assert.Equal("hc:t:", options.TagPrefix);
        Assert.Equal(1 << 20, options.MaximumPayloadBytes);
        Assert.Equal(1024, options.MaximumKeyLength);
    }

    [Fact]
    public void ResolvingWithoutMultiplexerThrowsMeaningfully()
    {
        using var provider = new ServiceCollection()
            .AddRedisHybridCache()
            .BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() => { _ = provider.GetRequiredService<HybridCache>(); });
        Assert.Contains("IConnectionMultiplexer", ex.Message);
    }
}
