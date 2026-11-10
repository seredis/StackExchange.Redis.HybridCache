using System;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using StackExchange.Redis.Hybrid;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registration extensions for <see cref="RedisHybridCache"/>.
/// </summary>
public static class RedisHybridCacheServiceExtensions
{
    /// <summary>
    /// Registers <see cref="RedisHybridCache"/> as the <see cref="HybridCache"/> implementation,
    /// using the <see cref="IConnectionMultiplexer"/> registered in the container. The
    /// multiplexer should be connected with RESP3 and <c>ClientCache</c> configured; see the
    /// package documentation for degraded modes when it is not.
    /// </summary>
    public static IServiceCollection AddRedisHybridCache(
        this IServiceCollection services,
        Action<RedisHybridCacheOptions>? configure = null)
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddOptions();
        services.TryAddSingleton<HybridCache>(static provider =>
        {
            var multiplexer = provider.GetService<IConnectionMultiplexer>()
                ?? throw new InvalidOperationException(
                    $"{nameof(AddRedisHybridCache)} requires an {nameof(IConnectionMultiplexer)} to be registered.");
            return new RedisHybridCache(
                multiplexer,
                provider.GetRequiredService<IOptions<RedisHybridCacheOptions>>(),
                provider.GetService<ILogger<RedisHybridCache>>());
        });
        return services;
    }
}
