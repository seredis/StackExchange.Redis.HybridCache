using System;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using StackExchange.Redis.HybridCache;
using StackExchange.Redis.HybridCache.Internal;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registration extensions for <see cref="RedisHybridCache"/>.
/// </summary>
public static class RedisHybridCacheServiceExtensions
{
    /// <summary>
    /// Registers <see cref="RedisHybridCache"/> as the <see cref="HybridCache"/> implementation,
    /// using the <see cref="IConnectionMultiplexer"/> registered in the container. A keyed
    /// registration with the key <c>typeof(HybridCache)</c> is preferred over the non-keyed one:
    /// client-side caching is tightly coupled to its connection (one cache per multiplexer,
    /// cache-wide tracking configuration, flush on disconnect), so a dedicated multiplexer for
    /// the cache — RESP3, <c>ClientCache</c> configured with the cache's key prefix — is actively
    /// encouraged, leaving the application's general-purpose connection untouched:
    /// <code>
    /// services.AddKeyedSingleton&lt;IConnectionMultiplexer&gt;(typeof(HybridCache), (_, _) =&gt; dedicated);
    /// </code>
    /// The multiplexer should be connected with RESP3 and <c>ClientCache</c> configured; see the
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

        // serializer registrations, mirroring the in-box AddHybridCache: inbuilt handling for
        // string/byte[], and a factory covering everything else via System.Text.Json (honouring
        // JsonSerializerOptions keyed by the open/closed IHybridCacheSerializer<> types)
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IHybridCacheSerializerFactory, DefaultJsonSerializerFactory>();
        services.TryAddSingleton<IHybridCacheSerializer<string>>(InbuiltSerializers.StringSerializer.Instance);
        services.TryAddSingleton<IHybridCacheSerializer<byte[]>>(InbuiltSerializers.ByteArraySerializer.Instance);

        services.TryAddSingleton<HybridCache>(static provider =>
        {
            // prefer a cache-dedicated connection, keyed by typeof(HybridCache) — the same keyed
            // convention the wider HybridCache ecosystem uses for cache-scoped services
            var multiplexer = (provider as IKeyedServiceProvider)?.GetKeyedService<IConnectionMultiplexer>(typeof(HybridCache))
                ?? provider.GetService<IConnectionMultiplexer>()
                ?? throw new InvalidOperationException(
                    $"{nameof(AddRedisHybridCache)} requires an {nameof(IConnectionMultiplexer)} to be registered, " +
                    $"either keyed with typeof({nameof(HybridCache)}) (preferred: a dedicated connection for the cache) or unkeyed.");
            return new RedisHybridCache(
                multiplexer,
                provider.GetRequiredService<IOptions<RedisHybridCacheOptions>>(),
                provider.GetService<ILogger<RedisHybridCache>>(),
                provider);
        });
        return services;
    }
}
