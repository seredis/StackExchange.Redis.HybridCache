// Setup.cs - the SE.Redis implementation: HybridCache built directly on client-side caching.
// The near cache is the multiplexer's client cache, and redis PUSHES invalidations into it
// (CLIENT TRACKING over RESP3): another server's write or tag invalidation evicts the local
// copy in milliseconds. The entry options are the same as the stock setup - what changes is
// the invalidation transport, not the policy. Invalidation is ACTIVE - push.

using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using StackExchange.Redis.Caching;

namespace InvalidationDemo;

static class Setup
{
    public const string Flavor = "StackExchange.Redis.HybridCache (client-side caching)";
    public const string Description =
        "Near cache: the multiplexer's client cache; redis pushes invalidations (CLIENT TRACKING).\n" +
        "A foreign change evicts the local copy immediately - staleness is not traded for anything.";

    private const string Endpoint = "localhost:16412";

    // both servers share the run's keyspace; a new run starts clean
    private static readonly string KeySpace = $"hybrid-{Guid.NewGuid():N}:";

    public static async Task<ServiceProvider> CreateServerAsync()
    {
        var config = ConfigurationOptions.Parse(Endpoint);
        config.Protocol = RedisProtocol.Resp3;                   // invalidations arrive as RESP3 push messages
        config.ClientCache = new CacheOptions { Prefixes = [KeySpace] };
        var muxer = await ConnectionMultiplexer.ConnectAsync(config);

        var services = new ServiceCollection();

        // a dedicated connection for the cache, keyed by the ecosystem convention
        services.AddKeyedSingleton<IConnectionMultiplexer>(typeof(HybridCache), muxer);

        services.AddRedisHybridCache(options =>
        {
            options.KeyPrefix = KeySpace;
            options.DefaultEntryOptions = new HybridCacheEntryOptions
            {
                Expiration = TimeSpan.FromMinutes(5),            // lifetime in redis
                LocalCacheExpiration = TimeSpan.FromSeconds(10), // same bound as the stock setup; a safety net here, not the mechanism
            };
        });

        return services.BuildServiceProvider();
    }
}
