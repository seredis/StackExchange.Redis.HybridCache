// Setup.cs - the stock pairing: the in-box HybridCache (DefaultHybridCache) over
// IDistributedCache-on-redis, with a MemoryCache near cache per server.
//
// There is NO channel from redis back to the near caches. When another server changes a
// value, this server keeps serving its local copy until that copy EXPIRES, so
// LocalCacheExpiration is a tradeoff you must choose: shorter = less stale, but more round
// trips. Invalidation is PASSIVE - expiry.

using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;

namespace InvalidationDemo;

static class Setup
{
    public const string Flavor = "stock HybridCache over IDistributedCache (redis)";
    public const string Description =
        "Near cache: a MemoryCache per server; no invalidation channel. A foreign change is\n" +
        "seen when the local copy expires - LocalCacheExpiration (here 10s) bounds the staleness.";

    private const string Endpoint = "localhost:16412";

    // both servers share the run's keyspace; a new run starts clean
    private static readonly string KeySpace = $"stock-{Guid.NewGuid():N}:";

    public static async Task<ServiceProvider> CreateServerAsync()
    {
        var services = new ServiceCollection();

        services.AddStackExchangeRedisCache(options =>
        {
            options.Configuration = Endpoint;
            options.InstanceName = KeySpace;
        });

        services.AddHybridCache(options => options.DefaultEntryOptions = new HybridCacheEntryOptions
        {
            Expiration = TimeSpan.FromMinutes(5),            // lifetime in redis
            LocalCacheExpiration = TimeSpan.FromSeconds(10), // lifetime of a local copy: the staleness bound
        });

        var provider = services.BuildServiceProvider();

        // fail fast if redis is down: this implementation treats an unreachable backend as
        // a cache miss, which would quietly turn the demo into a single-tier cache
        _ = await provider.GetRequiredService<IDistributedCache>().GetAsync("connectivity-check");
        return provider;
    }
}
