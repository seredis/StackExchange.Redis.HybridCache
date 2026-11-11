// Setup.cs - the ONLY file where the two modes differ; everything else in the demo
// (Usage.cs above all) is identical for both. The entry options are identical too:
// the variable is the invalidation TRANSPORT, not the policy.
//
//   Stock - the in-box HybridCache (DefaultHybridCache) over IDistributedCache-on-redis,
//     with a MemoryCache near cache per server. There is NO channel from redis back to the
//     near caches: a foreign change is seen when the local copy EXPIRES, so
//     LocalCacheExpiration is a tradeoff you must choose (shorter = less stale, but more
//     round trips). Invalidation is PASSIVE - expiry.
//
//   Push - StackExchange.Redis.HybridCache, built directly on client-side caching. The
//     near cache is the multiplexer's client cache, and redis PUSHES invalidations into it
//     (CLIENT TRACKING over RESP3): another server's write or tag invalidation evicts the
//     local copy in milliseconds. Invalidation is ACTIVE - push.

using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using StackExchange.Redis.Caching;

namespace InvalidationDemo;

enum CacheMode
{
    /// <summary>stock HybridCache over IDistributedCache-on-redis: passive (expiry) invalidation</summary>
    Stock,

    /// <summary>StackExchange.Redis.HybridCache on client-side caching: active (push) invalidation</summary>
    Push,
}

static class Setup
{
    private const string Endpoint = "localhost:16412";

    // identical for both modes
    private static HybridCacheEntryOptions EntryOptions => new()
    {
        Expiration = TimeSpan.FromMinutes(5),            // lifetime in redis
        LocalCacheExpiration = TimeSpan.FromSeconds(10), // lifetime of a local copy
    };

    // each mode gets its own keyspace, shared by both its servers; a new run starts clean
    private static readonly string StockKeySpace = $"stock-{Guid.NewGuid():N}:";
    private static readonly string PushKeySpace = $"push-{Guid.NewGuid():N}:";

    public static string Flavor(CacheMode mode) => mode switch
    {
        CacheMode.Stock => "stock HybridCache over IDistributedCache (redis)",
        _ => "StackExchange.Redis.HybridCache (client-side caching)",
    };

    public static string Header(CacheMode mode) => mode switch
    {
        CacheMode.Stock => "stock (passive)",
        _ => "SE.Redis (active)",
    };

    public static string Description(CacheMode mode) => mode switch
    {
        CacheMode.Stock =>
            "Near cache: a MemoryCache per server; no invalidation channel. A foreign change is\n" +
            "seen when the local copy expires - LocalCacheExpiration (here 10s) bounds the staleness.",
        _ =>
            "Near cache: the multiplexer's client cache; redis pushes invalidations (CLIENT TRACKING).\n" +
            "A foreign change evicts the local copy immediately - staleness is not traded for anything.",
    };

    public static Task<ServiceProvider> CreateServerAsync(CacheMode mode) => mode switch
    {
        CacheMode.Stock => CreateStockServerAsync(),
        _ => CreatePushServerAsync(),
    };

    private static async Task<ServiceProvider> CreateStockServerAsync()
    {
        var services = new ServiceCollection();

        services.AddStackExchangeRedisCache(options =>
        {
            options.Configuration = Endpoint;
            options.InstanceName = StockKeySpace;
        });

        services.AddHybridCache(options => options.DefaultEntryOptions = EntryOptions);

        var provider = services.BuildServiceProvider();

        // fail fast if redis is down: this implementation treats an unreachable backend as
        // a cache miss, which would quietly turn the demo into a single-tier cache
        _ = await provider.GetRequiredService<IDistributedCache>().GetAsync("connectivity-check");
        return provider;
    }

    private static async Task<ServiceProvider> CreatePushServerAsync()
    {
        var config = ConfigurationOptions.Parse(Endpoint);
        config.Protocol = RedisProtocol.Resp3;                       // invalidations arrive as RESP3 push messages
        config.ClientCache = new CacheOptions { Prefixes = [PushKeySpace] };
        var muxer = await ConnectionMultiplexer.ConnectAsync(config);

        var services = new ServiceCollection();

        // a dedicated connection for the cache, keyed by the ecosystem convention
        services.AddKeyedSingleton<IConnectionMultiplexer>(typeof(HybridCache), muxer);

        services.AddRedisHybridCache(options =>
        {
            options.KeyPrefix = PushKeySpace;
            options.DefaultEntryOptions = EntryOptions; // same bounds; LocalCacheExpiration is a safety net here, not the mechanism
        });

        return services.BuildServiceProvider();
    }
}
