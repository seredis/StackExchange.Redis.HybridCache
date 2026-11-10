using Microsoft.Extensions.Caching.Hybrid;

namespace StackExchange.Redis.HybridCache;

/// <summary>
/// Options for <see cref="RedisHybridCache"/>.
/// </summary>
public sealed class RedisHybridCacheOptions
{
    /// <summary>
    /// The prefix applied to every key this cache stores in redis (entries and tag-timestamp
    /// keys alike). It should also appear in the multiplexer's <c>CacheOptions.Prefixes</c>, so
    /// that reads are tracked and stored on first miss (<c>TrackingByPrefix</c>); with other
    /// tracking modes, per-call <c>CommandFlags.CacheOptIn</c> is used instead.
    /// </summary>
    public string KeyPrefix { get; set; } = "hc:";

    /// <summary>
    /// Default options applied to entries when the caller supplies none.
    /// </summary>
    public HybridCacheEntryOptions? DefaultEntryOptions { get; set; }

    /// <summary>
    /// The largest payload, in bytes, that will be cached (serialized form).
    /// </summary>
    public long MaximumPayloadBytes { get; set; } = 1 << 20; // 1 MiB, matching the contract default

    /// <summary>
    /// The longest permitted cache key, in characters.
    /// </summary>
    public int MaximumKeyLength { get; set; } = 1024;
}
