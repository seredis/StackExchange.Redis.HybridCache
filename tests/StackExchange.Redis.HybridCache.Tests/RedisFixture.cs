using System;
using StackExchange.Redis.Caching;

namespace StackExchange.Redis.HybridCache.Tests;

public sealed class RedisFixture : IDisposable
{
    public const string TrackedPrefix = "hc:";

    public ConnectionMultiplexer? Connection { get; }

    /// <summary>RESP3 + client-side caching enabled (tracking the <see cref="TrackedPrefix"/> keyspace); null when unavailable.</summary>
    public ConnectionMultiplexer? CachedConnection { get; }

    public string Host { get; }

    public string SkipReason { get; }

    public string CachedSkipReason { get; }

    public IDatabase Db => Connection!.GetDatabase();

    public RedisFixture()
    {
        Host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost:6379";
        SkipReason = CachedSkipReason = "";
        try
        {
            Connection = ConnectionMultiplexer.Connect($"{Host},connectTimeout=2000,abortConnect=true");
        }
        catch (Exception ex)
        {
            SkipReason = CachedSkipReason = $"redis not available at {Host}: {ex.Message}";
            return;
        }

        try
        {
            var options = ConfigurationOptions.Parse($"{Host},connectTimeout=2000,abortConnect=true");
            options.Protocol = RedisProtocol.Resp3;
            options.ClientCache = new CacheOptions
            {
                Prefixes = [TrackedPrefix],
            };
            CachedConnection = ConnectionMultiplexer.Connect(options);
        }
        catch (Exception ex)
        {
            CachedSkipReason = $"client-side caching unavailable at {Host}: {ex.Message}";
        }
    }

    public static string NewPrefix() => $"{TrackedPrefix}{Guid.NewGuid():N}:";

    public void Dispose()
    {
        Connection?.Dispose();
        CachedConnection?.Dispose();
    }
}
