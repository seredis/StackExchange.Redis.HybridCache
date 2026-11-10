using System;
using StackExchange.Redis;

namespace StackExchange.Redis.HybridCache.MixedMode.Tests;

public sealed class RedisFixture : IDisposable
{
    public ConnectionMultiplexer? Connection { get; }

    public string Host { get; }

    public string SkipReason { get; }

    public IDatabase Db => Connection!.GetDatabase();

    public RedisFixture()
    {
        Host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost:6379";
        try
        {
            Connection = ConnectionMultiplexer.Connect($"{Host},connectTimeout=2000,abortConnect=true");
            SkipReason = "";
        }
        catch (Exception ex)
        {
            SkipReason = $"redis not available at {Host}: {ex.Message}";
        }
    }

    public void Dispose() => Connection?.Dispose();
}
