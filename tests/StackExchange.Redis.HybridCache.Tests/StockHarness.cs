using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

/// <summary>
/// Builds the REAL default implementation (Microsoft.Extensions.Caching.Hybrid) over an
/// in-memory <see cref="IDistributedCache"/> stub, so its raw envelope bytes can be captured
/// and planted on either side. The stub is a custom type deliberately: a real
/// MemoryDistributedCache registration would be detected and ignored as L2.
/// </summary>
internal static class StockHarness
{
    public static (ServiceProvider Provider, HybridCacheBase Cache, StubDistributedCache L2) Create(Action<IServiceCollection>? configure = null)
    {
        var stub = new StubDistributedCache();
        var services = new ServiceCollection().AddSingleton<IDistributedCache>(stub);
        configure?.Invoke(services);
        var provider = services
            .AddHybridCache(o => o.DefaultEntryOptions = new HybridCacheEntryOptions
            {
                // force stock through its L2 so the envelope bytes are observable
                Flags = HybridCacheEntryFlags.DisableLocalCacheRead | HybridCacheEntryFlags.DisableLocalCacheWrite,
            })
            .Services
            .BuildServiceProvider();
        return (provider, provider.GetRequiredService<HybridCacheBase>(), stub);
    }

    public sealed class StubDistributedCache : IDistributedCache
    {
        public ConcurrentDictionary<string, byte[]> Store { get; } = new();

        public byte[]? Get(string key) => Store.TryGetValue(key, out var value) ? value : null;

        public Task<byte[]?> GetAsync(string key, CancellationToken token = default) => Task.FromResult(Get(key));

        public void Set(string key, byte[] value, DistributedCacheEntryOptions options) => Store[key] = value;

        public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
        {
            Set(key, value, options);
            return Task.CompletedTask;
        }

        public void Refresh(string key)
        {
        }

        public Task RefreshAsync(string key, CancellationToken token = default) => Task.CompletedTask;

        public void Remove(string key) => Store.TryRemove(key, out _);

        public Task RemoveAsync(string key, CancellationToken token = default)
        {
            Remove(key);
            return Task.CompletedTask;
        }
    }
}
