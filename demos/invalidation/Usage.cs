// Usage.cs - how the application consumes the cache. This file is identical for both modes:
// everything here is the standard HybridCache abstraction, so the code cannot tell which
// implementation it is running on. The implementations are swapped in Setup.cs, and
// nowhere else.

using Microsoft.Extensions.Caching.Hybrid;

namespace InvalidationDemo;

/// <summary>A product catalogue read through the cache: the classic read-through pattern.</summary>
sealed class CatalogService(string name, HybridCache cache, Database database)
{
    public string Name => name;

    private int _databaseQueries;

    /// <summary>How many times the factory actually ran (i.e. the cache did not have an answer).</summary>
    public int DatabaseQueries => Volatile.Read(ref _databaseQueries);

    /// <summary>Read-through: served from cache when possible, loaded from the database otherwise.</summary>
    public async ValueTask<Product> GetProductAsync(int id, CancellationToken cancellationToken = default)
        => await cache.GetOrCreateAsync(
            $"product/{id}",
            async token => await LoadFromDatabaseAsync(id, token),
            tags: ["catalog"],
            cancellationToken: cancellationToken);

    /// <summary>An update: write the new truth to the database, then through the cache.</summary>
    public async ValueTask UpdateProductAsync(Product product, CancellationToken cancellationToken = default)
    {
        database.Save(product);
        await cache.SetAsync($"product/{product.Id}", product, tags: ["catalog"], cancellationToken: cancellationToken);
    }

    /// <summary>An eviction: the cached entry is wrong (say the database was changed directly);
    /// remove it everywhere and let the next read reload it.</summary>
    public async ValueTask RemoveProductAsync(int id, CancellationToken cancellationToken = default)
        => await cache.RemoveAsync($"product/{id}", cancellationToken);

    /// <summary>A bulk change (think: a new price list just imported): every cached product is now wrong.</summary>
    public async ValueTask InvalidateCatalogAsync(CancellationToken cancellationToken = default)
        => await cache.RemoveByTagAsync("catalog", cancellationToken);

    private async ValueTask<Product> LoadFromDatabaseAsync(int id, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _databaseQueries);
        return await database.LoadAsync(id, cancellationToken);
    }
}
