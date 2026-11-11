// Database.cs - a stand-in for the system of record: an in-process table with a pretend query
// cost. Both "web servers" share one instance, exactly as they would share one database.

namespace InvalidationDemo;

sealed record Product(int Id, string Name, decimal Price, int Version)
{
    public override string ToString() => $"{Name} ${Price:0.00} (v{Version})";
}

sealed class Database
{
    private readonly Lock _lock = new();
    private readonly Dictionary<int, Product> _rows = [];

    public Product Save(Product product)
    {
        lock (_lock)
        {
            _rows[product.Id] = product;
        }

        return product;
    }

    public Product Get(int id)
    {
        lock (_lock)
        {
            return _rows[id];
        }
    }

    /// <summary>The next version of a product: price changed, version bumped. Not yet saved.</summary>
    public Product NextVersion(int id, decimal newPrice)
    {
        var current = Get(id);
        return current with { Price = newPrice, Version = current.Version + 1 };
    }

    public async ValueTask<Product> LoadAsync(int id, CancellationToken cancellationToken)
    {
        await Task.Delay(20, cancellationToken); // a pretend query cost
        return Get(id);
    }
}
