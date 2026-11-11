// Scenario.cs - the scripted demonstration (demo machinery, not example code; the files worth
// reading are Setup.cs and Usage.cs). Two "web servers" share one redis and one database.
// Three escalating scenarios, identical for both implementations:
//
//   1. server A updates a product through the cache (SetAsync);
//   2. the database changes behind the cache and server A evicts the key (RemoveAsync),
//      so the next read - wherever it lands - reloads from the database;
//   3. the price list changes and server A invalidates the 'catalog' tag, then keeps reading
//      (so redis gets re-populated by A's next read);
//   4. the same again, but server A goes quiet after invalidating - nothing re-populates redis.
//
// Each time, the measurement is how long SERVER B keeps serving the old value.

using System.Diagnostics;

namespace InvalidationDemo;

static class Scenario
{
    private const int ProductId = 1;
    private static readonly TimeSpan WatchLimit = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(40);

    public static async Task RunAsync(string flavor, string description, CatalogService a, CatalogService b, Database database)
    {
        Console.WriteLine($"== {flavor} ==");
        Console.WriteLine(description);
        Console.WriteLine();

        database.Save(new Product(ProductId, "coffee", 10.00m, Version: 1));

        Console.WriteLine("-- populate: both servers read product 1 --");
        Console.WriteLine($"           {a.Name} serves {await a.GetProductAsync(ProductId)}");
        Console.WriteLine($"           {b.Name} serves {await b.GetProductAsync(ProductId)}");
        await Task.Delay(1000); // both near caches now hold a warm copy

        var summary = new List<(string What, string A, string B)>();

        // 1. a single update, written through the cache
        Console.WriteLine();
        Console.WriteLine($"-- 1. update: {a.Name} writes a new price through the cache --");
        var updated = database.NextVersion(ProductId, 12.00m);
        await a.UpdateProductAsync(updated);
        Console.WriteLine($"    0.00 s  {a.Name}: SetAsync {updated}");
        var r1 = await WatchAsync([a, b], updated.Version);
        summary.Add(("1. update (SetAsync)", Format(r1[0]), Format(r1[1])));

        // 2. key-based invalidation: the database changed behind the cache; evict the key
        // everywhere, and the next read (wherever it lands) reloads from the database
        Console.WriteLine();
        Console.WriteLine($"-- 2. remove: the database changed behind the cache; {a.Name} evicts product 1 by key --");
        var evicted = database.Save(database.NextVersion(ProductId, 14.00m));
        await a.RemoveProductAsync(ProductId);
        Console.WriteLine($"    0.00 s  database now holds {evicted}; {a.Name}: RemoveAsync product/1");
        var r2 = await WatchAsync([a, b], evicted.Version);
        summary.Add(("2. remove (RemoveAsync)", Format(r2[0]), Format(r2[1])));

        // 3. a bulk change: invalidate by tag; the invalidating server keeps reading
        Console.WriteLine();
        Console.WriteLine($"-- 3. new price list: {a.Name} invalidates tag 'catalog', and keeps reading --");
        var reloaded = database.Save(database.NextVersion(ProductId, 16.00m));
        await a.InvalidateCatalogAsync();
        Console.WriteLine($"    0.00 s  database now holds {reloaded}; {a.Name}: RemoveByTagAsync(\"catalog\")");
        var r3 = await WatchAsync([a, b], reloaded.Version);
        summary.Add(("3. tag invalidation", Format(r3[0]), Format(r3[1])));

        // 4. the same, but the invalidating server goes quiet: nothing re-populates redis,
        // so the only way server B can learn is from the invalidation itself
        Console.WriteLine();
        Console.WriteLine($"-- 4. new price list again, but {a.Name} goes quiet after invalidating --");
        var final = database.Save(database.NextVersion(ProductId, 18.00m));
        await a.InvalidateCatalogAsync();
        Console.WriteLine($"    0.00 s  database now holds {final}; {a.Name}: RemoveByTagAsync(\"catalog\"), then silence");
        var r4 = await WatchAsync([null, b], final.Version);
        summary.Add(("4. tag invalidation, invalidator quiet", "(quiet)", Format(r4[1])));

        Console.WriteLine();
        Console.WriteLine("== time until each server served the new value ==");
        Console.WriteLine($"   {"",-40}{a.Name,14}{b.Name,14}");
        foreach (var (what, resultA, resultB) in summary)
        {
            Console.WriteLine($"   {what,-40}{resultA,14}{resultB,14}");
        }

        Console.WriteLine();
        Console.WriteLine($"   database queries: {a.Name} {a.DatabaseQueries}, {b.Name} {b.DatabaseQueries}");
    }

    /// <summary>Polls each non-null service until it serves <paramref name="expectedVersion"/> (or the
    /// watch limit passes), printing every transition; returns each one's time-to-fresh, null = never.</summary>
    private static async Task<TimeSpan?[]> WatchAsync(CatalogService?[] watched, int expectedVersion)
    {
        var lastVersion = new int[watched.Length];
        for (int i = 0; i < watched.Length; i++)
        {
            lastVersion[i] = expectedVersion - 1; // what everyone served before the change
        }

        var freshAfter = new TimeSpan?[watched.Length];
        long started = Stopwatch.GetTimestamp();
        while (true)
        {
            bool pending = false;
            for (int i = 0; i < watched.Length; i++)
            {
                if (watched[i] is not { } service || freshAfter[i] is not null)
                {
                    continue;
                }

                var product = await service.GetProductAsync(ProductId);
                var elapsed = Stopwatch.GetElapsedTime(started);
                if (product.Version != lastVersion[i])
                {
                    Console.WriteLine($"   {elapsed.TotalSeconds,5:0.00} s  {service.Name} serves {product}");
                    lastVersion[i] = product.Version;
                }

                if (product.Version >= expectedVersion)
                {
                    freshAfter[i] = elapsed;
                }
                else
                {
                    pending = true;
                }
            }

            if (!pending)
            {
                return freshAfter;
            }

            if (Stopwatch.GetElapsedTime(started) > WatchLimit)
            {
                for (int i = 0; i < watched.Length; i++)
                {
                    if (watched[i] is { } service && freshAfter[i] is null)
                    {
                        Console.WriteLine($"   {WatchLimit.TotalSeconds,5:0.00} s  {service.Name} STILL serves the old value; giving up");
                    }
                }

                return freshAfter;
            }

            await Task.Delay(PollInterval);
        }
    }

    private static string Format(TimeSpan? freshAfter)
        => freshAfter is { } t ? $"{t.TotalSeconds:0.00} s" : $"> {WatchLimit.TotalSeconds:0} s";
}
