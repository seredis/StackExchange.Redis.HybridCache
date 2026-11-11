// Program.cs - run the identical scripted scenario against one or both cache modes:
//
//   dotnet run              both modes, back to back, with a side-by-side comparison
//   dotnet run -- stock     the in-box HybridCache over IDistributedCache (passive invalidation)
//   dotnet run -- push      StackExchange.Redis.HybridCache (active invalidation)
//
// Each mode gets two independent "web servers" (each with its own near cache), sharing one
// redis and one database. The ONLY file that differs between the modes is Setup.cs.

using InvalidationDemo;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

CacheMode[] modes = args switch
{
    [] => [CacheMode.Stock, CacheMode.Push],
    ["stock"] => [CacheMode.Stock],
    ["push" or "redis" or "hybrid"] => [CacheMode.Push],
    _ => [],
};

if (modes.Length == 0)
{
    Console.Error.WriteLine("usage: dotnet run [-- stock|push]   (no argument: both modes, compared)");
    return 2;
}

var comparison = new List<(string Header, List<(string What, string A, string B)> Rows, string Queries)>();

foreach (var mode in modes)
{
    var database = new Database(); // fresh per mode: each starts from the same state

    ServiceProvider serverA, serverB;
    try
    {
        serverA = await Setup.CreateServerAsync(mode);
        serverB = await Setup.CreateServerAsync(mode);
    }
    catch (RedisConnectionException ex)
    {
        Console.Error.WriteLine($"cannot reach redis: {ex.Message}");
        Console.Error.WriteLine("start it first, from demos/invalidation: docker compose up -d --wait");
        return 1;
    }

    await using (serverA)
    await using (serverB)
    {
        var a = new CatalogService("server A", serverA.GetRequiredService<HybridCache>(), database);
        var b = new CatalogService("server B", serverB.GetRequiredService<HybridCache>(), database);
        var rows = await Scenario.RunAsync(Setup.Flavor(mode), Setup.Description(mode), a, b, database);
        comparison.Add((Setup.Header(mode), rows, $"A {a.DatabaseQueries}, B {b.DatabaseQueries}"));
    }

    Console.WriteLine();
}

// the summary: one column pair per mode - side by side when both ran
Console.WriteLine("== time until each server served the new value ==");
Console.Write($"   {"",-42}");
foreach (var (header, _, _) in comparison)
{
    Console.Write($"{header,21}   ");
}

Console.WriteLine();
Console.Write($"   {"",-42}");
foreach (var _ in comparison)
{
    Console.Write($"{"server A",10}{"server B",11}   ");
}

Console.WriteLine();
for (int i = 0; i < comparison[0].Rows.Count; i++)
{
    Console.Write($"   {comparison[0].Rows[i].What,-42}");
    foreach (var (_, rows, _) in comparison)
    {
        Console.Write($"{rows[i].A,10}{rows[i].B,11}   ");
    }

    Console.WriteLine();
}

Console.Write($"   {"database queries",-42}");
foreach (var (_, _, queries) in comparison)
{
    Console.Write($"{queries,21}   ");
}

Console.WriteLine();
return 0;
