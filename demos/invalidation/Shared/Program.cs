// Program.cs - shared VERBATIM by both demo projects: create two independent "web servers"
// (each with its own near cache), sharing one redis and one database; run the identical
// scripted scenario. The ONLY file that differs between the two projects is Setup.cs.

using InvalidationDemo;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

var database = new Database();

ServiceProvider serverA, serverB;
try
{
    serverA = await Setup.CreateServerAsync();
    serverB = await Setup.CreateServerAsync();
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
    await Scenario.RunAsync(Setup.Flavor, Setup.Description, a, b, database);
}

return 0;
