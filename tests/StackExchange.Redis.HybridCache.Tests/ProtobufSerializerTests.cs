using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ProtoBuf;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

/// <summary>
/// Validates that the protobuf-net HybridCache hooks (protobuf-net.HybridCache's
/// <c>IHybridCacheSerializerFactory</c>/<c>IHybridCacheSerializer&lt;T&gt;</c> registrations)
/// plug into our serializer resolution exactly as they do into the default implementation —
/// including cross-implementation envelope interop with protobuf payloads.
/// </summary>
public class ProtobufSerializerTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public ProtobufSerializerTests(RedisFixture fx) => _fx = fx;

    [ProtoContract]
    public sealed class ProtoPoco
    {
        [ProtoMember(1)]
        public string? JsonWouldEmitThisName { get; set; }

        [ProtoMember(2)]
        public int Value { get; set; }
    }

    public sealed record PlainPoco(string Name, int Value); // no [ProtoContract]: must fall through to JSON

    private ServiceProvider CreateOurs(string prefix)
        => new ServiceCollection()
            .AddSingleton<IConnectionMultiplexer>(_fx.Connection!)
            .AddProtobufNetHybridCacheSerializer()
            .AddRedisHybridCache(o => o.KeyPrefix = prefix)
            .BuildServiceProvider();

    [Fact]
    public async Task ProtoContractType_UsesProtobuf_NotJson()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = RedisFixture.NewPrefix();
        await using var provider = CreateOurs(prefix);
        var cache = provider.GetRequiredService<HybridCacheBase>();

        var stored = new ProtoPoco { JsonWouldEmitThisName = "proto", Value = 42 };
        await cache.SetAsync("pb1", stored, cancellationToken: TestContext.Current.CancellationToken);

        var fetched = await cache.GetOrCreateAsync(
            "pb1", _ => new ValueTask<ProtoPoco>(new ProtoPoco { JsonWouldEmitThisName = "factory-must-not-run" }),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("proto", fetched.JsonWouldEmitThisName);
        Assert.Equal(42, fetched.Value);

        // the stored payload must be protobuf, not JSON: STJ would emit the property name
        byte[] blob = (byte[])(await _fx.Db.StringGetAsync(prefix + "pb1"))!;
        byte[] jsonMarker = Encoding.UTF8.GetBytes("JsonWouldEmitThisName");
        Assert.DoesNotContain(jsonMarker, Chunks(blob, jsonMarker.Length));

        static byte[][] Chunks(byte[] haystack, int width)
            => Enumerable.Range(0, Math.Max(0, haystack.Length - width + 1))
                .Select(i => haystack.AsSpan(i, width).ToArray())
                .ToArray();
    }

    [Fact]
    public async Task NonContractType_FallsThroughToJson()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        await using var provider = CreateOurs(RedisFixture.NewPrefix());
        var cache = provider.GetRequiredService<HybridCacheBase>();

        await cache.SetAsync("pj1", new PlainPoco("plain", 7), cancellationToken: TestContext.Current.CancellationToken);
        var fetched = await cache.GetOrCreateAsync(
            "pj1", _ => new ValueTask<PlainPoco>(new PlainPoco("factory-must-not-run", -1)),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(new PlainPoco("plain", 7), fetched);
    }

    [Fact]
    public async Task ProtobufPayloads_InteropWithStock_BothDirections()
    {
        Assert.SkipWhen(_fx.Connection is null, _fx.SkipReason);
        var prefix = RedisFixture.NewPrefix();
        await using var ours = CreateOurs(prefix);
        var (stockProvider, stock, stub) = StockHarness.Create(s => s.AddProtobufNetHybridCacheSerializer());
        await using (stockProvider)
        {
            var cache = ours.GetRequiredService<HybridCacheBase>();

            // stock writes protobuf; we read it
            await stock.SetAsync("x1", new ProtoPoco { JsonWouldEmitThisName = "stock-proto", Value = 1 }, cancellationToken: TestContext.Current.CancellationToken);
            await _fx.Db.StringSetAsync(prefix + "x1", stub.Store["x1"]);
            var fromStock = await cache.GetOrCreateAsync(
                "x1", _ => new ValueTask<ProtoPoco>(new ProtoPoco { JsonWouldEmitThisName = "factory-must-not-run" }),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal("stock-proto", fromStock.JsonWouldEmitThisName);

            // we write protobuf; stock reads it
            await cache.SetAsync("x2", new ProtoPoco { JsonWouldEmitThisName = "our-proto", Value = 2 }, cancellationToken: TestContext.Current.CancellationToken);
            stub.Store["x2"] = (byte[])(await _fx.Db.StringGetAsync(prefix + "x2"))!;
            var fromUs = await stock.GetOrCreateAsync(
                "x2", _ => new ValueTask<ProtoPoco>(new ProtoPoco { JsonWouldEmitThisName = "factory-must-not-run" }),
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal("our-proto", fromUs.JsonWouldEmitThisName);
        }
    }
}
