// End-to-end event emission from real cache operations (issue #3): a read answered by the local
// cache (synchronous completion) is LocalCacheHit; a round trip is LocalCacheMiss + the
// distributed events. The EventSource is process-global, so these tests run in the serialized
// EventSource collection (no other tests execute while we listen); the assertions stay
// positive-only - the expected events must appear, stragglers are tolerated.

using System;
using System.Collections.Concurrent;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis.HybridCache.Internal;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

[Collection(nameof(EventSourceCollection))]
public class EventFlowTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public EventFlowTests(RedisFixture fx) => _fx = fx;

    [Fact]
    public async Task ReadPath_EmitsLocalAndDistributedEvents()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        using var listener = new FlowListener();

        // Only one EventSource instance owns a name, and since issue #14 the process has only
        // one (the Log singleton; TestEventListener listens to it rather than to a private
        // same-named instance), so this enable must stick - failure here is a real bug
        Assert.True(HybridCacheEventSource.Log.IsEnabled(), "the process-global event source failed to enable");

        await using var provider = new ServiceCollection()
            .AddSingleton<IConnectionMultiplexer>(_fx.CachedConnection!)
            .AddRedisHybridCache(o => o.KeyPrefix = RedisFixture.NewPrefix())
            .BuildServiceProvider();
        var cache = provider.GetRequiredService<HybridCacheBase>();
        var ct = TestContext.Current.CancellationToken;

        // cold: local miss, distributed miss, factory, distributed write
        listener.Clear();
        Assert.Equal(42, await cache.GetOrCreateAsync("flow", _ => new ValueTask<int>(42), cancellationToken: ct));
        listener.AssertContains(HybridCacheEventSource.EventIdLocalCacheMiss);
        listener.AssertContains(HybridCacheEventSource.EventIdDistributedCacheGet);
        listener.AssertContains(HybridCacheEventSource.EventIdDistributedCacheMiss);
        listener.AssertContains(HybridCacheEventSource.EventIdUnderlyingDataQueryStart);
        listener.AssertContains(HybridCacheEventSource.EventIdDistributedCacheWrite);

        // the write does not populate the local cache; this round trip does
        listener.Clear();
        Assert.Equal(42, await cache.GetOrCreateAsync("flow", _ => new ValueTask<int>(-1), cancellationToken: ct));
        listener.AssertContains(HybridCacheEventSource.EventIdDistributedCacheHit);

        // warm: answered by the local cache
        listener.Clear();
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(42, await cache.GetOrCreateAsync("flow", _ => new ValueTask<int>(-1), cancellationToken: ct));
        }

        listener.AssertContains(HybridCacheEventSource.EventIdLocalCacheHit);
    }

    private sealed class FlowListener : EventListener
    {
        private readonly ConcurrentQueue<int> _ids = new();

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            // by name: this can fire from inside the Log field's own static initializer, when
            // HybridCacheEventSource.Log is still null (so a ReferenceEquals check never
            // matches); OnEventWritten filters to the singleton, by then initialized.
            if (eventSource.Name == "StackExchange-Redis-HybridCache")
            {
                EnableEvents(eventSource, EventLevel.LogAlways);
            }

            base.OnEventSourceCreated(eventSource);
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            if (ReferenceEquals(eventData.EventSource, HybridCacheEventSource.Log))
            {
                _ids.Enqueue(eventData.EventId);
            }

            base.OnEventWritten(eventData);
        }

        public void Clear()
        {
            while (_ids.TryDequeue(out _))
            {
            }
        }

        public void AssertContains(int eventId)
            => Assert.Contains(eventId, _ids.ToArray());
    }
}
