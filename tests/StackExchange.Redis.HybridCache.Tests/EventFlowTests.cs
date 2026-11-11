// End-to-end event emission from real cache operations (issue #3): a read answered by the local
// cache (synchronous completion) is LocalCacheHit; a round trip is LocalCacheMiss + the
// distributed events. The EventSource is process-global and other test classes run in parallel
// (their caches also emit while we listen), so these assertions are positive-only: the expected
// events must appear, and foreign events are tolerated. The zero-alloc tests skip themselves
// while any listener is active, so enabling one here is safe.

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

public class EventFlowTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fx;

    public EventFlowTests(RedisFixture fx) => _fx = fx;

    [Fact]
    public async Task ReadPath_EmitsLocalAndDistributedEvents()
    {
        Assert.SkipWhen(_fx.CachedConnection is null, _fx.CachedSkipReason);

        using var listener = new FlowListener();

        // Only one EventSource instance can own a name: a second same-named instance cannot be
        // enabled at all (verified standalone). HybridCacheEventSourceTests' fixture creates a
        // private same-named instance, so in a full parallel run whichever is constructed second
        // is inert - when the live Log lost that race, this test cannot observe anything and
        // skips. It runs fully in isolation (and in runs where the live source wins the name).
        Assert.SkipWhen(!HybridCacheEventSource.Log.IsEnabled(), "another HybridCacheEventSource instance owns the source name in this process");

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
            // matches). TestEventListener's private same-named instance gets enabled too, which
            // is harmless: OnEventWritten filters to the live instance, by then initialized.
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
