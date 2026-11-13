// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Ported from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid.Tests/HybridCacheEventSourceTests.cs);
// the source is renamed StackExchange-Redis-HybridCache (GUID derives from the name), with the same
// event IDs and counter names.

using System;
using System.Threading.Tasks;

using System.Diagnostics.Tracing;
using StackExchange.Redis.HybridCache.Internal;
using Xunit;

namespace StackExchange.Redis.HybridCache.Tests;

[Collection(nameof(EventSourceCollection))]
public class HybridCacheEventSourceTests(ITestOutputHelper log, TestEventListener listener) : IClassFixture<TestEventListener>
{
    // see notes in TestEventListener for context on fixture usage; the collection serializes
    // these tests against the rest of the suite, because the listener observes the
    // process-global source that every cache operation emits into

    [Fact]
    public void MatchesNameAndGuid()
    {
        // Assert
        Assert.Equal("StackExchange-Redis-HybridCache", listener.Source.Name);
        Assert.Equal(Guid.Parse("699f7d36-cc8d-50d0-0ac1-a50181f58e35"), listener.Source.Guid); // from name
    }

    [Fact]
    public async Task LocalCacheHit()
    {
        AssertEnabled();

        listener.Reset().Source.LocalCacheHit();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdLocalCacheHit, "LocalCacheHit", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-local-cache-hits", "Total Local Cache Hits", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task LocalCacheMiss()
    {
        AssertEnabled();

        listener.Reset().Source.LocalCacheMiss();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdLocalCacheMiss, "LocalCacheMiss", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-local-cache-misses", "Total Local Cache Misses", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task DistributedCacheGet()
    {
        AssertEnabled();

        listener.Reset().Source.DistributedCacheGet();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdDistributedCacheGet, "DistributedCacheGet", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("current-distributed-cache-fetches", "Current Distributed Cache Fetches", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task DistributedCacheHit()
    {
        AssertEnabled();

        listener.Reset().Source.DistributedCacheGet();
        listener.Reset(resetCounters: false).Source.DistributedCacheHit();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdDistributedCacheHit, "DistributedCacheHit", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-distributed-cache-hits", "Total Distributed Cache Hits", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task DistributedCacheMiss()
    {
        AssertEnabled();

        listener.Reset().Source.DistributedCacheGet();
        listener.Reset(resetCounters: false).Source.DistributedCacheMiss();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdDistributedCacheMiss, "DistributedCacheMiss", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-distributed-cache-misses", "Total Distributed Cache Misses", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task DistributedCacheFailed()
    {
        AssertEnabled();

        listener.Reset().Source.DistributedCacheGet();
        listener.Reset(resetCounters: false).Source.DistributedCacheFailed();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdDistributedCacheFailed, "DistributedCacheFailed", EventLevel.Error);

        await AssertCountersAsync();
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task DistributedCacheCanceled()
    {
        AssertEnabled();

        listener.Reset().Source.DistributedCacheGet();
        listener.Reset(resetCounters: false).Source.DistributedCacheCanceled();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdDistributedCacheCanceled, "DistributedCacheCanceled", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task UnderlyingDataQueryStart()
    {
        AssertEnabled();

        listener.Reset().Source.UnderlyingDataQueryStart();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdUnderlyingDataQueryStart, "UnderlyingDataQueryStart", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("current-data-query", "Current Data Queries", 1);
        listener.AssertCounter("total-data-query", "Total Data Queries", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task UnderlyingDataQueryComplete()
    {
        AssertEnabled();

        listener.Reset().Source.UnderlyingDataQueryStart();
        listener.Reset(resetCounters: false).Source.UnderlyingDataQueryComplete();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdUnderlyingDataQueryComplete, "UnderlyingDataQueryComplete", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-data-query", "Total Data Queries", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task UnderlyingDataQueryFailed()
    {
        AssertEnabled();

        listener.Reset().Source.UnderlyingDataQueryStart();
        listener.Reset(resetCounters: false).Source.UnderlyingDataQueryFailed();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdUnderlyingDataQueryFailed, "UnderlyingDataQueryFailed", EventLevel.Error);

        await AssertCountersAsync();
        listener.AssertCounter("total-data-query", "Total Data Queries", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task UnderlyingDataQueryCanceled()
    {
        AssertEnabled();

        listener.Reset().Source.UnderlyingDataQueryStart();
        listener.Reset(resetCounters: false).Source.UnderlyingDataQueryCanceled();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdUnderlyingDataQueryCanceled, "UnderlyingDataQueryCanceled", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-data-query", "Total Data Queries", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task LocalCacheWrite()
    {
        AssertEnabled();

        listener.Reset().Source.LocalCacheWrite();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdLocalCacheWrite, "LocalCacheWrite", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-local-cache-writes", "Total Local Cache Writes", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task DistributedCacheWrite()
    {
        AssertEnabled();

        listener.Reset().Source.DistributedCacheWrite();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdDistributedCacheWrite, "DistributedCacheWrite", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-distributed-cache-writes", "Total Distributed Cache Writes", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task StampedeJoin()
    {
        AssertEnabled();

        listener.Reset().Source.StampedeJoin();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdStampedeJoin, "StampedeJoin", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-stampede-joins", "Total Stampede Joins", 1);
        listener.AssertRemainingCountersZero();
    }

    [Fact]
    public async Task TagInvalidated()
    {
        AssertEnabled();

        listener.Reset().Source.TagInvalidated();
        listener.AssertSingleEvent(HybridCacheEventSource.EventIdTagInvalidated, "TagInvalidated", EventLevel.Verbose);

        await AssertCountersAsync();
        listener.AssertCounter("total-tag-invalidations", "Total Tag Invalidations", 1);
        listener.AssertRemainingCountersZero();
    }

    private void AssertEnabled()
    {
        // including this data for visibility when tests fail - ETW subsystem can be ... weird
        log.WriteLine($".NET {Environment.Version} on {Environment.OSVersion}, {IntPtr.Size * 8}-bit");

        if (!listener.Source.IsEnabled() && listener.SourceMessages is { Count: > 0 } messages)
        {
            // not a quiet environmental skip: the source reported an internal error - name it
            Assert.Fail($"event source failed to enable: {string.Join(" | ", messages)}");
        }

        Assert.SkipUnless(listener.Source.IsEnabled(), "Event source not enabled");
    }

    private async Task AssertCountersAsync()
    {
        var count = await listener.TryAwaitCountersAsync();

        // ETW counters timing can be painfully unpredictable; generally
        // it'll work fine locally, especially on modern .NET, but:
        // CI servers and netfx in particular - not so much. The tests
        // can still observe and validate the simple events, though, which
        // should be enough to be credible that the eventing system is
        // fundamentally working. We're not meant to be testing that
        // the counters system *itself* works!

        if (count == 0)
        {
            Assert.Skip("No counters received");
        }
    }
}
