// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Ported from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid.Tests/StampedeTests.cs).
// Adaptations: the backend here is a single IConnectionMultiplexer rather than L1+L2 services, so
// the "everything disabled, backend must never be touched" harness uses the throwing
// PlaceholderMultiplexer; GetOrCreateStampedeState/ExecuteDirectAsync carry the extra
// skip-initial-read plumbing (see RedisHybridCache.Stampede.cs).

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis.HybridCache.Internal;
using Xunit;

using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache.Tests;

public class StampedeTests
{
    private static ServiceProvider GetDefaultCache(out RedisHybridCache cache)
    {
        var services = new ServiceCollection();
        services.AddSingleton(PlaceholderMultiplexer.Instance); // throws if the backend is ever touched
        services.AddRedisHybridCache(options =>
        {
            options.DefaultEntryOptions = new()
            {
                Flags = HybridCacheEntryFlags.DisableDistributedCache | HybridCacheEntryFlags.DisableLocalCache,
            };
        });
        ServiceProvider provider = services.BuildServiceProvider();
        cache = Assert.IsType<RedisHybridCache>(provider.GetRequiredService<HybridCacheBase>());
        return provider;
    }

    [Fact]
    public void ToString_Key()
    {
        using var provider = GetDefaultCache(out RedisHybridCache cache);

        var key = new RedisHybridCache.StampedeKey("test_key", HybridCacheEntryFlags.DisableLocalCache);

        const string Expected = "test_key (DisableLocalCache)";

        Assert.Equal(Expected, key.ToString());

        var state = new RedisHybridCache.StampedeState<string, string>(cache, in key, TagSet.Empty, true);
        Assert.Equal(Expected, state.ToString());
    }

    [Fact]
    public async Task CompletedOldOperationDoesNotRemoveReplacement()
    {
        using var scope = GetDefaultCache(out RedisHybridCache cache);
        const string Key = "test_key";
        const HybridCacheEntryFlags Flags = HybridCacheEntryFlags.DisableDistributedCache | HybridCacheEntryFlags.DisableLocalCache;
        var oldStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseOld = new SemaphoreSlim(0);

        Assert.True(cache.GetOrCreateStampedeState<int, int>(Key, Flags, out RedisHybridCache.StampedeState<int, int> old, true, null, out _));

        int state = 0;
        Task oldExecution = old.ExecuteDirectAsync(in state, async (_, cancellationToken) =>
        {
            oldStarted.SetResult(true);
            await releaseOld.WaitAsync(CancellationToken.None);
            cancellationToken.ThrowIfCancellationRequested();
            return 1;
        }, null, skipInitialRead: true);

        await oldStarted.Task;
        old.CancelCaller();

        Assert.True(cache.GetOrCreateStampedeState<int, int>(Key, Flags, out RedisHybridCache.StampedeState<int, int> replacement, false, null, out _));
        Assert.Equal(1, cache.DebugGetCallerCount(Key, Flags));

        releaseOld.Release();
        await oldExecution; // BackgroundFetchAsync routes faults to the shared result; the task itself completes

        Assert.Equal(1, cache.DebugGetCallerCount(Key, Flags));
        Assert.False(cache.GetOrCreateStampedeState<int, int>(Key, Flags, out RedisHybridCache.StampedeState<int, int> joined, false, null, out _));
        Assert.Same(replacement, joined);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(10, false)]
    [InlineData(10, true)]
    public async Task MultipleCallsShareExecution_NoCancellation(int callerCount, bool canBeCanceled)
    {
        using var scope = GetDefaultCache(out RedisHybridCache cache);
        using var semaphore = new SemaphoreSlim(0);

        using var cts = canBeCanceled ? new CancellationTokenSource() : null;
        CancellationToken token = cts?.Token ?? CancellationToken.None;

        int executeCount = 0;
        int cancelCount = 0;
        var results = new Task<Guid>[callerCount];
        for (int i = 0; i < callerCount; i++)
        {
            results[i] = cache.GetOrCreateAsync(Me(), async ct =>
            {
                using CancellationTokenRegistration reg = ct.Register(() => Interlocked.Increment(ref cancelCount));
                if (!await semaphore.WaitAsync(5_000, CancellationToken.None))
                {
                    throw new TimeoutException("Failed to activate");
                }

                Interlocked.Increment(ref executeCount);
                ct.ThrowIfCancellationRequested(); // assert not cancelled
                return Guid.NewGuid();
            }, cancellationToken: token).AsTask();
        }

        Assert.Equal(callerCount, cache.DebugGetCallerCount(Me()));

        // everyone is queued up; release the hounds and check
        // that we all got the same result
        Assert.Equal(0, Volatile.Read(ref executeCount));
        Assert.Equal(0, Volatile.Read(ref cancelCount));
        semaphore.Release();
        Guid first = await results[0];
        Assert.Equal(1, Volatile.Read(ref executeCount));
        Assert.Equal(0, Volatile.Read(ref cancelCount));
        foreach (Task<Guid> result in results)
        {
            Assert.Equal(first, await result);
        }

        Assert.Equal(1, Volatile.Read(ref executeCount));
        Assert.Equal(0, Volatile.Read(ref cancelCount));

        // and do it a second time; we expect different results
        Volatile.Write(ref executeCount, 0);
        for (int i = 0; i < callerCount; i++)
        {
            results[i] = cache.GetOrCreateAsync(Me(), async ct =>
            {
                using CancellationTokenRegistration reg = ct.Register(() => Interlocked.Increment(ref cancelCount));
                if (!await semaphore.WaitAsync(5_000, CancellationToken.None))
                {
                    throw new TimeoutException("Failed to activate");
                }

                Interlocked.Increment(ref executeCount);
                ct.ThrowIfCancellationRequested(); // assert not cancelled
                return Guid.NewGuid();
            }, cancellationToken: token).AsTask();
        }

        Assert.Equal(callerCount, cache.DebugGetCallerCount(Me()));

        Assert.Equal(0, Volatile.Read(ref executeCount));
        Assert.Equal(0, Volatile.Read(ref cancelCount));
        semaphore.Release();
        Guid second = await results[0];
        Assert.NotEqual(first, second);
        Assert.Equal(1, Volatile.Read(ref executeCount));
        Assert.Equal(0, Volatile.Read(ref cancelCount));
        foreach (Task<Guid> result in results)
        {
            Assert.Equal(second, await result);
        }

        Assert.Equal(1, Volatile.Read(ref executeCount));
        Assert.Equal(0, Volatile.Read(ref cancelCount));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    public async Task MultipleCallsShareExecution_EveryoneCancels(int callerCount)
    {
        // what we want to prove here is that everyone ends up cancelling promptly by
        // *their own* cancellation (not dependent on the shared task), and that
        // the shared task becomes cancelled (which can be later)

        using var scope = GetDefaultCache(out RedisHybridCache cache);
        using var semaphore = new SemaphoreSlim(0);

        int executeCount = 0;
        int cancelCount = 0;
        var results = new Task<Guid>[callerCount];
        var cancels = new CancellationTokenSource[callerCount];
        for (int i = 0; i < callerCount; i++)
        {
            cancels[i] = new CancellationTokenSource();
            results[i] = cache.GetOrCreateAsync(Me(), async ct =>
            {
                using CancellationTokenRegistration reg = ct.Register(() => Interlocked.Increment(ref cancelCount));
                if (!await semaphore.WaitAsync(5_000, CancellationToken.None))
                {
                    throw new TimeoutException("Failed to activate");
                }

                try
                {
                    Interlocked.Increment(ref executeCount);
                    ct.ThrowIfCancellationRequested();
                    return Guid.NewGuid();
                }
                finally
                {
                    semaphore.Release(); // handshake so we can check when available again
                }
            }, cancellationToken: cancels[i].Token).AsTask();
        }

        Assert.Equal(callerCount, cache.DebugGetCallerCount(Me()));

        foreach (CancellationTokenSource cancel in cancels)
        {
            cancel.Cancel();
        }

        // allow the shared underlying task time to enter its semaphore wait
        await Task.Delay(500);

        for (int i = 0; i < callerCount; i++)
        {
            Task<Guid> result = results[i];

            // cancellation happens on a worker; wait up to 5 seconds total for the caller's task to observe its cancel
            await WaitForCompletionAsync(result, 5_000);
            Assert.Equal(TaskStatus.Canceled, result.Status);
            OperationCanceledException ex = Assert.Throws<OperationCanceledException>(() => result.GetAwaiter().GetResult());
            Assert.Equal(cancels[i].Token, ex.CancellationToken); // each gets the correct blame
        }

        Assert.Equal(0, Volatile.Read(ref executeCount));
        semaphore.Release();

        // wait for underlying task to hand back to us
        if (!await semaphore.WaitAsync(5_000))
        {
            throw new TimeoutException("Didn't get handshake back from task");
        }

        Assert.Equal(1, Volatile.Read(ref executeCount));
        Assert.Equal(1, Volatile.Read(ref cancelCount));
    }

    [Theory]
    [InlineData(2, 0)]
    [InlineData(2, 1)]
    [InlineData(10, 0)]
    [InlineData(10, 1)]
    [InlineData(10, 7)]
    public async Task MultipleCallsShareExecution_MostCancel(int callerCount, int remaining)
    {
        Assert.True(callerCount >= 2); // "most" is not "one"

        using var scope = GetDefaultCache(out RedisHybridCache cache);
        using var semaphore = new SemaphoreSlim(0);

        int executeCount = 0;
        int cancelCount = 0;
        var results = new Task<Guid>[callerCount];
        var cancels = new CancellationTokenSource[callerCount];
        for (int i = 0; i < callerCount; i++)
        {
            cancels[i] = new CancellationTokenSource();
            results[i] = cache.GetOrCreateAsync(Me(), async ct =>
            {
                using CancellationTokenRegistration reg = ct.Register(() => Interlocked.Increment(ref cancelCount));
                if (!await semaphore.WaitAsync(5_000, CancellationToken.None))
                {
                    throw new TimeoutException("Failed to activate");
                }

                try
                {
                    Interlocked.Increment(ref executeCount);
                    ct.ThrowIfCancellationRequested();
                    return Guid.NewGuid();
                }
                finally
                {
                    semaphore.Release(); // handshake so we can check when available again
                }
            }, cancellationToken: cancels[i].Token).AsTask();
        }

        Assert.Equal(callerCount, cache.DebugGetCallerCount(Me()));

        for (int i = 0; i < callerCount; i++)
        {
            if (i != remaining)
            {
                cancels[i].Cancel();
            }
        }

        // allow the shared underlying task time to enter its semaphore wait
        await Task.Delay(500);

        for (int i = 0; i < callerCount; i++)
        {
            if (i != remaining)
            {
                Task<Guid> result = results[i];

                await WaitForCompletionAsync(result, 5_000);
                Assert.Equal(TaskStatus.Canceled, result.Status);
                OperationCanceledException ex = Assert.Throws<OperationCanceledException>(() => result.GetAwaiter().GetResult());
                Assert.Equal(cancels[i].Token, ex.CancellationToken); // each gets the correct blame
            }
        }

        Assert.Equal(0, Volatile.Read(ref executeCount));
        semaphore.Release();

        // wait for underlying task to hand back to us
        if (!await semaphore.WaitAsync(5_000))
        {
            throw new TimeoutException("Didn't get handshake back from task");
        }

        Assert.Equal(1, Volatile.Read(ref executeCount));
        Assert.Equal(0, Volatile.Read(ref cancelCount)); // ran to completion
        await results[remaining];
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ImmutableTypesShareFinalTask(bool withCancelation)
    {
        using CancellationTokenSource? cts = withCancelation ? new() : null;
        CancellationToken token = cts?.Token ?? CancellationToken.None;

        using var scope = GetDefaultCache(out RedisHybridCache cache);
        using var semaphore = new SemaphoreSlim(0);

        // note AsTask *in this scenario* fetches the underlying incomplete task
        Task<Guid> first = cache.GetOrCreateAsync(Me(), async ct =>
        {
            await semaphore.WaitAsync(CancellationToken.None);
            semaphore.Release();
            return Guid.NewGuid();
        }, cancellationToken: token).AsTask();

        Task<Guid> second = cache.GetOrCreateAsync(Me(), async ct =>
        {
            await semaphore.WaitAsync(CancellationToken.None);
            semaphore.Release();
            return Guid.NewGuid();
        }, cancellationToken: token).AsTask();

        if (withCancelation)
        {
            Assert.NotSame(first, second);
        }
        else
        {
            Assert.Same(first, second);
        }

        semaphore.Release();
        Assert.Equal(await first, await second);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ImmutableCustomTypesShareFinalTask(bool withCancelation)
    {
        using CancellationTokenSource? cts = withCancelation ? new() : null;
        CancellationToken token = cts?.Token ?? CancellationToken.None;

        using var scope = GetDefaultCache(out RedisHybridCache cache);
        using var semaphore = new SemaphoreSlim(0);

        Task<Immutable> first = cache.GetOrCreateAsync(Me(), async ct =>
        {
            await semaphore.WaitAsync(CancellationToken.None);
            semaphore.Release();
            return new Immutable(Guid.NewGuid());
        }, cancellationToken: token).AsTask();

        Task<Immutable> second = cache.GetOrCreateAsync(Me(), async ct =>
        {
            await semaphore.WaitAsync(CancellationToken.None);
            semaphore.Release();
            return new Immutable(Guid.NewGuid());
        }, cancellationToken: token).AsTask();

        if (withCancelation)
        {
            Assert.NotSame(first, second);
        }
        else
        {
            Assert.Same(first, second);
        }

        semaphore.Release();

        Immutable x = await first;
        Immutable y = await second;
        Assert.Equal(x.Value, y.Value);
        Assert.Same(x, y); // same instance regardless of whether the tasks were shared
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MutableTypesNeverShareFinalTask(bool withCancelation)
    {
        using CancellationTokenSource? cts = withCancelation ? new() : null;
        CancellationToken token = cts?.Token ?? CancellationToken.None;

        using var scope = GetDefaultCache(out RedisHybridCache cache);
        using var semaphore = new SemaphoreSlim(0);

        Task<Mutable> first = cache.GetOrCreateAsync(Me(), async ct =>
        {
            await semaphore.WaitAsync(CancellationToken.None);
            semaphore.Release();
            return new Mutable(Guid.NewGuid());
        }, cancellationToken: token).AsTask();

        Task<Mutable> second = cache.GetOrCreateAsync(Me(), async ct =>
        {
            await semaphore.WaitAsync(CancellationToken.None);
            semaphore.Release();
            return new Mutable(Guid.NewGuid());
        }, cancellationToken: token).AsTask();

        Assert.NotSame(first, second);
        semaphore.Release();

        Mutable x = await first;
        Mutable y = await second;
        Assert.Equal(x.Value, y.Value);
        Assert.NotSame(x, y);
    }

    [Fact]
    public void ValidatePartitioning()
    {
        // we just want to validate that key-level partitioning is
        // happening to some degree, i.e. it isn't fundamentally broken
        using var scope = GetDefaultCache(out RedisHybridCache cache);
        Dictionary<object, int> counts = [];
        for (int i = 0; i < 1024; i++)
        {
            var key = new RedisHybridCache.StampedeKey(Guid.NewGuid().ToString(), default);
            Assert.True(key.Equals(key), "typed equality self");
            Assert.True(key.Equals((object)key), "object equality self");
            object obj = cache.GetPartitionedSyncLock(in key);
            if (!counts.TryGetValue(obj, out int count))
            {
                count = 0;
            }

            counts[obj] = count + 1;
        }

        // We just want to prove that we got 8 non-empty partitions.
        // This is *technically* non-deterministic, but: we'd
        // need to be having a very bad day for the math gods
        // to conspire against us that badly.
        Assert.Equal(8, counts.Count);
        foreach (KeyValuePair<object, int> pair in counts)
        {
            // the *median* should be 128 here; let's
            // not be aggressive about it, though
            Assert.True(pair.Value > 16);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    public Task JoinedCallersWithoutUnderlyingDataBalanceReservations_Immutable(int callerCount)
        => JoinedCallersWithoutUnderlyingDataBalanceReservationsAsync<Immutable>(callerCount);

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    public Task JoinedCallersWithoutUnderlyingDataBalanceReservations_Mutable(int callerCount)
        => JoinedCallersWithoutUnderlyingDataBalanceReservationsAsync<Mutable>(callerCount);

    private static async Task JoinedCallersWithoutUnderlyingDataBalanceReservationsAsync<T>(int callerCount)
        where T : class
    {
        // regression (stock): a miss with DisableUnderlyingData used to complete every joined caller with one
        // shared item that held a single reservation, so each extra caller over-released it
        using var scope = GetDefaultCache(out RedisHybridCache cache);
        const HybridCacheEntryFlags Flags = HybridCacheEntryFlags.DisableDistributedCache | HybridCacheEntryFlags.DisableLocalCache
            | HybridCacheEntryFlags.DisableUnderlyingData;

        Assert.True(cache.GetOrCreateStampedeState<int, T>(Me(), Flags, out RedisHybridCache.StampedeState<int, T> stampede, canBeCanceled: true, tags: null, out _));
        for (int i = 1; i < callerCount; i++)
        {
            Assert.False(cache.GetOrCreateStampedeState<int, T>(Me(), Flags, out RedisHybridCache.StampedeState<int, T> joined, canBeCanceled: true, tags: null, out _));
            Assert.Same(stampede, joined);
        }

        Assert.Equal(callerCount, cache.DebugGetCallerCount(Me(), Flags));

        await stampede.ExecuteDirectAsync(0, static (_, _) => throw new InvalidOperationException("Underlying data should not be invoked"), options: null, skipInitialRead: true);
        Assert.Equal(0, cache.DebugGetCallerCount(Me(), Flags)); // no longer an active operation

        // every caller must receive an item that holds a reservation for it
#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks
        RedisHybridCache.CacheItem<T> item = await stampede.Task;
#pragma warning restore VSTHRD003
        Assert.Equal(callerCount, item.RefCount);

        for (int i = 0; i < callerCount; i++)
        {
            Assert.Null(await stampede.JoinAsync(NullLogger.Instance, CancellationToken.None));
        }

        Assert.Equal(0, item.RefCount);
    }

    private class Mutable(Guid value)
    {
        public Guid Value => value;
    }

    [ImmutableObject(true)]
    public sealed class Immutable(Guid value)
    {
        public Guid Value => value;
    }

    private static string Me([CallerMemberName] string caller = "") => caller;

    private static async Task WaitForCompletionAsync(
        Task task,
        int timeoutMs,
        [CallerArgumentExpression(nameof(task))] string? expression = null)
    {
        if (task.IsCompleted)
        {
            return;
        }

#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks
        Task completed = await Task.WhenAny(task, Task.Delay(timeoutMs));
#pragma warning restore VSTHRD003
        if (completed != task)
        {
            throw new TimeoutException($"Task '{expression}' did not complete within {timeoutMs}ms");
        }
    }
}
