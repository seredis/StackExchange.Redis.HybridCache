// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/DefaultHybridCache.StampedeState.cs).

using System;
using System.Diagnostics;
using System.Threading;

namespace StackExchange.Redis.HybridCache;

public sealed partial class RedisHybridCache
{
    internal abstract class StampedeState
#if NET8_0_OR_GREATER
        : IThreadPoolWorkItem
#endif
    {
        internal readonly CancellationToken SharedToken; // this might have a value even when _sharedCancellation is null

        // Because multiple callers can enlist, we need to track when the *last* caller cancels
        // (and keep going until then); that means we need to run with custom cancellation.
        private readonly CancellationTokenSource? _sharedCancellation;

        private readonly RedisHybridCache _cache;
        private readonly CacheItem _cacheItem;

        // we expose the key as a by-ref readonly; this minimizes the stack work involved in passing the key around
        private readonly StampedeKey _key;

        /// <summary>
        /// Initializes a new instance of the <see cref="StampedeState"/> class optionally with shared cancellation support.
        /// </summary>
        protected StampedeState(RedisHybridCache cache, in StampedeKey key, CacheItem cacheItem, bool canBeCanceled)
        {
            _cache = cache;
            _key = key;
            _cacheItem = cacheItem;
            if (canBeCanceled)
            {
                // If the first (or any) caller can't be cancelled, we'll never get to zero: no point tracking.
                // (in reality, all callers usually use the same path, so cancellation is usually "all" or "none")
                _sharedCancellation = new();
                SharedToken = _sharedCancellation.Token;
            }
            else
            {
                SharedToken = CancellationToken.None;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StampedeState"/> class using a fixed cancellation token.
        /// </summary>
        protected StampedeState(RedisHybridCache cache, in StampedeKey key, CacheItem cacheItem, CancellationToken token)
        {
            _cache = cache;
            _key = key;
            _cacheItem = cacheItem;
            SharedToken = token;
        }

        public ref readonly StampedeKey Key => ref _key;

        public abstract Type Type { get; }

        public int DebugCallerCount => _cacheItem.RefCount;

        protected CacheItem CacheItem => _cacheItem;

        protected RedisHybridCache Cache => _cache;

#if !NET8_0_OR_GREATER
        protected static readonly WaitCallback SharedWaitCallback = static obj => ((StampedeState)obj!).Execute();
#endif

        public abstract void Execute();

        public override string ToString() => Key.ToString();

        public abstract void SetCanceled();

        public void CancelCaller()
        {
            // note that TryAddCaller has protections to avoid getting back from zero
            if (_cacheItem.Release())
            {
                // we're the last to leave; turn off the lights
                _sharedCancellation?.Cancel();
                SetCanceled();
            }
        }

        public bool TryAddCaller() => _cacheItem.TryReserve();
    }

    private void RemoveStampedeState(StampedeState expected)
    {
        ref readonly StampedeKey key = ref expected.Key;

        // see notes in RedisHybridCache.SyncLock.cs
        lock (GetPartitionedSyncLock(in key))
        {
            if (_currentOperations.TryGetValue(key, out StampedeState? current)
                && ReferenceEquals(current, expected))
            {
                // Replacements take the same partitioned lock, and the unlocked TryAdd cannot replace an existing entry,
                // so this is safe.
                _ = _currentOperations.TryRemove(key, out StampedeState? removed);
                Debug.Assert(ReferenceEquals(removed, expected), "The stampede state changed while holding its partition lock.");
            }
        }
    }
}
