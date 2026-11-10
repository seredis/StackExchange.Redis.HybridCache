// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/DefaultHybridCache.CacheItem.cs,
// ImmutableCacheItem.cs and MutableCacheItem.cs). Differences: there is no L1 cache holding
// long-lived CacheItem instances here (the client cache below IDatabase holds serialized
// payloads instead), so a CacheItem's lifetime is the stampede operation itself - from creation
// until the last joined caller has unwrapped its value and any backend write has completed; the
// L1 size-tracking and eviction-callback machinery is therefore not carried over.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Logging;
using StackExchange.Redis.HybridCache.Internal;

namespace StackExchange.Redis.HybridCache;

public sealed partial class RedisHybridCache
{
    internal abstract class CacheItem
    {
        private long _creationTimestamp;

        private int _refCount = 1; // the number of pending operations against this cache item

        protected CacheItem(long creationTimestamp, TagSet tags)
        {
            Tags = tags;
            _creationTimestamp = creationTimestamp;
        }

        public abstract bool DebugIsImmutable { get; }

        public long CreationTimestamp => _creationTimestamp;

        public TagSet Tags { get; }

        // Note: the ref count is the number of callers anticipating this value at any given time. Initially,
        // it is one for a simple "get the value" flow, but if another call joins with us, it'll be incremented.
        // If either cancels, it will get decremented, with the entire flow being cancelled if it ever becomes
        // zero. In the case of mutable data, the final release recycles the retained serialized buffer.
        internal int RefCount => Volatile.Read(ref _refCount);

        internal void UnsafeSetCreationTimestamp(long timestamp)
            => _creationTimestamp = timestamp;

        public abstract bool TryReserveBuffer(out BufferChunk buffer);

        /// <summary>
        /// Signal that the consumer is done with this item (ref-count decr).
        /// </summary>
        /// <returns>True if this is the final release.</returns>
        public bool Release()
        {
            int newCount = Interlocked.Decrement(ref _refCount);
            Debug.Assert(newCount >= 0, "over-release detected");
            if (newCount == 0)
            {
                // perform per-item clean-up, i.e. buffer recycling (if defensive copies needed)
                OnFinalRelease();
                return true;
            }

            return false;
        }

        public bool TryReserve()
        {
            // This is basically interlocked increment, but with a check against:
            // a) incrementing upwards from zero
            // b) overflowing *back* to zero
            int oldValue = Volatile.Read(ref _refCount);
            do
            {
                if (oldValue is 0 or -1)
                {
                    return false; // already burned, or about to roll around back to zero
                }

                int updated = Interlocked.CompareExchange(ref _refCount, oldValue + 1, oldValue);
                if (updated == oldValue)
                {
                    return true; // we exchanged
                }

                oldValue = updated; // we failed, but we have an updated state
            }
            while (true);
        }

        protected virtual void OnFinalRelease() // any required release semantics
        {
        }
    }

    internal abstract class CacheItem<T> : CacheItem
    {
        protected CacheItem(long creationTimestamp, TagSet tags)
            : base(creationTimestamp, tags)
        {
        }

        // Attempt to get a value that was *not* previously reserved.
        public abstract bool TryGetValue(ILogger log, out T value);

        // get a value that *was* reserved, countermanding our reservation in the process
        public T GetReservedValue(ILogger log)
        {
            if (!TryGetValue(log, out T? value))
            {
                Throw();
            }

            _ = Release();
            return value;

            static void Throw() => throw new ObjectDisposedException("The cache item has been recycled before the value was obtained");
        }

        internal static CacheItem<T> Create(long creationTimestamp, TagSet tags) => ImmutableTypeCache<T>.IsImmutable
            ? new ImmutableCacheItem<T>(creationTimestamp, tags) : new MutableCacheItem<T>(creationTimestamp, tags);
    }

    internal sealed class ImmutableCacheItem<T> : CacheItem<T> // used to hold types that do not require defensive copies
    {
        private T _value = default!; // deferred until SetValue

        public ImmutableCacheItem(long creationTimestamp, TagSet tags)
            : base(creationTimestamp, tags)
        {
        }

        public override bool DebugIsImmutable => true;

        public void SetValue(T value) => _value = value;

        public override bool TryGetValue(ILogger log, out T value)
        {
            value = _value;
            return true; // always available
        }

        public override bool TryReserveBuffer(out BufferChunk buffer)
        {
            buffer = default;
            return false; // we don't have one to reserve!
        }
    }

    internal sealed class MutableCacheItem<T> : CacheItem<T> // used to hold types that require defensive copies
    {
        private IHybridCacheSerializer<T>? _serializer;
        private BufferChunk _buffer;
        private T? _fallbackValue; // only used in the case of serialization failures
#if DEBUG
        private RedisHybridCache? _debugTrackingCache;
#endif

        public MutableCacheItem(long creationTimestamp, TagSet tags)
            : base(creationTimestamp, tags)
        {
        }

        public override bool DebugIsImmutable => false;

        public void SetValue(ref BufferChunk buffer, IHybridCacheSerializer<T> serializer)
        {
            _serializer = serializer;
            _buffer = buffer;
            buffer = default; // we're taking over the lifetime; the caller no longer has it!
        }

        public void SetFallbackValue(T fallbackValue) => _fallbackValue = fallbackValue;

        [System.Diagnostics.Conditional("DEBUG")]
        internal void DebugOnlyTrackBuffer(RedisHybridCache cache)
        {
#if DEBUG
            _debugTrackingCache = cache;
            if (_buffer.ReturnToPool)
            {
                cache.DebugOnlyIncrementOutstandingBuffers();
            }
#endif
        }

        public override bool TryGetValue(ILogger log, out T value)
        {
            // only if we haven't already burned
            if (TryReserve())
            {
                try
                {
                    IHybridCacheSerializer<T>? serializer = _serializer;
                    value = serializer is null ? _fallbackValue! : serializer.Deserialize(_buffer.AsSequence());
                    return true;
                }
                catch (Exception ex)
                {
                    log.DeserializationFailure(ex);
                    throw;
                }
                finally
                {
                    _ = Release();
                }
            }

            value = default!;
            return false;
        }

        public override bool TryReserveBuffer(out BufferChunk buffer)
        {
            // only if we haven't already burned
            if (TryReserve())
            {
                buffer = _buffer.DoNotReturnToPool(); // not up to them!
                return true;
            }

            buffer = default;
            return false;
        }

        protected override void OnFinalRelease()
        {
#if DEBUG
            if (_buffer.ReturnToPool)
            {
                _debugTrackingCache?.DebugOnlyDecrementOutstandingBuffers();
            }
#endif
            _buffer.RecycleIfAppropriate();
        }
    }

#if DEBUG
    private int _debugOutstandingBuffers;

    internal int DebugOnlyGetOutstandingBuffers(bool flush = false)
        => flush ? Interlocked.Exchange(ref _debugOutstandingBuffers, 0) : Volatile.Read(ref _debugOutstandingBuffers);

    internal void DebugOnlyIncrementOutstandingBuffers() => Interlocked.Increment(ref _debugOutstandingBuffers);

    internal void DebugOnlyDecrementOutstandingBuffers() => Interlocked.Decrement(ref _debugOutstandingBuffers);
#endif
}
