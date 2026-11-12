// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/DefaultHybridCache.StampedeStateT.cs).
// Differences from stock: there is no L1 write (the client cache below IDatabase is the near
// cache, populated by reads), so SetResult just completes the shared task; the backend read
// inside the stampede is skipped when the creating caller's own fresh miss is good evidence
// (the fast-path GET in GetOrCreateAsync *was* the backend read - re-reading would double the
// round trips on every cold read); L1 size/TTL plumbing is not carried over.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Logging;
using StackExchange.Redis.HybridCache.Internal;

namespace StackExchange.Redis.HybridCache;

public sealed partial class RedisHybridCache
{
    internal sealed class StampedeState<TState, T> : StampedeState
    {
        private readonly TaskCompletionSource<CacheItem<T>>? _result;
        private TState? _state;
        private Func<TState, CancellationToken, ValueTask<T>>? _factory; // main data factory
        private HybridCacheEntryOptions? _options;
        private bool _skipInitialRead;
        private Task<T>? _sharedUnwrap; // allows multiple non-cancellable callers to share a single task (when no defensive copy needed)

        public StampedeState(RedisHybridCache cache, in StampedeKey key, TagSet tags, bool canBeCanceled)
            : base(cache, key, CacheItem<T>.Create(cache.CurrentTimestamp(), tags), canBeCanceled)
        {
            _result = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public StampedeState(RedisHybridCache cache, in StampedeKey key, TagSet tags, CancellationToken token)
            : base(cache, key, CacheItem<T>.Create(cache.CurrentTimestamp(), tags), token)
        {
            // no TCS in this case - this is for SetValue only
        }

        public override Type Type => typeof(T);

        public Task<CacheItem<T>> Task
        {
            get
            {
                Debug.Assert(_result is not null, "result should be assigned");
                return _result is null ? InvalidAsync() : _result.Task;

                static Task<CacheItem<T>> InvalidAsync() => System.Threading.Tasks.Task.FromException<CacheItem<T>>(
                    new InvalidOperationException("Task should not be accessed for non-shared instances"));
            }
        }

        private new CacheItem<T> CacheItem => (CacheItem<T>)base.CacheItem;

        public void QueueUserWorkItem(in TState state, Func<TState, CancellationToken, ValueTask<T>> factory, HybridCacheEntryOptions? options, bool skipInitialRead)
        {
            Debug.Assert(_factory is null, "should not already have factory field");
            Debug.Assert(factory is not null, "factory argument should be meaningful");

            // initialize the callback state
            _state = state;
            _factory = factory;
            _options = options;
            _skipInitialRead = skipInitialRead;

#if NET8_0_OR_GREATER
            ThreadPool.UnsafeQueueUserWorkItem(this, false);
#else
            ThreadPool.UnsafeQueueUserWorkItem(SharedWaitCallback, this);
#endif
        }

        public Task ExecuteDirectAsync(in TState state, Func<TState, CancellationToken, ValueTask<T>> factory, HybridCacheEntryOptions? options, bool skipInitialRead)
        {
            Debug.Assert(_factory is null, "should not already have factory field");
            Debug.Assert(factory is not null, "factory argument should be meaningful");

            // initialize the callback state
            _state = state;
            _factory = factory;
            _options = options;
            _skipInitialRead = skipInitialRead;

            return BackgroundFetchAsync();
        }

        public override void Execute() => _ = BackgroundFetchAsync();

        public override void SetCanceled() => _result?.TrySetCanceled(SharedToken);

        public ValueTask<T> JoinAsync(ILogger log, CancellationToken token)
        {
            // If the underlying has already completed, and/or our local token can't cancel: we
            // can simply wrap the shared task; otherwise, we need our own cancellation state.
            return token.CanBeCanceled && !Task.IsCompleted ? WithCancellationAsync(log, this, token) : UnwrapReservedAsync(log);

            static async ValueTask<T> WithCancellationAsync(ILogger log, StampedeState<TState, T> stampede, CancellationToken token)
            {
                var cancelStub = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                using CancellationTokenRegistration reg = token.Register(static obj =>
                {
                    _ = ((TaskCompletionSource<bool>)obj!).TrySetResult(true);
                }, cancelStub);

                CacheItem<T> result;
                try
                {
                    Task first = await System.Threading.Tasks.Task.WhenAny(stampede.Task, cancelStub.Task).ConfigureAwait(false);
                    if (ReferenceEquals(first, cancelStub.Task))
                    {
                        // we expect this to throw, because otherwise we wouldn't have gotten here
                        token.ThrowIfCancellationRequested(); // get an appropriate exception
                    }

                    Debug.Assert(ReferenceEquals(first, stampede.Task), "should not be cancelled");

                    // this has already completed, but we'll get the stack nicely
                    result = await stampede.Task.ConfigureAwait(false);
                }
                catch
                {
                    stampede.CancelCaller();
                    throw;
                }

                // outside the catch, so we know we only decrement one way or the other
                return result.GetReservedValue(log);
            }
        }

        internal ValueTask<T> UnwrapReservedAsync(ILogger log)
        {
            Task<CacheItem<T>> task = Task;
#if NET8_0_OR_GREATER
            if (task.IsCompletedSuccessfully)
#else
            if (task.Status == TaskStatus.RanToCompletion)
#endif
            {
                return new(task.Result.GetReservedValue(log));
            }

            // if the type is immutable, callers can share the final step too (this may leave dangling
            // reservation counters, but that's OK)
            Task<T> result = ImmutableTypeCache<T>.IsImmutable ? (_sharedUnwrap ??= AwaitedAsync(log, Task)) : AwaitedAsync(log, Task);
            return new(result);

            static async Task<T> AwaitedAsync(ILogger log, Task<CacheItem<T>> task)
                => (await task.ConfigureAwait(false)).GetReservedValue(log);
        }

        [DoesNotReturn]
        private static CacheItem<T> ThrowUnexpectedCacheItem() => throw new InvalidOperationException("Unexpected cache item");

        private async Task BackgroundFetchAsync()
        {
            bool eventSourceEnabled = HybridCacheEventSource.Log.IsEnabled();
            try
            {
                HybridCacheEntryFlags activeFlags = Key.Flags;

                if ((activeFlags & HybridCacheEntryFlags.DisableDistributedCache) != HybridCacheEntryFlags.DisableDistributedCache)
                {
                    // in order to use the backend, the tags and keys must be valid unicode, to avoid security complications
                    if (!ValidateUnicodeCorrectness(Cache._logger, Key.Key, CacheItem.Tags))
                    {
                        activeFlags |= HybridCacheEntryFlags.DisableDistributedCache;
                    }
                }

                // read from the backend if appropriate; the common path skips this (the creating
                // caller's fast-path GET was the backend read; see type remarks) - it runs only
                // for stampedes created through the contested path, where a fresh value may have
                // just been written by a completing operation
                if ((activeFlags & HybridCacheEntryFlags.DisableDistributedCacheRead) == 0 && !_skipInitialRead)
                {
                    CachedEnvelope? result;
                    try
                    {
                        if (eventSourceEnabled)
                        {
                            HybridCacheEventSource.Log.DistributedCacheGet();
                        }

                        result = await Cache.GetFromBackendAsync(Key.Key, activeFlags, _options).ConfigureAwait(false);
                        if (eventSourceEnabled)
                        {
                            if (result is not null)
                            {
                                HybridCacheEventSource.Log.DistributedCacheHit();
                            }
                            else
                            {
                                HybridCacheEventSource.Log.DistributedCacheMiss();
                            }
                        }
                    }
                    catch (OperationCanceledException) when (SharedToken.IsCancellationRequested)
                    {
                        if (eventSourceEnabled)
                        {
                            HybridCacheEventSource.Log.DistributedCacheCanceled();
                        }

                        throw; // don't just treat as miss - exit ASAP
                    }
                    catch (Exception ex)
                    {
                        if (eventSourceEnabled)
                        {
                            HybridCacheEventSource.Log.DistributedCacheFailed();
                        }

                        Cache._logger.CacheUnderlyingDataQueryFailure(ex);
                        result = null; // treat as "miss"
                    }

                    if (result is not null
                        && Cache.TryGetPayload(result, Key.Key.AsSpan(), out HybridCachePayload.Parsed parsed)
                        && await Cache.AreTagsCurrentAsync(parsed.CreationTime, parsed).ConfigureAwait(false))
                    {
                        // move into the payload segment (minus any framing/header/etc data); the
                        // segment aliases the envelope's own array, which may be shared via the
                        // client cache's result memo - never pooled, never recycled
                        SetResultFromBytes(new BufferChunk(parsed.Payload.Array!, parsed.Payload.Offset, parsed.Payload.Count, returnToPool: false));
                        return;
                    }
                }

                // nothing from the backend; invoke the underlying data store
                if ((activeFlags & HybridCacheEntryFlags.DisableUnderlyingData) == 0)
                {
                    // invoke the callback supplied by the caller
                    T newValue;
                    try
                    {
                        if (eventSourceEnabled)
                        {
                            HybridCacheEventSource.Log.UnderlyingDataQueryStart();
                        }

                        newValue = await _factory!(_state!, SharedToken).ConfigureAwait(false);

                        if (eventSourceEnabled)
                        {
                            HybridCacheEventSource.Log.UnderlyingDataQueryComplete();
                        }
                    }
                    catch (Exception ex)
                    {
                        if (eventSourceEnabled)
                        {
                            if (ex is OperationCanceledException && SharedToken.IsCancellationRequested)
                            {
                                HybridCacheEventSource.Log.UnderlyingDataQueryCanceled();
                            }
                            else
                            {
                                HybridCacheEventSource.Log.UnderlyingDataQueryFailed();
                            }
                        }

                        throw;
                    }

                    // check whether we're going to hit a timing problem with tag invalidation
                    if (!await Cache.IsCurrentAsync(CacheItem).ConfigureAwait(false))
                    {
                        // We need to avoid a problem where either "*" or one of the active tags
                        // matches "now" - it would then be ambiguous whether the data is
                        // invalidated; consider all of the following happen *in the same measured
                        // instant*:
                        // - write with value A
                        // - invalidate by tag (or wildcard)
                        // - write with value B
                        // Both A and B have the same timestamp as the invalidation; to avoid this
                        // problem, we detect this (very rare) scenario, and inject an artificial
                        // delay, such that B effectively gets written at a later time.
                        long time = Cache.CurrentTimestamp();
                        if (time <= CacheItem.CreationTimestamp)
                        {
                            // Clock hasn't changed; this is *very rare*, and honestly mostly applies to
                            // tests with dummy fetch calls; inject an artificial delay and re-fetch
                            // the time.
                            await System.Threading.Tasks.Task.Delay(1, CancellationToken.None).ConfigureAwait(false);
                            time = Cache.CurrentTimestamp();
                        }

                        // We can safely update the timestamp without fear of torn values etc; no competing code
                        // will access this until we set the result, and we (the current thread/path) are the
                        // only execution for this instance.
                        CacheItem.UnsafeSetCreationTimestamp(time);
                    }

                    // Serialization is required when the CacheItem is mutable (the serialized bytes are the
                    // stampede storage - each joined caller deserializes a defensive copy - and they double
                    // as the backend payload), or when we are writing to the backend at all.
                    CacheItem<T> cacheItem = CacheItem;
                    bool skipSerialize = cacheItem is ImmutableCacheItem<T>
                        && (activeFlags & HybridCacheEntryFlags.DisableDistributedCacheWrite) != 0;

                    if (skipSerialize)
                    {
                        SetImmutableResultWithoutSerialize(newValue);
                    }
                    else if (cacheItem.TryReserve())
                    {
                        // ^^^ The first thing we need to do is make sure we're not getting into a thread race over buffer disposal.
                        // In particular, if this cache item is somehow so short-lived that the buffers would be released *before* we're
                        // done writing them to the backend, which happens *after* we've provided the value to consumers.
                        BufferChunk bufferToRelease = default;
                        if (Cache.TrySerialize(newValue, out BufferChunk buffer, out IHybridCacheSerializer<T>? serializer))
                        {
                            // note we also capture the resolved serializer ^^^ - we'll need it again later

                            // protect "buffer" (this is why we "reserved") for the backend write if needed; SetResultPreSerialized
                            // *may* (depending on mutability) claim this buffer, in which case "bufferToRelease" gets reset, and
                            // the final RecycleIfAppropriate() is a no-op; however, the buffer is valid in either event
                            // (with TryReserve above guaranteeing that we aren't in a race condition).
                            bufferToRelease = buffer;

                            // and since "bufferToRelease" is the thing that will be returned at some point, we can make it explicit
                            // that we do not need or want "buffer" to do any recycling (they're the same memory)
                            buffer = buffer.DoNotReturnToPool();

                            // Write to the backend BEFORE releasing the waiting callers. This is a deliberate
                            // inversion of the stock order (which completes callers after its synchronous L1
                            // write, with the L2 write in the background): there is no L1 here, so completing
                            // first would let a caller's immediately-following read miss its own just-created
                            // value and re-run the factory. The backend write *is* our read-your-write step.
                            if ((activeFlags & HybridCacheEntryFlags.DisableDistributedCacheWrite) == 0)
                            {
                                // We already have the payload serialized, so this is trivial to do.
                                try
                                {
                                    await Cache.SetBackendAsync(Key.Key, cacheItem, buffer, _options).ConfigureAwait(false);

                                    if (eventSourceEnabled)
                                    {
                                        HybridCacheEventSource.Log.DistributedCacheWrite();
                                    }
                                }
                                catch (Exception ex)
                                {
                                    // log the backend write failure, but that doesn't need to interrupt the app flow (so:
                                    // don't rethrow); the caller still gets their value
                                    Cache._logger.CacheBackendWriteFailure(ex);
                                }
                            }

                            // set the underlying result for this operation, releasing the waiting callers
                            SetResultPreSerialized(newValue, ref bufferToRelease, serializer);
                        }
                        else
                        {
                            // unable to serialize (or quota exceeded); try to at least store the onwards value; this is
                            // especially useful for immutable data types
                            SetResultPreSerialized(newValue, ref bufferToRelease, serializer);
                        }

                        // Release our hook on the CacheItem (only really important for "mutable").
                        _ = cacheItem.Release();

                        // Finally, recycle whatever was left over from SetResultPreSerialized; using "bufferToRelease"
                        // here is NOT a typo; if SetResultPreSerialized left this value alone (immutable), then
                        // this is our recycle step; if SetResultPreSerialized transferred ownership to the (mutable)
                        // CacheItem, then this becomes a no-op, and the buffer only gets fully recycled when the
                        // CacheItem itself is fully clear.
                        bufferToRelease.RecycleIfAppropriate();
                    }
                    else
                    {
                        throw new InvalidOperationException("Internal HybridCache failure: unable to reserve cache item to assign result");
                    }
                }
                else
                {
                    // can't read from data store; implies we shouldn't write
                    // back to anywhere else, either
                    SetDefaultResult();
                }
            }
            catch (Exception ex)
            {
                SetException(ex);
            }
        }

        private void SetException(Exception ex)
        {
            if (_result is not null)
            {
                Cache.RemoveStampedeState(this);
                _ = _result.TrySetException(ex);
            }
        }

        private void SetDefaultResult()
        {
            // note we don't store this dummy result anywhere; we complete with our own cache item, because
            // it already holds one reservation per joined caller, and each caller releases one when unwrapping
            if (_result is not null)
            {
                CacheItem<T> cacheItem;
                switch (CacheItem)
                {
                    case ImmutableCacheItem<T> immutable:
                        immutable.SetValue(default!);
                        cacheItem = immutable;
                        break;
                    case MutableCacheItem<T> mutable:
                        mutable.SetFallbackValue(default!);
                        cacheItem = mutable;
                        break;
                    default:
                        cacheItem = ThrowUnexpectedCacheItem();
                        break;
                }

                Cache.RemoveStampedeState(this);
                _ = _result.TrySetResult(cacheItem);
            }
        }

        private void SetResultFromBytes(BufferChunk value)
        {
            // set a result from a backend read
            Debug.Assert(value.OversizedArray is not null, "expected buffer");

            IHybridCacheSerializer<T> serializer = Cache.GetSerializer<T>();
            CacheItem<T> cacheItem;
            switch (CacheItem)
            {
                case ImmutableCacheItem<T> immutable:
                    // deserialize and store the object; the buffer is not needed afterwards
                    immutable.SetValue(serializer.Deserialize(value.AsSequence()));
                    value.RecycleIfAppropriate();
                    cacheItem = immutable;
                    break;
                case MutableCacheItem<T> mutable:
                    // use the buffer directly as the backing in the cache-item; do *not* recycle now
                    mutable.SetValue(ref value, serializer);
                    mutable.DebugOnlyTrackBuffer(Cache);
                    cacheItem = mutable;
                    break;
                default:
                    cacheItem = ThrowUnexpectedCacheItem();
                    break;
            }

            SetResult(cacheItem);
        }

        private void SetImmutableResultWithoutSerialize(T value)
        {
            // set a result from a value we calculated directly
            CacheItem<T> cacheItem;
            switch (CacheItem)
            {
                case ImmutableCacheItem<T> immutable:
                    // no serialize needed
                    immutable.SetValue(value);
                    cacheItem = immutable;
                    break;
                default:
                    cacheItem = ThrowUnexpectedCacheItem();
                    break;
            }

            SetResult(cacheItem);
        }

        private void SetResultPreSerialized(T value, ref BufferChunk buffer, IHybridCacheSerializer<T>? serializer)
        {
            // set a result from a value we calculated directly that
            // has ALREADY BEEN SERIALIZED (we can optionally consume this buffer)
            CacheItem<T> cacheItem;
            switch (CacheItem)
            {
                case ImmutableCacheItem<T> immutable:
                    // no deserialize needed
                    immutable.SetValue(value);
                    cacheItem = immutable;

                    // (but leave the buffer alone)
                    break;
                case MutableCacheItem<T> mutable:
                    if (serializer is null)
                    {
                        // serialization is failing; set fallback value
                        mutable.SetFallbackValue(value);
                    }
                    else
                    {
                        mutable.SetValue(ref buffer, serializer);
                        mutable.DebugOnlyTrackBuffer(Cache);
                    }

                    cacheItem = mutable;
                    break;
                default:
                    cacheItem = ThrowUnexpectedCacheItem();
                    break;
            }

            SetResult(cacheItem);
        }

        private void SetResult(CacheItem<T> value)
        {
            // Unlike stock, there is no L1 write here: the client cache below IDatabase is
            // populated by reads (so the *next* read of this key fills it), and our own write
            // has read-your-writes semantics through the same multiplexer.
            if (_result is not null)
            {
                Cache.RemoveStampedeState(this);
                _ = _result.TrySetResult(value);
            }
        }
    }

    private static bool ValidateUnicodeCorrectness(ILogger logger, string key, TagSet tags)
    {
        // for reliable confidence of unicode correctness: encode and decode, and verify equality
        int maxChars = Math.Max(key.Length, tags.MaxLength());
        int maxBytes = HybridCachePayload.Encoding.GetMaxByteCount(maxChars);

        byte[] byteBuffer = ArrayPool<byte>.Shared.Rent(maxBytes);
        char[] charBuffer = ArrayPool<char>.Shared.Rent(maxChars);

        bool isValid = true;

        if (!Test(key, byteBuffer, charBuffer))
        {
            logger.KeyInvalidUnicode();
            isValid = false;
        }

        if (isValid)
        {
            switch (tags.Count)
            {
                case 0:
                    break;
                case 1:
                    if (!Test(tags.GetSinglePrechecked(), byteBuffer, charBuffer))
                    {
                        logger.TagInvalidUnicode();
                        isValid = false;
                    }

                    break;
                default:
                    foreach (string tag in tags.GetSpanPrechecked())
                    {
                        if (!Test(tag, byteBuffer, charBuffer))
                        {
                            logger.TagInvalidUnicode();
                            isValid = false;
                            break;
                        }
                    }

                    break;
            }
        }

        ArrayPool<char>.Shared.Return(charBuffer);
        ArrayPool<byte>.Shared.Return(byteBuffer);
        return isValid;

        static bool Test(string value, byte[] byteBuffer, char[] charBuffer)
        {
            int bytes = HybridCachePayload.Encoding.GetBytes(value, 0, value.Length, byteBuffer, 0);
            int chars = HybridCachePayload.Encoding.GetChars(byteBuffer, 0, bytes, charBuffer, 0);
            return charBuffer.AsSpan(0, chars).SequenceEqual(value.AsSpan());
        }
    }
}
