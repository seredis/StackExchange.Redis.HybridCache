using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis.HybridCache.Internal;

namespace StackExchange.Redis.HybridCache;

/// <summary>
/// A <see cref="Microsoft.Extensions.Caching.Hybrid.HybridCache"/> implementation built natively
/// on StackExchange.Redis server-assisted client-side caching (<c>CLIENT TRACKING</c>).
/// </summary>
/// <remarks>
/// <para>
/// Unlike the default <c>Microsoft.Extensions.Caching.Hybrid</c> implementation, there is no
/// separate in-process L1: the multiplexer's client-side cache is the single near cache,
/// holding the serialized payload and invalidated by server push. Entries are plain redis
/// strings with absolute expiry (<c>SET ... PX</c>) — sliding expiration is deliberately
/// unsupported, because a sliding read is a write in disguise and is broken under client-side
/// caching either way. <c>RemoveAsync</c> is a delete (every process in the fleet drops its
/// near copy), and <c>RemoveByTagAsync</c> writes a per-tag invalidation-timestamp key
/// (<c>__SER_HCT__&lt;tag&gt;</c>) that is itself client-side cached and push-invalidated, so
/// tag invalidation is coherent across the fleet rather than local-plus-eventual.
/// </para>
/// <para>
/// Concurrent <c>GetOrCreateAsync</c> calls for the same key and flags are combined (stampede
/// protection): one underlying fetch runs, late callers join it, and the operation is only
/// cancelled when the last joined caller has cancelled. Mutable result types get a defensive
/// copy per caller (deserialized from the shared serialized form); immutable types (strings,
/// blittable values, and sealed types marked <c>[ImmutableObject(true)]</c>) share the instance.
/// </para>
/// <para>
/// The payload envelope is byte-compatible with the default implementation's
/// (<c>HybridCachePayload</c>), given the same serializer configuration; the redis-level
/// representation is not. An entry holding a foreign value (for example a stock hash entry, or
/// anything unparseable) is treated as a logged miss, never a surfaced error.
/// See <c>docs/design.md</c> and <c>docs/parity-plan.md</c> for the architecture, the deliberate
/// divergences from the in-box implementation, and phasing.
/// </para>
/// </remarks>
public sealed partial class RedisHybridCache : Microsoft.Extensions.Caching.Hybrid.HybridCache
{
    internal const string TagKeyInfix = "__SER_HCT__";
    internal const string WildcardTag = TagSet.WildcardTag;
    internal const int DefaultExpirationMinutes = 5; // mirrors stock

    private static readonly TimeSpan _tagKeyLifetime = TimeSpan.FromDays(1000); // mirrors stock MaxCacheDays

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly RedisHybridCacheOptions _options;
    private readonly ILogger _logger;
    private readonly TimeProvider _clock;

    private readonly HybridCacheEntryFlags _hardFlags; // *always* present (none today: the backend is mandatory)
    private readonly HybridCacheEntryFlags _defaultFlags; // note this already includes hardFlags
    private readonly TimeSpan _defaultExpiration;
    private readonly TimeSpan _defaultLocalCacheExpiration;
    private readonly int _maximumKeyLength;

    internal RedisHybridCache(
        IConnectionMultiplexer multiplexer,
        IOptions<RedisHybridCacheOptions> options,
        ILogger<RedisHybridCache>? logger = null,
        IServiceProvider? services = null)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? NullLogger<RedisHybridCache>.Instance;
        _services = services;
        _clock = (TimeProvider?)services?.GetService(typeof(TimeProvider)) ?? TimeProvider.System;

        MaximumPayloadBytes = checked((int)_options.MaximumPayloadBytes);
        _maximumKeyLength = _options.MaximumKeyLength;

        HybridCacheEntryOptions? defaultEntryOptions = _options.DefaultEntryOptions;
        _hardFlags = HybridCacheEntryFlags.None;
        _defaultFlags = (defaultEntryOptions?.Flags ?? HybridCacheEntryFlags.None) | _hardFlags;
        _defaultExpiration = defaultEntryOptions?.Expiration ?? TimeSpan.FromMinutes(DefaultExpirationMinutes);
        _defaultLocalCacheExpiration = GetEffectiveLocalCacheExpiration(defaultEntryOptions) ?? _defaultExpiration;
    }

    internal RedisHybridCacheOptions Options => _options;

    /// <inheritdoc />
    public override ValueTask<T> GetOrCreateAsync<TState, T>(
        string key,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        HybridCacheEntryOptions? options = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (factory is null)
        {
            throw new ArgumentNullException(nameof(factory));
        }

        bool canBeCanceled = cancellationToken.CanBeCanceled;
        if (canBeCanceled)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }

        HybridCacheEntryFlags flags = GetEffectiveFlags(options);
        if (!ValidateKey(key))
        {
            // the cache mechanisms are bypassed entirely (matching stock behaviour)
            return (flags & HybridCacheEntryFlags.DisableUnderlyingData) == 0
                ? factory(state, cancellationToken) : default;
        }

        if ((flags & HybridCacheEntryFlags.DisableDistributedCacheRead) != 0)
        {
            // no backend read: go straight to the shared operation, synchronously - this mirrors
            // stock (and lets non-cancellable callers of immutable types share the final task)
            return JoinOrCreateStampede(key, state, factory, options, tags, flags, canBeCanceled, readPerformed: false, cancellationToken);
        }

        return GetOrCreateCoreAsync(key, key.AsSpan(), state, factory, options, tags, flags, canBeCanceled, cancellationToken);
    }

#if NET10_0_OR_GREATER
    /// <inheritdoc />
    /// <remarks>
    /// The allocation-free spelling: the abstract base routes its interpolated-key
    /// (<see cref="System.Runtime.CompilerServices.DefaultInterpolatedStringHandler"/>) overloads
    /// here via the handler's text span. The key travels to the backend as a borrowed span
    /// (<c>RespKey</c>), so a locally-served hit never materializes it as a <see cref="string"/>;
    /// the key is only settled when the call has to go further - a miss (the stampede and the
    /// stored envelope need the real key) or a read that leaves the synchronous path (a span
    /// cannot bridge an await).
    /// </remarks>
    public override ValueTask<T> GetOrCreateAsync<TState, T>(
        ReadOnlySpan<char> key,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        HybridCacheEntryOptions? options = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (factory is null)
        {
            throw new ArgumentNullException(nameof(factory));
        }

        bool canBeCanceled = cancellationToken.CanBeCanceled;
        if (canBeCanceled)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }

        HybridCacheEntryFlags flags = GetEffectiveFlags(options);
        if (!ValidateKey(key))
        {
            // the cache mechanisms are bypassed entirely (matching stock behaviour)
            return (flags & HybridCacheEntryFlags.DisableUnderlyingData) == 0
                ? factory(state, cancellationToken) : default;
        }

        if ((flags & HybridCacheEntryFlags.DisableDistributedCacheRead) != 0)
        {
            // no backend read: the stampede needs the key as its identity anyway
            return JoinOrCreateStampede(key.ToString(), state, factory, options, tags, flags, canBeCanceled, readPerformed: false, cancellationToken);
        }

        return GetOrCreateCoreAsync(null, key, state, factory, options, tags, flags, canBeCanceled, cancellationToken);
    }
#endif

    private ValueTask<T> JoinOrCreateStampede<TState, T>(
        string key,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        HybridCacheEntryOptions? options,
        IEnumerable<string>? tags,
        HybridCacheEntryFlags flags,
        bool canBeCanceled,
        bool readPerformed,
        CancellationToken cancellationToken)
    {
        bool isNew = GetOrCreateStampedeState<TState, T>(key, flags, out StampedeState<TState, T> stampede, canBeCanceled, tags, out bool newStateCanSkipRead);
        if (!isNew)
        {
            if (HybridCacheEventSource.Log.IsEnabled())
            {
                HybridCacheEventSource.Log.StampedeJoin();
            }

            return stampede.JoinAsync(_logger, cancellationToken);
        }

        // we own this stampede and must dispatch the factory; the creating caller's own fresh miss
        // means the stampede need not re-read the backend, unless creation was contested (see the
        // Stampede partial)
        bool skipInitialRead = readPerformed && newStateCanSkipRead;

        if (canBeCanceled)
        {
            // *we* might cancel, but someone else might be depending on the result; start the
            // work independently, then join the outcome
            stampede.QueueUserWorkItem(state, factory, options, skipInitialRead);
            return stampede.JoinAsync(_logger, cancellationToken);
        }

        // we're going to run to completion; no need to get complicated
        _ = stampede.ExecuteDirectAsync(state, factory, options, skipInitialRead); // this larger task includes the backend write etc

        return stampede.UnwrapReservedAsync(_logger);
    }

    // Fast path: a direct read, outside any stampede - with client-side caching enabled this is
    // the equivalent of the stock implementation's L1 hit. A locally-served read completes
    // synchronously through the v4 context API (no Task, no state machine), the envelope is
    // parsed in place over one pooled rent, and the tag checks are themselves locally-served
    // reads - so a hit allocates nothing beyond the deserialized value. (The in-stampede read
    // only runs for the rare contested-creation path; see the stampede partials.)
    //
    // keyString is null when the caller only has a span (an interpolated key): the string is
    // settled on demand - at a miss (the stampede and the stored envelope need the real key), or
    // when a read leaves the synchronous path (a span cannot bridge an await).
    private ValueTask<T> GetOrCreateCoreAsync<TState, T>(
        string? keyString,
        ReadOnlySpan<char> key,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        HybridCacheEntryOptions? options,
        IEnumerable<string>? tags,
        HybridCacheEntryFlags flags,
        bool canBeCanceled,
        CancellationToken cancellationToken)
    {
        // Event mapping (stock semantics, adapted): a read that completes synchronously was
        // answered by the local cache - that is this implementation's L1, so a value served
        // from it is LocalCacheHit, with no distributed events (stock's L1 hit never consults
        // L2). Anything else is LocalCacheMiss; the distributed Get/Hit/Miss/Failed events are
        // then emitted only when a real round trip happens. A synchronously-served nil (or a
        // rejected/invalidated envelope) is a local miss with no distributed fetch: the local
        // answer IS the fleet answer, and the factory events that follow tell the rest.
        bool eventSourceEnabled = HybridCacheEventSource.Log.IsEnabled();

        ValueTask<CachedEnvelope?> pendingRead;
        try
        {
            pendingRead = GetFromBackendAsync(key, flags, options);
        }
        catch (Exception ex)
        {
            if (eventSourceEnabled)
            {
                HybridCacheEventSource.Log.LocalCacheMiss();
                HybridCacheEventSource.Log.DistributedCacheGet();
                HybridCacheEventSource.Log.DistributedCacheFailed();
            }

            // includes WRONGTYPE from a foreign (e.g. stock hash) entry: a logged miss, never an error
            _logger.CacheUnderlyingDataQueryFailure(ex);
            return JoinOrCreateStampede(keyString ?? key.ToString(), state, factory, options, tags, flags, canBeCanceled, readPerformed: true, cancellationToken);
        }

        if (!pendingRead.IsCompleted)
        {
            // a real round trip
            if (eventSourceEnabled)
            {
                HybridCacheEventSource.Log.LocalCacheMiss();
                HybridCacheEventSource.Log.DistributedCacheGet();
            }

            return AwaitBackendReadAsync(keyString ?? key.ToString(), pendingRead, state, factory, options, tags, flags, canBeCanceled, eventSourceEnabled, cancellationToken);
        }

        CachedEnvelope? envelope;
        try
        {
            envelope = pendingRead.GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            if (eventSourceEnabled)
            {
                HybridCacheEventSource.Log.LocalCacheMiss();
                HybridCacheEventSource.Log.DistributedCacheGet();
                HybridCacheEventSource.Log.DistributedCacheFailed();
            }

            _logger.CacheUnderlyingDataQueryFailure(ex);
            return JoinOrCreateStampede(keyString ?? key.ToString(), state, factory, options, tags, flags, canBeCanceled, readPerformed: true, cancellationToken);
        }

        if (envelope is not null && TryGetPayload(envelope, key, out HybridCachePayload.Parsed parsed))
        {
            ValueTask<bool> tagsCurrent = AreTagsCurrentAsync(parsed.CreationTime, parsed);
            if (!tagsCurrent.IsCompletedSuccessfully)
            {
                // a cold tag timestamp (first consult since connect); rare by construction
                return AwaitTagsAsync(keyString ?? key.ToString(), tagsCurrent, envelope, parsed.Payload, state, factory, options, tags, flags, canBeCanceled, eventSourceEnabled, cancellationToken);
            }

            if (tagsCurrent.Result && TryDeserialize(envelope, parsed.Payload, out T value))
            {
                if (eventSourceEnabled)
                {
                    HybridCacheEventSource.Log.LocalCacheHit();
                }

                return new(value);
            }
        }

        if (eventSourceEnabled)
        {
            HybridCacheEventSource.Log.LocalCacheMiss();
        }

        // miss: join or create the shared operation for this (key, flags)
        return JoinOrCreateStampede(keyString ?? key.ToString(), state, factory, options, tags, flags, canBeCanceled, readPerformed: true, cancellationToken);
    }

    private async ValueTask<T> AwaitBackendReadAsync<TState, T>(
        string key,
        ValueTask<CachedEnvelope?> pendingRead,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        HybridCacheEntryOptions? options,
        IEnumerable<string>? tags,
        HybridCacheEntryFlags flags,
        bool canBeCanceled,
        bool eventSourceEnabled,
        CancellationToken cancellationToken)
    {
        CachedEnvelope? envelope = null;
        try
        {
            envelope = await pendingRead.ConfigureAwait(false);
            if (eventSourceEnabled)
            {
                if (envelope is not null)
                {
                    HybridCacheEventSource.Log.DistributedCacheHit();
                }
                else
                {
                    HybridCacheEventSource.Log.DistributedCacheMiss();
                }
            }
        }
        catch (Exception ex)
        {
            if (eventSourceEnabled)
            {
                HybridCacheEventSource.Log.DistributedCacheFailed();
            }

            // includes WRONGTYPE from a foreign (e.g. stock hash) entry: a logged miss, never an error
            _logger.CacheUnderlyingDataQueryFailure(ex);
        }

        if (envelope is not null && TryGetPayload(envelope, key.AsSpan(), out HybridCachePayload.Parsed parsed))
        {
            if (await AreTagsCurrentAsync(parsed.CreationTime, parsed).ConfigureAwait(false)
                && TryDeserialize(envelope, parsed.Payload, out T value))
            {
                return value;
            }
        }

        return await JoinOrCreateStampede(key, state, factory, options, tags, flags, canBeCanceled, readPerformed: true, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<T> AwaitTagsAsync<TState, T>(
        string key,
        ValueTask<bool> tagsCurrent,
        CachedEnvelope envelope,
        ArraySegment<byte> payload,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        HybridCacheEntryOptions? options,
        IEnumerable<string>? tags,
        HybridCacheEntryFlags flags,
        bool canBeCanceled,
        bool eventSourceEnabled,
        CancellationToken cancellationToken)
    {
        // the entry itself was answered by the local cache (only a tag timestamp went remote),
        // so the local hit/miss verdict follows whether that answer is finally served
        if (await tagsCurrent.ConfigureAwait(false) && TryDeserialize(envelope, payload, out T value))
        {
            if (eventSourceEnabled)
            {
                HybridCacheEventSource.Log.LocalCacheHit();
            }

            return value;
        }

        if (eventSourceEnabled)
        {
            HybridCacheEventSource.Log.LocalCacheMiss();
        }

        return await JoinOrCreateStampede(key, state, factory, options, tags, flags, canBeCanceled, readPerformed: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override ValueTask SetAsync<T>(
        string key,
        T value,
        HybridCacheEntryOptions? options = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (!ValidateKey(key))
        {
            return default; // nowhere to sensibly write (matching the GetOrCreateAsync bypass)
        }

        // since we're forcing a write: disable read; we use a direct pass-thru of the value as the
        // callback, to reuse all the machinery (validation, tag-race handling, serialization, write).
        // note also that this stampede is standalone: it is never registered for joining.
        HybridCacheEntryFlags flags = GetEffectiveFlags(options) | (HybridCacheEntryFlags.DisableLocalCacheRead | HybridCacheEntryFlags.DisableDistributedCacheRead);
        var state = new StampedeState<T, T>(this, new StampedeKey(key, flags), TagSet.Create(tags), cancellationToken);
        return new(state.ExecuteDirectAsync(value, static (value, _) => new(value), options, skipInitialRead: true)); // spans the backend write etc
    }

    /// <inheritdoc />
    public override async ValueTask RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        if (ValidateKey(key))
        {
            // the server pushes invalidation to every tracking client; a local write through this
            // multiplexer also invalidates the local client cache before it is sent (read-your-writes)
            _ = await DeleteBackendAsync(key).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public override async ValueTask RemoveAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default)
    {
        if (keys is null)
        {
            throw new ArgumentNullException(nameof(keys));
        }

        // individual deletes, issued concurrently: they pipeline on the multiplexer, and unlike a
        // single multi-key DEL they cannot fail on cluster slot boundaries
        List<Task>? pending = null;
        foreach (string key in keys)
        {
            if (ValidateKey(key))
            {
                (pending ??= []).Add(DeleteBackendAsync(key).AsTask());
            }
        }

        if (pending is not null)
        {
            await Task.WhenAll(pending).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public override async ValueTask RemoveByTagAsync(string tag, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(tag))
        {
            return; // nothing sensible to do (matches stock)
        }

        if (HybridCacheEventSource.Log.IsEnabled())
        {
            HybridCacheEventSource.Log.TagInvalidated();
        }

        _ = await InvalidateTagAsync(tag, CurrentTimestamp()).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async ValueTask RemoveByTagAsync(IEnumerable<string> tags, CancellationToken cancellationToken = default)
    {
        if (tags is null)
        {
            throw new ArgumentNullException(nameof(tags));
        }

        bool eventSourceEnabled = HybridCacheEventSource.Log.IsEnabled();
        long timestamp = CurrentTimestamp(); // one instant for the whole batch
        List<Task>? pending = null;
        foreach (string tag in tags)
        {
            if (!string.IsNullOrWhiteSpace(tag))
            {
                if (eventSourceEnabled)
                {
                    HybridCacheEventSource.Log.TagInvalidated();
                }

                (pending ??= []).Add(InvalidateTagAsync(tag, timestamp).AsTask());
            }
        }

        if (pending is not null)
        {
            await Task.WhenAll(pending).ConfigureAwait(false);
        }
    }

    internal HybridCacheEntryFlags GetEffectiveFlags(HybridCacheEntryOptions? options)
        => (options?.Flags | _hardFlags) ?? _defaultFlags;

    // reserve non-printable characters from keys, to prevent potential backend abuse.
    // A plain loop on every TFM: MemoryExtensions.IndexOfAnyInRange<char> measured 96 B of
    // allocation PER CALL here (net10 runtime; see the zero-alloc hit tests, which would
    // catch any regression), and keys are short.
    private static bool ContainsReservedCharacters(ReadOnlySpan<char> key)
    {
        const char MaxControlChar = (char)31;

        foreach (char c in key)
        {
            if (c <= MaxControlChar)
            {
                return true;
            }
        }

        return false;
    }

    private bool ValidateKey(string? key)
        => key is not null ? ValidateKey(key.AsSpan()) : FailEmptyKey();

    private bool ValidateKey(ReadOnlySpan<char> key)
    {
        if (key.IsEmpty || key.IsWhiteSpace())
        {
            return FailEmptyKey();
        }

        if (key.Length > _maximumKeyLength)
        {
            _logger.MaximumKeyLengthExceeded(_maximumKeyLength, key.Length);
            return false;
        }

        if (ContainsReservedCharacters(key))
        {
            _logger.KeyInvalidContent();
            return false;
        }

        // nothing to complain about
        return true;
    }

    private bool FailEmptyKey()
    {
        _logger.KeyEmptyOrWhitespace();
        return false;
    }
}
