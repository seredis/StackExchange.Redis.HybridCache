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

        return GetOrCreateCoreAsync(key, state, factory, options, tags, flags, canBeCanceled, cancellationToken);
    }

    private async ValueTask<T> GetOrCreateCoreAsync<TState, T>(
        string key,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        HybridCacheEntryOptions? options,
        IEnumerable<string>? tags,
        HybridCacheEntryFlags flags,
        bool canBeCanceled,
        CancellationToken cancellationToken)
    {
        bool eventSourceEnabled = HybridCacheEventSource.Log.IsEnabled();

        // Fast path: a direct read, outside any stampede - with client-side caching enabled this
        // is the equivalent of the stock implementation's L1 hit, costing no stampede allocation.
        // (The in-stampede read only runs for the rare contested-creation path; see the stampede
        // partials.)
        bool readPerformed = false;
        if ((flags & HybridCacheEntryFlags.DisableDistributedCacheRead) == 0)
        {
            readPerformed = true;

            // overlap the wildcard-invalidation read (needed to validate any payload) with the entry read
            Task<long> wildcard = GetTagInvalidationTicksAsync(WildcardTag);

            BufferChunk blob = default;
            try
            {
                if (eventSourceEnabled)
                {
                    HybridCacheEventSource.Log.DistributedCacheGet();
                }

                blob = await GetFromBackendAsync(key, flags).ConfigureAwait(false);
                if (eventSourceEnabled)
                {
                    if (blob.HasValue)
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

            if (blob.HasValue
                && TryGetPayload(blob.AsArraySegment(), key, out HybridCachePayload.Parsed parsed)
                && await AreTagsCurrentAsync(parsed.CreationTime, parsed.Tags, wildcard).ConfigureAwait(false)
                && TryDeserialize(parsed.Payload, out T value))
            {
                return value;
            }
        }

        // miss: join or create the shared operation for this (key, flags)
        bool isNew = GetOrCreateStampedeState<TState, T>(key, flags, out StampedeState<TState, T> stampede, canBeCanceled, tags, out bool newStateCanSkipRead);
        if (!isNew)
        {
            if (eventSourceEnabled)
            {
                HybridCacheEventSource.Log.StampedeJoin();
            }

            return await stampede.JoinAsync(_logger, cancellationToken).ConfigureAwait(false);
        }

        // we own this stampede and must dispatch the factory; our own fresh miss means the
        // stampede need not re-read the backend, unless creation was contested (see Stampede partial)
        bool skipInitialRead = readPerformed && newStateCanSkipRead;

        if (canBeCanceled)
        {
            // *we* might cancel, but someone else might be depending on the result; start the
            // work independently, then join the outcome
            stampede.QueueUserWorkItem(state, factory, options, skipInitialRead);
            return await stampede.JoinAsync(_logger, cancellationToken).ConfigureAwait(false);
        }

        // we're going to run to completion; no need to get complicated
        _ = stampede.ExecuteDirectAsync(state, factory, options, skipInitialRead); // this larger task includes the backend write etc

        return await stampede.UnwrapReservedAsync(_logger).ConfigureAwait(false);
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
            await Database.KeyDeleteAsync(EntryKey(key)).ConfigureAwait(false);
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
                (pending ??= []).Add(Database.KeyDeleteAsync(EntryKey(key)));
            }
        }

        if (pending is not null)
        {
            await Task.WhenAll(pending).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public override ValueTask RemoveByTagAsync(string tag, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(tag))
        {
            return default; // nothing sensible to do (matches stock)
        }

        if (HybridCacheEventSource.Log.IsEnabled())
        {
            HybridCacheEventSource.Log.TagInvalidated();
        }

        return new(InvalidateTagAsync(tag, CurrentTimestamp()));
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

                (pending ??= []).Add(InvalidateTagAsync(tag, timestamp));
            }
        }

        if (pending is not null)
        {
            await Task.WhenAll(pending).ConfigureAwait(false);
        }
    }

    internal HybridCacheEntryFlags GetEffectiveFlags(HybridCacheEntryOptions? options)
        => (options?.Flags | _hardFlags) ?? _defaultFlags;

    // reserve non-printable characters from keys, to prevent potential backend abuse
    private static bool ContainsReservedCharacters(ReadOnlySpan<char> key)
    {
        const char MaxControlChar = (char)31;

#if NET8_0_OR_GREATER
        return key.IndexOfAnyInRange((char)0, MaxControlChar) >= 0;
#else
        foreach (char c in key)
        {
            if (c <= MaxControlChar)
            {
                return true;
            }
        }

        return false;
#endif
    }

    private bool ValidateKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            _logger.KeyEmptyOrWhitespace();
            return false;
        }

        if (key.Length > _maximumKeyLength)
        {
            _logger.MaximumKeyLengthExceeded(_maximumKeyLength, key.Length);
            return false;
        }

        if (ContainsReservedCharacters(key.AsSpan()))
        {
            _logger.KeyInvalidContent();
            return false;
        }

        // nothing to complain about
        return true;
    }
}
