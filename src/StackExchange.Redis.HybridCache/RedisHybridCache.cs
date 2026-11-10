using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis.HybridCache.Internal;

// the namespace shadows the base type's name; alias it once
using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

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
/// The payload envelope is byte-compatible with the default implementation's
/// (<c>HybridCachePayload</c>), given the same serializer configuration; the redis-level
/// representation is not. An entry holding a foreign value (for example a stock hash entry, or
/// anything unparseable) is treated as a logged miss, never a surfaced error.
/// See <c>docs/design.md</c> for the architecture and phasing.
/// </para>
/// <para>
/// Phase 1 limitations: no stampede protection yet (concurrent callers for the same key each
/// run the factory), and per-read tag-timestamp lookups are not locally memoized (the
/// client-side cache provides that when enabled).
/// </para>
/// </remarks>
public sealed partial class RedisHybridCache : HybridCacheBase
{
    internal const string TagKeyInfix = "__SER_HCT__";
    internal const string WildcardTag = "*";
    private static readonly TimeSpan _tagKeyLifetime = TimeSpan.FromDays(1000); // mirrors stock MaxCacheDays
    private static readonly TimeSpan _defaultExpiration = TimeSpan.FromMinutes(5); // stock default

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly RedisHybridCacheOptions _options;
    private readonly ILogger<RedisHybridCache> _logger;

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
    }

    private IDatabase Database => _multiplexer.GetDatabase();

    private static long NowTicks => DateTimeOffset.UtcNow.UtcTicks;

    private RedisKey EntryKey(string key) => _options.KeyPrefix + key;

    private RedisKey TagKey(string tag) => _options.KeyPrefix + TagKeyInfix + tag;

    /// <inheritdoc />
    public override async ValueTask<T> GetOrCreateAsync<TState, T>(
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

        HybridCacheEntryFlags flags = EffectiveFlags(options);
        bool keyUsable = ValidateKey(key);
        IDatabase db = Database;

        if (keyUsable && (flags & HybridCacheEntryFlags.DisableDistributedCacheRead) == 0)
        {
            byte[]? blob = null;
            try
            {
                blob = (byte[]?)await db.StringGetAsync(EntryKey(key)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // includes WRONGTYPE from a foreign (e.g. stock hash) entry: a logged miss, never an error
                _logger.LogError(ex, "Cache backend read failure.");
            }

            if (blob is not null && TryGetPayload(blob, key, out HybridCachePayload.Parsed parsed)
                && await AreTagsCurrentAsync(db, parsed.CreationTime, parsed.Tags).ConfigureAwait(false))
            {
                if (TryDeserialize(parsed.Payload, out T value))
                {
                    return value;
                }
            }
        }

        if ((flags & HybridCacheEntryFlags.DisableUnderlyingData) != 0)
        {
            return default!;
        }

        T created = await factory(state, cancellationToken).ConfigureAwait(false);

        if (keyUsable && (flags & HybridCacheEntryFlags.DisableDistributedCacheWrite) == 0)
        {
            await WriteEntryAsync(db, key, created, options, tags).ConfigureAwait(false);
        }

        return created;
    }

    /// <inheritdoc />
    public override async ValueTask SetAsync<T>(
        string key,
        T value,
        HybridCacheEntryOptions? options = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        HybridCacheEntryFlags flags = EffectiveFlags(options);
        if (ValidateKey(key) && (flags & HybridCacheEntryFlags.DisableDistributedCacheWrite) == 0)
        {
            await WriteEntryAsync(Database, key, value, options, tags).ConfigureAwait(false);
        }
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
    public override async ValueTask RemoveByTagAsync(string tag, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(tag))
        {
            return; // nothing sensible to do (matches stock)
        }

        byte[] ticks = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(ticks, NowTicks);

        // entries created before this instant are now invalid; every client's cached copy of the
        // tag key is push-invalidated, so the whole fleet converges within the push latency
        await Database.StringSetAsync(TagKey(tag), ticks, _tagKeyLifetime).ConfigureAwait(false);
    }

    private HybridCacheEntryFlags EffectiveFlags(HybridCacheEntryOptions? options)
        => options?.Flags ?? _options.DefaultEntryOptions?.Flags ?? HybridCacheEntryFlags.None;

    private TimeSpan EffectiveExpiration(HybridCacheEntryOptions? options)
        => options?.Expiration ?? _options.DefaultEntryOptions?.Expiration ?? _defaultExpiration;

    private bool ValidateKey(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentException("A cache key must be provided.", nameof(key));
        }

        if (key.Length > _options.MaximumKeyLength)
        {
            // matches the documented contract: over-length keys bypass the cache mechanisms
            _logger.LogWarning("Cache key exceeds MaximumKeyLength ({Length} > {Max}); bypassing cache.", key.Length, _options.MaximumKeyLength);
            return false;
        }

        return true;
    }

    private bool TryGetPayload(byte[] blob, string key, out HybridCachePayload.Parsed parsed)
    {
        HybridCachePayload.ParseResult result = HybridCachePayload.TryParse(
            new ArraySegment<byte>(blob), key, NowTicks, out parsed, out Exception? fault);
        switch (result)
        {
            case HybridCachePayload.ParseResult.Success:
                return true;
            case HybridCachePayload.ParseResult.ExpiredByEntry:
                return false; // normal: redis TTL has coarser resolution than the envelope duration
            case HybridCachePayload.ParseResult.ParseFault:
                _logger.LogError(fault, "Cache payload parse fault for key; treating as a miss.");
                return false;
            default:
                // foreign or damaged content, or a key-echo mismatch (colliding/case-insensitive backend)
                _logger.LogWarning("Cache payload not usable ({Result}); treating as a miss.", result);
                return false;
        }
    }

    private async ValueTask<bool> AreTagsCurrentAsync(IDatabase db, long creationTime, string[] tags)
    {
        if (await GetTagInvalidationTicksAsync(db, WildcardTag).ConfigureAwait(false) >= creationTime)
        {
            return false;
        }

        foreach (string tag in tags)
        {
            if (await GetTagInvalidationTicksAsync(db, tag).ConfigureAwait(false) >= creationTime)
            {
                return false;
            }
        }

        return true;
    }

    private async ValueTask<long> GetTagInvalidationTicksAsync(IDatabase db, string tag)
    {
        try
        {
            byte[]? value = (byte[]?)await db.StringGetAsync(TagKey(tag)).ConfigureAwait(false);
            return value is { Length: sizeof(long) } ? BinaryPrimitives.ReadInt64LittleEndian(value) : 0;
        }
        catch (Exception ex)
        {
            // fail closed, matching stock: an unreadable tag invalidates everything it covers,
            // which costs a refill but never serves a value past its invalidation
            _logger.LogError(ex, "Tag invalidation read failure; treating tag as expired.");
            return long.MaxValue;
        }
    }

    private async ValueTask WriteEntryAsync<T>(IDatabase db, string key, T value, HybridCacheEntryOptions? options, IEnumerable<string>? tags)
    {
        try
        {
            string[] tagArray = tags switch
            {
                null => [],
                string[] arr => arr,
                _ => tags.ToArray(),
            };
            foreach (string tag in tagArray)
            {
                if (string.IsNullOrWhiteSpace(tag) || tag == WildcardTag)
                {
                    throw new ArgumentException($"Tags cannot be empty or '{WildcardTag}'.", nameof(tags));
                }
            }

            using var writer = new PooledBufferWriter();
            GetSerializer<T>().Serialize(value, writer);

            if (writer.Length > _options.MaximumPayloadBytes)
            {
                _logger.LogWarning("Serialized payload for key exceeds MaximumPayloadBytes ({Length} > {Max}); not stored.", writer.Length, _options.MaximumPayloadBytes);
                return;
            }

            TimeSpan expiration = EffectiveExpiration(options);
            int maxLen = HybridCachePayload.GetMaxBytes(key, tagArray, checked((int)writer.Length));
            byte[] oversized = System.Buffers.ArrayPool<byte>.Shared.Rent(maxLen);
            try
            {
                int len = HybridCachePayload.Write(oversized, key, NowTicks, expiration, tagArray, writer.WrittenSequence);
                await db.StringSetAsync(EntryKey(key), new ReadOnlyMemory<byte>(oversized, 0, len), expiration).ConfigureAwait(false);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(oversized);
            }
        }
        catch (ArgumentException)
        {
            throw; // caller error (bad tags): surface it
        }
        catch (Exception ex)
        {
            // write failures are logged and swallowed (matching stock): the caller still has the value
            _logger.LogError(ex, "Cache backend write failure.");
        }
    }
}
