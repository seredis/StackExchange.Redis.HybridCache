// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// The redis-backend equivalents of the stock implementation's L2 + tag-invalidation partials
// (dotnet/extensions, Internal/DefaultHybridCache.L2.cs and DefaultHybridCache.TagInvalidation.cs),
// rebuilt over the StackExchange.Redis v4 context API (RespDatabaseContext + custom IRespHandler
// implementations), not IDatabase: a locally-served read completes synchronously with no Task, no
// state machine and no payload copy beyond one pooled rent, which is what makes the hit path
// allocation-free. Entries are plain strings with PX expiry; tag-invalidation timestamps are
// plain-string keys under the same tracked prefix, so with client-side caching enabled both are
// locally cached and push-coherent - the stock local tag memo (and its refresh machinery) has no
// equivalent here because the client cache *is* that memo, fleet-coherent.
//
// The key prefix lives on the context (AppendKeyPrefix), so keys are composed during the frame
// render rather than by string concatenation; keys travel as RespKey (a borrowed span), which is
// what lets the ReadOnlySpan<char> GetOrCreateAsync overloads reach the backend without ever
// materializing the interpolated key on a hit.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using RESPite.Messages;
using StackExchange.Redis.HybridCache.Internal;
using StackExchange.Redis.Protocol;

namespace StackExchange.Redis.HybridCache;

public sealed partial class RedisHybridCache
{
    private static readonly RespCommand _get = "GET".Command();
    private static readonly RespCommand _set = "SET".Command();
    private static readonly RespCommand _unlink = "UNLINK".Command();

    // "__SER_HCT__*": the wildcard tag-timestamp key, consulted on every read
    private static readonly byte[] _wildcardTagKeyUtf8 = CreateTagKeyUtf8(WildcardTag);

    // resolved on first backend use, NOT in the constructor: constructing the cache must make no
    // call on the multiplexer (DI can register a placeholder that is configured later; the
    // stampede-only paths never need a backend at all)
    private RespContext? _rawContext;
    private int _configurationChecked;

    // memoized read-time freshness bounds (one derived context per distinct LocalCacheExpiration:
    // deriving a policy and context allocates, and hits must not); created on first use, benign
    // race as above
    private ConcurrentDictionary<TimeSpan, RespContext>? _maxAgeContexts;

    private static ReadOnlySpan<byte> TagKeyInfixUtf8 => "__SER_HCT__"u8;

    internal long CurrentTimestamp() => _clock.GetUtcNow().UtcTicks;

    private RespDatabaseContext Context => new(_rawContext ??= CreateRawContext());

    // the policy per-read derivations narrow from; the connection's own default when there is a
    // client cache, so the non-TimeToLive knobs survive the derivation
    private Caching.CachePolicy BaseCachePolicy
        => _multiplexer.GetClientCache()?.Options.DefaultPolicy ?? Caching.CachePolicy.Default;

    private RespContext CreateRawContext()
    {
        // a benign race: concurrent first uses each build an identical context, one wins
        RespDatabaseContext context = _multiplexer.GetDatabaseContext();
        string prefix = _options.KeyPrefix;
        if (!string.IsNullOrEmpty(prefix))
        {
            context = context.AppendKeyPrefix(prefix);
        }

        CheckConfiguration();
        return (RespContext)context;
    }

    // Degraded configurations work, but must say what they cost (docs/design.md: "validated ...
    // loudly"); deferred to first backend use for the same reason the context is (see above).
    private void CheckConfiguration()
    {
        if (Interlocked.Exchange(ref _configurationChecked, 1) != 0)
        {
            return;
        }

        Caching.ClientCache? cache = _multiplexer.GetClientCache();
        if (cache is null || !cache.Options.Enabled)
        {
            // no local cache at all: correct, but it is a distributed-only cache - the user
            // probably wanted the near cache they came here for
            _logger.ClientCacheNotAvailable();
            return;
        }

        Caching.CacheTrackingMode mode = cache.EffectiveTrackingMode;
        TimeSpan timeToLive = cache.Options.DefaultPolicy.TimeToLive;
        switch (mode)
        {
            case Caching.CacheTrackingMode.None:
                // no invalidation of any kind: entries AND tag timestamps can be stale up to the TTL
                _logger.ClientCacheNoServerInvalidation(timeToLive);
                break;
            case Caching.CacheTrackingMode.KeyspaceNotifications:
                // includes the runtime fallback from Default (RESP2, CLIENT disabled, tracking refused)
                _logger.ClientCacheKeyspaceNotifications(timeToLive);
                break;
            default:
                _logger.ClientCacheValidated(mode);
                break;
        }

        // Under Broadcast, KeyspaceNotifications and None the configured prefixes are a hard
        // boundary: a key outside them is never announced, so it is never stored - our reads
        // quietly become distributed-only. (The per-key tracking modes need no check: our
        // per-call CacheOptIn reaches any key there, prefixes or not.)
        if (mode is Caching.CacheTrackingMode.Broadcast or Caching.CacheTrackingMode.KeyspaceNotifications or Caching.CacheTrackingMode.None
            && cache.Options.Prefixes is { Count: > 0 } prefixes
            && !CoversKeyPrefix(prefixes, _options.KeyPrefix))
        {
            _logger.ClientCacheKeyPrefixNotCovered(_options.KeyPrefix, mode);
        }
    }

    private static bool CoversKeyPrefix(System.Collections.Generic.IReadOnlyList<RedisKey> prefixes, string keyPrefix)
    {
        for (int i = 0; i < prefixes.Count; i++)
        {
            // every key this cache renders starts with KeyPrefix, so coverage of the prefix
            // itself is coverage of them all; anything partial is reported as not covered
            if ((string?)prefixes[i] is { } prefix && keyPrefix.StartsWith(prefix, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static byte[] CreateTagKeyUtf8(string tag)
    {
        byte[] result = new byte[TagKeyInfixUtf8.Length + HybridCachePayload.Encoding.GetByteCount(tag)];
        TagKeyInfixUtf8.CopyTo(result);
        HybridCachePayload.EncodeUtf8(tag.AsSpan(), result.AsSpan(TagKeyInfixUtf8.Length));
        return result;
    }

    // CacheOptIn: being a HybridCache entry *is* the admission decision - force first-miss
    // admission (and reach) under every tracking mode, not just TrackingByPrefix. The local
    // flags map to CacheOptOut: the read/store suppression split is not expressible per-call
    // today (candidate SE.Redis API; see docs/design.md), so either local flag opts out of both.
    private static CommandFlags ReadCommandFlags(HybridCacheEntryFlags flags)
        => (flags & HybridCacheEntryFlags.DisableLocalCache) == 0 ? CommandFlags.CacheOptIn : CommandFlags.CacheOptOut;

    internal ValueTask<BufferChunk> GetFromBackendAsync(string key, HybridCacheEntryFlags flags, HybridCacheEntryOptions? options)
        => ReadContext(options).SendAsync($"{_get}{new RespKey(key)}", ReadCommandFlags(flags), PooledBlobHandler.Instance);

    internal ValueTask<BufferChunk> GetFromBackendAsync(ReadOnlySpan<char> key, HybridCacheEntryFlags flags, HybridCacheEntryOptions? options)
        => ReadContext(options).SendAsync($"{_get}{new RespKey(key)}", ReadCommandFlags(flags), PooledBlobHandler.Instance);

    /// <summary>
    /// The context for an entry read: the plain context, or - when the effective
    /// <see cref="HybridCacheEntryOptions.LocalCacheExpiration"/> is strictly tighter than the
    /// backend expiration - a memoized context carrying a narrowed read-time policy
    /// (<c>WithCachePolicy</c>, <c>TimeToLive</c>), so a locally
    /// cached copy older than the local expiration is refetched rather than served. The
    /// envelope's own expiry check already bounds staleness at the (creation-based, so tighter)
    /// backend expiration, which is why an equal-or-wider local bound adds nothing and takes
    /// the plain context; that keeps the common no-options path at zero cost.
    /// </summary>
    private RespDatabaseContext ReadContext(HybridCacheEntryOptions? options)
    {
        TimeSpan local = GetEffectiveLocalCacheExpiration(options) ?? _defaultLocalCacheExpiration;
        return local < GetBackendExpiration(options) ? MaxAgeContext(local) : Context;
    }

    private RespDatabaseContext MaxAgeContext(TimeSpan maxAge)
    {
        ConcurrentDictionary<TimeSpan, RespContext> contexts = _maxAgeContexts ??= new();
        if (!contexts.TryGetValue(maxAge, out RespContext? context))
        {
            // derived from the connection's default policy, not CachePolicy.Default: a per-read
            // policy REPLACES the default for those reads, and the other knobs (removal handler,
            // refresh, grace) must survive the narrowing. TimeToLive narrows, never widens.
            Caching.CachePolicy policy = BaseCachePolicy with { TimeToLive = maxAge };

            // the value overload, not a factory lambda: GetOrAdd(key, factory) allocates the
            // delegate per call, and this runs on the hit path
            context = contexts.GetOrAdd(maxAge, (RespContext)Context.WithCachePolicy(policy));
        }

        return new(context);
    }

    // mirrors stock (DefaultHybridCache.GetEffectiveLocalCacheExpiration): within one options
    // object, LocalCacheExpiration inherits from Expiration, so that a caller who only sets
    // Expiration keeps the two in sync rather than silently keeping a stale default local bound
    private static TimeSpan? GetEffectiveLocalCacheExpiration(HybridCacheEntryOptions? options)
        => options is null ? null : options.LocalCacheExpiration ?? options.Expiration;

    internal async ValueTask SetBackendAsync(string key, CacheItem item, BufferChunk payload, HybridCacheEntryOptions? options)
    {
        TimeSpan expiration = GetBackendExpiration(options);
        int maxLen = HybridCachePayload.GetMaxBytes(key, item.Tags, payload.Length);
        byte[] oversized = ArrayPool<byte>.Shared.Rent(maxLen);
        ValueTask<bool> pending;
        try
        {
            int len = HybridCachePayload.Write(oversized, key, item.CreationTimestamp, expiration, item.Tags, payload.AsSequence());

            // the frame is rendered (and the value copied) synchronously inside the call, so the
            // rented buffer can go back before the await
            pending = Context.SendAsync<bool>(
                $"{_set}{new RespKey(key)}{oversized.AsSpan(0, len)}{(Expiration)expiration}");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(oversized);
        }

        _ = await pending.ConfigureAwait(false);
    }

    internal ValueTask<long> DeleteBackendAsync(string key)
        => Context.SendAsync<long>($"{_unlink}{new RespKey(key)}");

    private bool TryGetPayload(ArraySegment<byte> blob, ReadOnlySpan<char> key, out HybridCachePayload.Parsed parsed)
    {
        HybridCachePayload.ParseResult result = HybridCachePayload.TryParse(
            blob, key, CurrentTimestamp(), out parsed, out Exception? fault);
        switch (result)
        {
            case HybridCachePayload.ParseResult.Success:
                return true;
            case HybridCachePayload.ParseResult.ExpiredByEntry:
                return false; // normal: redis TTL has coarser resolution than the envelope duration
            default:
                // foreign or damaged content, or a key-echo mismatch (colliding/case-insensitive backend)
                _logger.CacheBackendDataRejected(result, fault);
                return false;
        }
    }

    /// <summary>Checks a post-factory entry against the wildcard + its tags (the "invalidation raced the factory" probe).</summary>
    internal async ValueTask<bool> IsCurrentAsync(CacheItem item)
    {
        TagSet tags = item.Tags;
        long creationTime = item.CreationTimestamp;
        int count = tags.Count;
        for (int i = 0; i < count; i++)
        {
            if (await GetTagInvalidationTicksAsync(tags[i]).ConfigureAwait(false) >= creationTime)
            {
                return false;
            }
        }

        return await GetWildcardInvalidationTicksAsync().ConfigureAwait(false) < creationTime;
    }

    /// <summary>
    /// Checks whether an entry created at <paramref name="creationTime"/> survives the wildcard
    /// and all of <paramref name="parsed"/>'s tags (raw UTF-8, from the envelope - tag
    /// names are never materialized). Tag timestamps are hot by construction (the wildcard is
    /// consulted on every read), so with client-side caching these reads are typically local and
    /// synchronous, and this method then completes synchronously without allocating.
    /// </summary>
    internal ValueTask<bool> AreTagsCurrentAsync(long creationTime, in HybridCachePayload.Parsed parsed)
    {
        // hand-rolled rather than an async method, so the common all-local case runs purely
        // synchronously: tag timestamps are hot by construction, and only a cold timestamp
        // (first consult since connect) drops to the awaiting slow path
        HybridCachePayload.TagEnumerator tags = parsed.GetTagEnumerator();
        while (tags.TryGetNext(out ArraySegment<byte> tag))
        {
            ValueTask<long> pending = GetTagInvalidationTicksAsync(tag);
            if (!pending.IsCompletedSuccessfully)
            {
                return AwaitTagWalkAsync(pending, tags, creationTime);
            }

            if (pending.Result >= creationTime)
            {
                return new(false);
            }
        }

        ValueTask<long> wildcard = GetWildcardInvalidationTicksAsync();
        return wildcard.IsCompletedSuccessfully
            ? new(wildcard.Result < creationTime)
            : AwaitWildcardAsync(wildcard, creationTime);
    }

    private async ValueTask<bool> AwaitTagWalkAsync(ValueTask<long> pending, HybridCachePayload.TagEnumerator tags, long creationTime)
    {
        if (await pending.ConfigureAwait(false) >= creationTime)
        {
            return false;
        }

        while (tags.TryGetNext(out ArraySegment<byte> tag))
        {
            if (await GetTagInvalidationTicksAsync(tag).ConfigureAwait(false) >= creationTime)
            {
                return false;
            }
        }

        return await GetWildcardInvalidationTicksAsync().ConfigureAwait(false) < creationTime;
    }

    private async ValueTask<bool> AwaitWildcardAsync(ValueTask<long> wildcard, long creationTime)
        => await wildcard.ConfigureAwait(false) < creationTime;

    internal ValueTask<long> GetWildcardInvalidationTicksAsync()
    {
        try
        {
            return FailClosedAsync(GetTagTicksCoreAsync(_wildcardTagKeyUtf8));
        }
        catch (Exception ex)
        {
            // fail closed even when the send itself throws synchronously (an unusable
            // multiplexer, say) - matching the per-tag reads
            return FailClosed(ex);
        }
    }

    /// <summary>Reads a tag's invalidation timestamp by its UTF-8 name (no string materialized).</summary>
    internal ValueTask<long> GetTagInvalidationTicksAsync(ArraySegment<byte> tagUtf8)
    {
        try
        {
            int length = TagKeyInfixUtf8.Length + tagUtf8.Count;
            byte[] rented = ArrayPool<byte>.Shared.Rent(length);
            TagKeyInfixUtf8.CopyTo(rented);
            tagUtf8.AsSpan().CopyTo(rented.AsSpan(TagKeyInfixUtf8.Length));

            // the frame render copies the key before SendAsync returns, so the rent can go
            // straight back - even when the read itself is still pending
            ValueTask<long> pending = GetTagTicksCoreAsync(rented.AsSpan(0, length));
            ArrayPool<byte>.Shared.Return(rented);
            return FailClosedAsync(pending);
        }
        catch (Exception ex)
        {
            return FailClosed(ex);
        }
    }

    /// <summary>Reads a tag's invalidation timestamp by name (the post-factory probe, where tags are strings).</summary>
    internal ValueTask<long> GetTagInvalidationTicksAsync(string tag)
    {
        try
        {
            int maxLength = TagKeyInfixUtf8.Length + HybridCachePayload.Encoding.GetMaxByteCount(tag.Length);
            byte[] rented = ArrayPool<byte>.Shared.Rent(maxLength);
            TagKeyInfixUtf8.CopyTo(rented);
            int length = TagKeyInfixUtf8.Length + HybridCachePayload.EncodeUtf8(tag.AsSpan(), rented.AsSpan(TagKeyInfixUtf8.Length));

            ValueTask<long> pending = GetTagTicksCoreAsync(rented.AsSpan(0, length));
            ArrayPool<byte>.Shared.Return(rented);
            return FailClosedAsync(pending);
        }
        catch (Exception ex)
        {
            return FailClosed(ex);
        }
    }

    // tag timestamps are consulted on every read of a tagged entry (the wildcard on every read);
    // CacheOptIn makes them locally cached + push-coherent under every tracking mode
    private ValueTask<long> GetTagTicksCoreAsync(ReadOnlySpan<byte> tagKeyUtf8)
        => Context.SendAsync($"{_get}{new RespKey(tagKeyUtf8)}", CommandFlags.CacheOptIn, TagTicksHandler.Instance);

    private ValueTask<long> FailClosedAsync(ValueTask<long> pending)
    {
        if (pending.IsCompletedSuccessfully)
        {
            return pending; // common: a locally-served read; no wrapper needed
        }

        return Awaited(this, pending);

        static async ValueTask<long> Awaited(RedisHybridCache @this, ValueTask<long> pending)
        {
            try
            {
                return await pending.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                return @this.FailClosedCore(ex);
            }
        }
    }

    private ValueTask<long> FailClosed(Exception ex) => new(FailClosedCore(ex));

    private long FailClosedCore(Exception ex)
    {
        // fail closed, matching stock: an unreadable tag invalidates everything it covers,
        // which costs a refill but never serves a value past its invalidation
        _logger.TagInvalidationReadFailure(ex);
        return long.MaxValue;
    }

    private ValueTask<bool> InvalidateTagAsync(string tag, long timestamp)
    {
        int maxLength = TagKeyInfixUtf8.Length + HybridCachePayload.Encoding.GetMaxByteCount(tag.Length);
        byte[] rented = ArrayPool<byte>.Shared.Rent(maxLength + sizeof(long));
        TagKeyInfixUtf8.CopyTo(rented);
        int length = TagKeyInfixUtf8.Length + HybridCachePayload.EncodeUtf8(tag.AsSpan(), rented.AsSpan(TagKeyInfixUtf8.Length));

        // entries created before this instant are now invalid; every client's cached copy of the
        // tag key is push-invalidated, so the whole fleet converges within the push latency
        BinaryPrimitives.WriteInt64LittleEndian(rented.AsSpan(maxLength, sizeof(long)), timestamp);
        ValueTask<bool> pending = Context.SendAsync<bool>(
            $"{_set}{new RespKey(rented.AsSpan(0, length))}{rented.AsSpan(maxLength, sizeof(long))}{(Expiration)_tagKeyLifetime}");
        ArrayPool<byte>.Shared.Return(rented);
        return pending;
    }

    /// <summary>Copies the raw reply into a pooled buffer; a nil reply is a miss (<c>default</c>).</summary>
    private sealed class PooledBlobHandler : IRespHandler<BufferChunk>
    {
        public static readonly PooledBlobHandler Instance = new();

        private PooledBlobHandler()
        {
        }

        public BufferChunk Parse(ref RespReader reader)
        {
            if (reader.IsNull)
            {
                return default;
            }

            int length = reader.ScalarLength();
            byte[] oversized = ArrayPool<byte>.Shared.Rent(Math.Max(length, 1));
            int copied = reader.CopyTo(oversized);
            Debug.Assert(copied == length, "scalar length disagreed with itself");
            return new BufferChunk(oversized, 0, length, returnToPool: true);
        }
    }

    /// <summary>Reads a tag-timestamp value in place: 8 little-endian ticks, anything else (including nil) is zero.</summary>
    private sealed class TagTicksHandler : IRespHandler<long>
    {
        public static readonly TagTicksHandler Instance = new();

        private TagTicksHandler()
        {
        }

        public long Parse(ref RespReader reader)
        {
            if (reader.IsNull || !reader.ScalarLengthIs(sizeof(long)))
            {
                return 0;
            }

            Span<byte> buffer = stackalloc byte[sizeof(long)];
            int copied = reader.CopyTo(buffer);
            Debug.Assert(copied == sizeof(long), "scalar length disagreed with itself");
            return BinaryPrimitives.ReadInt64LittleEndian(buffer);
        }
    }

    internal TimeSpan GetBackendExpiration(HybridCacheEntryOptions? options)
        => options?.Expiration ?? _defaultExpiration;
}
