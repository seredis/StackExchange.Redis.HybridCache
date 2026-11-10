// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// The redis-backend equivalents of the stock implementation's L2 + tag-invalidation partials
// (dotnet/extensions, Internal/DefaultHybridCache.L2.cs and DefaultHybridCache.TagInvalidation.cs),
// rebuilt over IDatabase: entries are plain strings with PX expiry; tag-invalidation timestamps
// are plain-string keys under the same tracked prefix, so with client-side caching enabled both
// are locally cached and push-coherent - the stock local tag memo (and its refresh machinery)
// has no equivalent here because the client cache *is* that memo, fleet-coherent.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using StackExchange.Redis.HybridCache.Internal;

namespace StackExchange.Redis.HybridCache;

public sealed partial class RedisHybridCache
{
    internal long CurrentTimestamp() => _clock.GetUtcNow().UtcTicks;

    private IDatabase Database => _multiplexer.GetDatabase();

    private RedisKey EntryKey(string key) => _options.KeyPrefix + key;

    private RedisKey TagKey(string tag) => _options.KeyPrefix + TagKeyInfix + tag;

    // CacheOptIn: being a HybridCache entry *is* the admission decision - force first-miss
    // admission (and reach) under every tracking mode, not just TrackingByPrefix. The local
    // flags map to CacheOptOut: the read/store suppression split is not expressible per-call
    // today (candidate SE.Redis API; see docs/design.md), so either local flag opts out of both.
    private static CommandFlags ReadCommandFlags(HybridCacheEntryFlags flags)
        => (flags & HybridCacheEntryFlags.DisableLocalCache) == 0 ? CommandFlags.CacheOptIn : CommandFlags.CacheOptOut;

    internal async Task<BufferChunk> GetFromBackendAsync(string key, HybridCacheEntryFlags flags)
    {
        byte[]? blob = (byte[]?)await Database.StringGetAsync(EntryKey(key), ReadCommandFlags(flags)).ConfigureAwait(false);
        return blob is null ? default : new BufferChunk(blob);
    }

    internal async Task SetBackendAsync(string key, CacheItem item, BufferChunk payload, HybridCacheEntryOptions? options)
    {
        TimeSpan expiration = GetBackendExpiration(options);
        int maxLen = HybridCachePayload.GetMaxBytes(key, item.Tags, payload.Length);
        byte[] oversized = ArrayPool<byte>.Shared.Rent(maxLen);
        try
        {
            int len = HybridCachePayload.Write(oversized, key, item.CreationTimestamp, expiration, item.Tags, payload.AsSequence());
            _ = await Database.StringSetAsync(EntryKey(key), new ReadOnlyMemory<byte>(oversized, 0, len), expiration).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(oversized);
        }
    }

    internal TimeSpan GetBackendExpiration(HybridCacheEntryOptions? options)
        => options?.Expiration ?? _defaultExpiration;

    private bool TryGetPayload(ArraySegment<byte> blob, string key, out HybridCachePayload.Parsed parsed)
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
    internal ValueTask<bool> IsCurrentAsync(CacheItem item)
    {
        TagSet tags = item.Tags;
        long creationTime = item.CreationTimestamp;
        return tags.Count switch
        {
            0 => AreTagsCurrentAsync(creationTime, [], wildcardPrefetch: null),
            1 => AreTagsCurrentAsync(creationTime, [tags.GetSinglePrechecked()], wildcardPrefetch: null),
            _ => AreTagsCurrentAsync(creationTime, tags.ToArray(), wildcardPrefetch: null),
        };
    }

    /// <summary>
    /// Checks whether an entry created at <paramref name="creationTime"/> survives the wildcard
    /// and all of <paramref name="tags"/>; tag reads are issued concurrently (they pipeline on
    /// the multiplexer, and with client-side caching they are typically local anyway).
    /// </summary>
    internal async ValueTask<bool> AreTagsCurrentAsync(long creationTime, string[] tags, Task<long>? wildcardPrefetch = null)
    {
        Task<long> wildcard = wildcardPrefetch ?? GetTagInvalidationTicksAsync(TagSet.WildcardTag);

        switch (tags.Length)
        {
            case 0:
                break;
            case 1:
                if (await GetTagInvalidationTicksAsync(tags[0]).ConfigureAwait(false) >= creationTime)
                {
                    return false;
                }

                break;
            default:
                Task<long>[] pending = new Task<long>[tags.Length];
                for (int i = 0; i < tags.Length; i++)
                {
                    pending[i] = GetTagInvalidationTicksAsync(tags[i]);
                }

                foreach (Task<long> task in pending)
                {
                    if (await task.ConfigureAwait(false) >= creationTime)
                    {
                        return false;
                    }
                }

                break;
        }

        return await wildcard.ConfigureAwait(false) < creationTime;
    }

    internal async Task<long> GetTagInvalidationTicksAsync(string tag)
    {
        try
        {
            // tag timestamps are hot by construction (consulted on every read of a tagged entry,
            // the wildcard on every read); CacheOptIn makes them locally cached + push-coherent
            // under every tracking mode
            byte[]? value = (byte[]?)await Database.StringGetAsync(TagKey(tag), CommandFlags.CacheOptIn).ConfigureAwait(false);
            return value is { Length: sizeof(long) } ? BinaryPrimitives.ReadInt64LittleEndian(value) : 0;
        }
        catch (Exception ex)
        {
            // fail closed, matching stock: an unreadable tag invalidates everything it covers,
            // which costs a refill but never serves a value past its invalidation
            _logger.TagInvalidationReadFailure(ex);
            return long.MaxValue;
        }
    }

    private Task InvalidateTagAsync(string tag, long timestamp)
    {
        byte[] ticks = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(ticks, timestamp);

        // entries created before this instant are now invalid; every client's cached copy of the
        // tag key is push-invalidated, so the whole fleet converges within the push latency
        return Database.StringSetAsync(TagKey(tag), ticks, _tagKeyLifetime);
    }
}
