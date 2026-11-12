// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

#if NET10_0_OR_GREATER
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace StackExchange.Redis.HybridCache.Internal;

/// <summary>
/// The direct tag-timestamp memo (issue #7): <c>tag → invalidation ticks</c>, consulted before
/// the client-side-cache read, so the per-read tag check on a hot entry costs a span lookup
/// instead of a RESP key render + cache probe per tag. This is the stock implementation's
/// local tag memo (<c>_tagInvalidationTimes</c>) with its foreign-invalidation flaw fixed: an
/// entry only exists here while the client cache holds the tag key WITH a removal observer
/// armed (SE.Redis per-key removal notification, <c>CachePolicy.RemovalHandler</c>), so a
/// foreign <c>RemoveByTag</c> - which stock's memo never hears about - drops the entry within
/// push latency.
/// </summary>
/// <remarks>
/// <para>
/// <b>Coherence invariant: a memo entry implies an armed enrollment.</b> An entry is only
/// written from a read served through the enrolled context (the serve arms the observer; the
/// first removal at or after it fires and the entry is dropped), and our own tag writes DROP
/// their entry rather than stamping it - the next read refetches, re-arms and re-populates.
/// Stamping on write would leave a fresh-looking entry with no armed observer behind it.
/// </para>
/// <para>
/// <b>The epoch closes the read/invalidate race</b>: a populate snapshots the epoch before the
/// cache read and stores only while it is unchanged (checked again after the write, removing
/// on a late race) - every removal bumps it, so an invalidation landing mid-read can only cost
/// a refetch, never resurrect a stale value.
/// </para>
/// <para>
/// <b>The freshness window makes degraded modes degrade identically</b>: an entry older than
/// the connection's <c>CachePolicy.TimeToLive</c> is not served, which is exactly the bound
/// the client cache itself enforces - under <c>TrackingMode.None</c> (no invalidations at all)
/// the memo is then no staler than the cache it fronts. Removal notifications for lifetime
/// reasons (<c>Expired</c>/<c>Evicted</c>) fire at lazy physical reclamation, which is why the
/// window is enforced here rather than waited for.
/// </para>
/// <para>
/// Like stock's memo (and the client cache's own key table), the map is unbounded on tag
/// cardinality; entries are a string plus two longs. Keyed by the bare tag (prefixes already
/// stripped by the owner), with a UTF-8 alternate lookup so the hot path - tag names straight
/// from the envelope - never materializes a string; that lookup is what gates this type (and
/// the feature) to net10. The wildcard tag is a dedicated field: it is consulted on every
/// read of every entry, and a volatile reference beats a dictionary probe.
/// </para>
/// </remarks>
internal sealed class TagTicksMemo
{
    private readonly ConcurrentDictionary<string, TagStamp> _map;
    private readonly ConcurrentDictionary<string, TagStamp>.AlternateLookup<ReadOnlySpan<byte>> _byUtf8;
    private readonly long _freshnessMs;
    private volatile TagStamp? _wildcard;
    private long _epoch;

    public TagTicksMemo(TimeSpan timeToLive)
    {
        // the same bound the client cache enforces; TimeToLive is documented finite, but clamp
        // against overflow in the age arithmetic regardless
        double ms = timeToLive.TotalMilliseconds;
        _freshnessMs = ms >= long.MaxValue / 2 ? long.MaxValue / 2 : (long)ms;
        _map = new ConcurrentDictionary<string, TagStamp>(TagNameComparer.Instance);
        _byUtf8 = _map.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    /// <summary>Bumped by every removal; a populate stores only across an unchanged epoch.</summary>
    public long Epoch => Volatile.Read(ref _epoch);

    public bool TryGetFresh(ReadOnlySpan<byte> tagUtf8, out long ticks)
        => IsFresh(_byUtf8.TryGetValue(tagUtf8, out TagStamp? stamp) ? stamp : null, out ticks);

    public bool TryGetFresh(string tag, out long ticks)
        => IsFresh(_map.TryGetValue(tag, out TagStamp? stamp) ? stamp : null, out ticks);

    public bool TryGetFreshWildcard(out long ticks) => IsFresh(_wildcard, out ticks);

    public void Set(ReadOnlySpan<byte> tagUtf8, long ticks, long epoch)
    {
        if (Volatile.Read(ref _epoch) == epoch)
        {
            _byUtf8[tagUtf8] = new TagStamp(ticks);
            if (Volatile.Read(ref _epoch) != epoch)
            {
                // raced a removal between the check and the write: drop, which fails safe
                // (the next read refetches; a false miss, never a stale serve)
                _byUtf8.TryRemove(tagUtf8, out _);
            }
        }
    }

    public void Set(string tag, long ticks, long epoch)
    {
        if (Volatile.Read(ref _epoch) == epoch)
        {
            _map[tag] = new TagStamp(ticks);
            if (Volatile.Read(ref _epoch) != epoch)
            {
                _map.TryRemove(tag, out _);
            }
        }
    }

    public void SetWildcard(long ticks, long epoch)
    {
        if (Volatile.Read(ref _epoch) == epoch)
        {
            _wildcard = new TagStamp(ticks);
            if (Volatile.Read(ref _epoch) != epoch)
            {
                _wildcard = null;
            }
        }
    }

    /// <summary>The removal-notification path: the bare tag, sliced from the notified key.</summary>
    public void Remove(ReadOnlySpan<byte> tagUtf8)
    {
        Interlocked.Increment(ref _epoch); // first, so an in-flight populate cannot land after the removal
        if (tagUtf8.SequenceEqual("*"u8))
        {
            _wildcard = null;
        }
        else
        {
            _byUtf8.TryRemove(tagUtf8, out _);
        }
    }

    /// <summary>The own-write path (<c>RemoveByTag</c>): drop rather than stamp - the removal
    /// notification dispatches on the thread pool and can lag the next read, but the client
    /// cache's own entry is already invalidated (<c>LocalWrite</c> is synchronous with the
    /// write), so dropping here makes the next read refetch, re-arm and re-populate.</summary>
    public void Remove(string tag)
    {
        Interlocked.Increment(ref _epoch);
        if (tag == TagSet.WildcardTag)
        {
            _wildcard = null;
        }
        else
        {
            _map.TryRemove(tag, out _);
        }
    }

    /// <summary>Everything at once (an unattributable notification): cheap and always safe.</summary>
    public void Clear()
    {
        Interlocked.Increment(ref _epoch);
        _wildcard = null;
        _map.Clear();
    }

    private bool IsFresh(TagStamp? stamp, out long ticks)
    {
        if (stamp is not null && Environment.TickCount64 - stamp.StampedAt < _freshnessMs)
        {
            ticks = stamp.Ticks;
            return true;
        }

        ticks = 0;
        return false;
    }

    /// <summary>A timestamped reading; a class, so every hand-over is one atomic reference.</summary>
    private sealed class TagStamp(long ticks)
    {
        public long Ticks { get; } = ticks;

        public long StampedAt { get; } = Environment.TickCount64;
    }

    /// <summary>
    /// Ordinal equality with a UTF-8 alternate view: hashes are computed over the UTF-8 bytes on
    /// both sides (the string side transcodes to hash), so the hot envelope-bytes lookup never
    /// materializes a string. String-side hashing only runs on the cold paths (populate by
    /// string, the post-factory probe, own writes).
    /// </summary>
    private sealed class TagNameComparer : IEqualityComparer<string>, IAlternateEqualityComparer<ReadOnlySpan<byte>, string>
    {
        public static readonly TagNameComparer Instance = new();

        private TagNameComparer()
        {
        }

        public bool Equals(string? x, string? y) => string.Equals(x, y, StringComparison.Ordinal);

        public int GetHashCode(string obj)
        {
            const int StackLimit = 256;
            byte[]? rented = null;
            int maxBytes = HybridCachePayload.Encoding.GetMaxByteCount(obj.Length);
            Span<byte> scratch = maxBytes <= StackLimit
                ? stackalloc byte[StackLimit]
                : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
            try
            {
                int written = HybridCachePayload.EncodeUtf8(obj.AsSpan(), scratch);
                return Hash(scratch.Slice(0, written));
            }
            finally
            {
                if (rented is not null)
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
        }

        public bool Equals(ReadOnlySpan<byte> alternate, string other) => HybridCachePayload.Utf8Equals(alternate, other);

        public int GetHashCode(ReadOnlySpan<byte> alternate) => Hash(alternate);

        public string Create(ReadOnlySpan<byte> alternate) => HybridCachePayload.Encoding.GetString(alternate);

        private static int Hash(ReadOnlySpan<byte> utf8)
        {
            // FNV-1a: tags are short; the constant factor beats fancier mixing here
            uint hash = 2166136261;
            foreach (byte b in utf8)
            {
                hash = (hash ^ b) * 16777619;
            }

            return (int)hash;
        }
    }
}
#endif
