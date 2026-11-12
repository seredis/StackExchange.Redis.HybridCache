// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Threading;

namespace StackExchange.Redis.HybridCache.Internal;

/// <summary>
/// A stored entry's envelope, parsed ONCE into an immutable atom and then shared across reads:
/// the backend handler opts this type into SE.Redis result memoization
/// (<c>IRespResultReuse</c>), so with client-side caching a repeat hit on the same entry is
/// handed this object rather than raw bytes to re-copy and re-parse. Everything positional
/// (creation time, absolute expiry, key echo, tag run, payload region) is settled at parse;
/// the checks that are genuinely per-read - envelope expiry against the current clock, the
/// key-echo compare, tag currency - stay with the reader.
/// </summary>
/// <remarks>
/// A structurally damaged (or foreign) reply also parses to an envelope, carrying the
/// rejection verdict: memoizing the verdict means the structural parse is never repeated,
/// while the per-read rejection (and its log) still happens. The one mutable slot is the
/// deserialized-value memo: first-wins, and only ever set for values the HybridCache
/// immutability conventions (<see cref="ImmutableTypeCache{T}"/>) prove shareable - which is
/// what makes handing one instance to many callers caching rather than corruption.
/// </remarks>
internal sealed class CachedEnvelope
{
    private readonly byte[] _blob;
    private readonly int _keyOffset;
    private readonly int _keyLength;
    private readonly int _tagsOffset;
    private readonly int _tagsLength;
    private readonly int _tagCount;
    private readonly int _payloadOffset;
    private readonly int _payloadLength;

    // the deserialized-value memo; object, so one envelope can be read as different T
    // (first-wins: a mismatched later T just deserializes per read, as it always did)
    private object? _valueMemo;

    /// <summary>A structurally damaged envelope: the verdict (and any fault), memoized so the parse is not repeated.</summary>
    internal CachedEnvelope(byte[] blob, HybridCachePayload.ParseResult result, Exception? fault)
    {
        _blob = blob;
        Result = result;
        Fault = fault;
    }

    internal CachedEnvelope(
        byte[] blob,
        long creationTime,
        long expiryTime,
        int keyOffset,
        int keyLength,
        int tagsOffset,
        int tagsLength,
        int tagCount,
        int payloadOffset,
        int payloadLength)
    {
        _blob = blob;
        Result = HybridCachePayload.ParseResult.Success;
        CreationTime = creationTime;
        ExpiryTime = expiryTime;
        _keyOffset = keyOffset;
        _keyLength = keyLength;
        _tagsOffset = tagsOffset;
        _tagsLength = tagsLength;
        _tagCount = tagCount;
        _payloadOffset = payloadOffset;
        _payloadLength = payloadLength;
    }

    public HybridCachePayload.ParseResult Result { get; }

    public Exception? Fault { get; }

    public long CreationTime { get; }

    /// <summary>Absolute UTC ticks (creation + stored duration, saturated); the per-read expiry bound.</summary>
    public long ExpiryTime { get; }

    /// <summary>The byte-budget estimate for the client cache: the blob plus object overhead.
    /// A later-memoized deserialized value is not counted (an estimate is all this can be).</summary>
    public long Size => _blob.Length + 96;

    public bool IsExpired(long nowTicks) => nowTicks >= ExpiryTime;

    /// <summary>The per-read key-echo check (bitwise over UTF-8), protecting against
    /// case-misconfigured prefixes or any other way two logical keys could land on one backend
    /// key; per-read because a memoized envelope is served to every reader of the backend key.</summary>
    public bool KeyMatches(ReadOnlySpan<char> key)
        => HybridCachePayload.Utf8Equals(_blob.AsSpan(_keyOffset, _keyLength), key);

    /// <summary>The positional view the read path consumes; segments alias the shared blob, which is never recycled.</summary>
    public HybridCachePayload.Parsed AsParsed()
        => new(
            new ArraySegment<byte>(_blob, _payloadOffset, _payloadLength),
            CreationTime,
            new ArraySegment<byte>(_blob, _tagsOffset, _tagsLength),
            _tagCount);

    /// <summary>Serves the memoized deserialized value, when there is one of the right shape.</summary>
    public bool TryGetValue<T>(out T value)
    {
        // the immutability gate is repeated on the read side (it is a JIT constant) so a serve
        // can never widen what the store admitted - e.g. a base type the conventions reject
        if (ImmutableTypeCache<T>.IsImmutable && _valueMemo is T typed)
        {
            value = typed;
            return true;
        }

        value = default!;
        return false;
    }

    /// <summary>Memoizes a freshly deserialized value, when the immutability conventions allow sharing it; first-wins.</summary>
    public void MaybeMemoizeValue<T>(T value)
    {
        if (ImmutableTypeCache<T>.IsImmutable && value is not null)
        {
            Interlocked.CompareExchange(ref _valueMemo, value, null);
        }
    }
}
