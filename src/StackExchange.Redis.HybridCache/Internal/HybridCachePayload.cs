// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/HybridCachePayload.cs);
// the wire format is kept byte-compatible. Tag/wildcard expiration checks are performed by the
// caller here (they are asynchronous redis reads in this implementation), rather than inline.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace StackExchange.Redis.HybridCache.Internal;

/// <summary>
/// The stored-entry envelope, byte-compatible with the default HybridCache implementation's.
/// </summary>
/// <remarks>
/// FORMAT (v2):
/// fixed-size header: 2 bytes sentinel+version; 2 bytes entropy; 8 bytes creation time (UTC ticks, LE);
/// then varint flags, varint payload size, varint duration (ticks relative to creation),
/// (v2 only, when HasLocalSize) varint local cache size, varint tag count, varint+utf8 key,
/// per-tag varint+utf8, payload bytes, trailing 2-byte sentinel+version; all bytes must be exhausted.
/// v1 is identical minus the optional local-size varint; we write v1 (no local-size override) and read both.
/// </remarks>
internal static class HybridCachePayload
{
    private const int MaxVarint64Length = 10;
    private const byte SentinelPrefix = 0x03;
    private const ushort UInt16SentinelPrefixPairV1 = (0x01 << 8) | SentinelPrefix;
    private const ushort UInt16SentinelPrefixPairV2 = (0x02 << 8) | SentinelPrefix;

    private const uint FlagHasLocalSize = 1;

    private static readonly Random _entropySource = new();

    public static UTF8Encoding Encoding { get; } = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: false);

    internal enum ParseResult
    {
        Success = 0,
        FormatNotRecognized = 1,
        InvalidData = 2,
        InvalidKey = 3,
        ExpiredByEntry = 4,
        ParseFault = 5,
    }

    internal readonly struct Parsed
    {
        public Parsed(ArraySegment<byte> payload, long creationTime, ArraySegment<byte> tagsRegion, int tagCount)
        {
            Payload = payload;
            CreationTime = creationTime;
            TagsRegion = tagsRegion;
            TagCount = tagCount;
        }

        public ArraySegment<byte> Payload { get; }

        public long CreationTime { get; }

        /// <summary>
        /// The raw length-prefixed UTF-8 tag run, as validated slices of the source buffer: tag
        /// names are never materialized as strings on the read path (the tag check only needs the
        /// bytes, to build the tag-timestamp keys). Walk with <see cref="TagEnumerator"/>.
        /// </summary>
        public ArraySegment<byte> TagsRegion { get; }

        public int TagCount { get; }

        public TagEnumerator GetTagEnumerator() => new(TagsRegion);
    }

    /// <summary>Walks the length-prefixed UTF-8 tags of a <see cref="Parsed"/> envelope, in place.</summary>
    internal struct TagEnumerator
    {
        // indices rather than spans, so a caller can hold this across awaits
        private readonly ArraySegment<byte> _region;
        private int _offset;

        public TagEnumerator(ArraySegment<byte> region)
        {
            _region = region;
            _offset = 0;
        }

        /// <summary>Gets the next tag's UTF-8 bytes; the region was validated during parse, so this cannot fail mid-walk.</summary>
        public bool TryGetNext(out ArraySegment<byte> tag)
        {
            if (_offset >= _region.Count)
            {
                tag = default;
                return false;
            }

            ReadOnlySpan<byte> remaining = _region.AsSpan(_offset);
            int before = remaining.Length;
            if (!TryRead7BitEncodedInt64(ref remaining, out ulong u64))
            {
                tag = default; // unreachable post-validation; fail safe regardless
                return false;
            }

            int length = (int)u64;
            int start = _region.Offset + _offset + (before - remaining.Length);
            tag = new ArraySegment<byte>(_region.Array!, start, length);
            _offset = (start - _region.Offset) + length;
            return true;
        }
    }

    public static int GetMaxBytes(string key, in TagSet tags, int payloadSize)
    {
        int length =
            2 // sentinel+version
            + 2 // entropy
            + 8 // creation time
            + MaxVarint64Length // flags
            + MaxVarint64Length // payload size
            + MaxVarint64Length // duration
            + MaxVarint64Length // tag count
            + 2 // trailing sentinel+version
            + GetMaxStringLength(key.Length)
            + payloadSize;

        int tagCount = tags.Count;
        for (int i = 0; i < tagCount; i++)
        {
            length += GetMaxStringLength(tags[i].Length);
        }

        return length;

        static int GetMaxStringLength(int charCount) => MaxVarint64Length + Encoding.GetMaxByteCount(charCount);
    }

    /// <summary>Writes a v1 envelope; returns the bytes written.</summary>
    public static int Write(byte[] destination, string key, long creationTime, TimeSpan duration, in TagSet tags, in ReadOnlySequence<byte> payload)
    {
        int payloadLength = checked((int)payload.Length);

        BinaryPrimitives.WriteUInt16LittleEndian(destination.AsSpan(0, 2), UInt16SentinelPrefixPairV1);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.AsSpan(2, 2), (ushort)_entropySource.Next(0, 0x010000));
        BinaryPrimitives.WriteInt64LittleEndian(destination.AsSpan(4, 8), creationTime);
        int len = 12;

        long durationTicks = duration.Ticks;
        if (durationTicks < 0)
        {
            durationTicks = 0;
        }

        Write7BitEncodedInt64(destination, ref len, 0); // flags (none; we never persist a local-size override)
        Write7BitEncodedInt64(destination, ref len, (ulong)payloadLength);
        Write7BitEncodedInt64(destination, ref len, (ulong)durationTicks);
        int tagCount = tags.Count;
        Write7BitEncodedInt64(destination, ref len, (ulong)tagCount);
        WriteString(destination, ref len, key);
        for (int i = 0; i < tagCount; i++)
        {
            WriteString(destination, ref len, tags[i]);
        }

        payload.CopyTo(destination.AsSpan(len, payloadLength));
        len += payloadLength;
        BinaryPrimitives.WriteUInt16LittleEndian(destination.AsSpan(len, 2), UInt16SentinelPrefixPairV1);
        return len + 2;

        static void Write7BitEncodedInt64(byte[] target, ref int offset, ulong value)
        {
            while (value > 0x7Fu)
            {
                target[offset++] = (byte)((uint)value | ~0x7Fu);
                value >>= 7;
            }

            target[offset++] = (byte)value;
        }

        static void WriteString(byte[] target, ref int offset, string value)
        {
            int byteCount = Encoding.GetByteCount(value);
            Write7BitEncodedInt64(target, ref offset, (ulong)byteCount);
            offset += Encoding.GetBytes(value, 0, value.Length, target, offset);
        }
    }

    /// <summary>
    /// Parses an envelope's structure ONCE into a <see cref="CachedEnvelope"/> - the atom that
    /// SE.Redis result memoization shares across reads - validating framing and settling every
    /// position (key echo, tag run, payload) and the absolute expiry. The checks that are
    /// genuinely per-read are deliberately NOT performed here: entry expiry against the current
    /// clock and the key-echo compare live on the envelope
    /// (<see cref="CachedEnvelope.IsExpired"/>, <see cref="CachedEnvelope.KeyMatches"/>), and
    /// tag/wildcard currency stays with the caller, checked against the tag-timestamp keys via
    /// <see cref="Parsed.TagsRegion"/> and <see cref="Parsed.CreationTime"/>.
    /// </summary>
    /// <remarks>
    /// Always returns an envelope: damaged or foreign content parses to one carrying the
    /// rejection verdict, so the (memoized) structural parse never repeats while the per-read
    /// rejection still logs. <paramref name="source"/> ownership transfers to the envelope - it
    /// must be exact-sized and never pooled, because the envelope may be shared indefinitely.
    /// </remarks>
    public static CachedEnvelope ParseEnvelope(byte[] source)
    {
        ReadOnlySpan<byte> bytes = source;
        if (bytes.Length < 19)
        {
            return new CachedEnvelope(source, ParseResult.FormatNotRecognized, null);
        }

        try
        {
            ushort sentinel = BinaryPrimitives.ReadUInt16LittleEndian(bytes);
            switch (sentinel)
            {
                case UInt16SentinelPrefixPairV1:
                case UInt16SentinelPrefixPairV2:
                    break;
                default:
                    return new CachedEnvelope(source, ParseResult.FormatNotRecognized, null);
            }

            long creationTime = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(4)); // after 2-byte entropy
            bytes = bytes.Slice(12);

            if (!TryRead7BitEncodedInt64(ref bytes, out ulong u64)) // flags
            {
                return new CachedEnvelope(source, ParseResult.InvalidData, null);
            }

            uint flags = (uint)u64;

            if (!TryRead7BitEncodedInt64(ref bytes, out u64) || u64 > int.MaxValue) // payload length
            {
                return new CachedEnvelope(source, ParseResult.InvalidData, null);
            }

            int payloadLength = (int)u64;

            if (!TryRead7BitEncodedInt64(ref bytes, out ulong duration))
            {
                return new CachedEnvelope(source, ParseResult.InvalidData, null);
            }

            // absolute expiry, saturated: a negative creation time (foreign content) can only
            // produce an always-expired envelope, never wrap into the future
            long expiryTime;
            if (creationTime < 0)
            {
                expiryTime = long.MinValue;
            }
            else
            {
                ulong total = (ulong)creationTime + duration;
                expiryTime = total >= long.MaxValue ? long.MaxValue : (long)total;
            }

            if ((flags & FlagHasLocalSize) != 0)
            {
                if (!TryRead7BitEncodedInt64(ref bytes, out _)) // local cache size: parsed, unused here
                {
                    return new CachedEnvelope(source, ParseResult.InvalidData, null);
                }
            }

            if (!TryRead7BitEncodedInt64(ref bytes, out u64) || u64 > int.MaxValue) // tag count
            {
                return new CachedEnvelope(source, ParseResult.InvalidData, null);
            }

            int tagCount = (int)u64;

            // the key echo's position; the bitwise compare against the reading key is per-read
            // (CachedEnvelope.KeyMatches), because a memoized envelope serves every reader
            if (!TryReadLengthPrefixed(ref bytes, out ReadOnlySpan<byte> keyBytes))
            {
                return new CachedEnvelope(source, ParseResult.InvalidData, null);
            }

            int keyOffset = source.Length - bytes.Length - keyBytes.Length;

            // validate the tag run's framing now (so later walks cannot fail), but keep the tags as
            // raw bytes: the read path only ever needs them to build tag-timestamp keys
            int tagsStart = source.Length - bytes.Length;
            for (int i = 0; i < tagCount; i++)
            {
                if (!TryReadLengthPrefixed(ref bytes, out _))
                {
                    return new CachedEnvelope(source, ParseResult.InvalidData, null);
                }
            }

            int tagsLength = (source.Length - bytes.Length) - tagsStart;

            if (bytes.Length != payloadLength + 2
                || BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(payloadLength)) != sentinel)
            {
                return new CachedEnvelope(source, ParseResult.InvalidData, null);
            }

            return new CachedEnvelope(
                source,
                creationTime,
                expiryTime,
                keyOffset,
                keyBytes.Length,
                tagsStart,
                tagsLength,
                tagCount,
                payloadOffset: source.Length - (payloadLength + 2),
                payloadLength);
        }
        catch (Exception ex)
        {
            return new CachedEnvelope(source, ParseResult.ParseFault, ex);
        }
    }

    /// <summary>Compares UTF-8 bytes against UTF-16 text without allocating.</summary>
    internal static bool Utf8Equals(ReadOnlySpan<byte> utf8, ReadOnlySpan<char> text)
    {
        if (utf8.Length > text.Length * 3)
        {
            return false; // cheap length bound: a char never encodes to more than 3 bytes
        }

        const int StackLimit = 256;
        byte[]? rented = null;
        int maxBytes = Encoding.GetMaxByteCount(text.Length);
        Span<byte> scratch = maxBytes <= StackLimit
            ? stackalloc byte[StackLimit]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        try
        {
            int written = EncodeUtf8(text, scratch);
            return utf8.SequenceEqual(scratch.Slice(0, written));
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>Encodes UTF-16 text as UTF-8 into <paramref name="target"/>, returning the byte count.</summary>
    internal static int EncodeUtf8(ReadOnlySpan<char> text, Span<byte> target)
    {
#if NET8_0_OR_GREATER
        return Encoding.GetBytes(text, target);
#else
        if (text.IsEmpty)
        {
            return 0;
        }

        // downlevel: no span encoding API; bounce through pooled arrays
        char[] chars = ArrayPool<char>.Shared.Rent(text.Length);
        byte[] bytes = ArrayPool<byte>.Shared.Rent(target.Length);
        try
        {
            text.CopyTo(chars);
            int written = Encoding.GetBytes(chars, 0, text.Length, bytes, 0);
            bytes.AsSpan(0, written).CopyTo(target);
            return written;
        }
        finally
        {
            ArrayPool<char>.Shared.Return(chars);
            ArrayPool<byte>.Shared.Return(bytes);
        }
#endif
    }

    private static bool TryReadLengthPrefixed(ref ReadOnlySpan<byte> buffer, out ReadOnlySpan<byte> value)
    {
        int length;
        if (!TryRead7BitEncodedInt64(ref buffer, out ulong u64Length)
            || u64Length > int.MaxValue
            || buffer.Length < (length = (int)u64Length))
        {
            value = default;
            return false;
        }

        value = buffer.Slice(0, length);
        buffer = buffer.Slice(length);
        return true;
    }

    private static bool TryRead7BitEncodedInt64(ref ReadOnlySpan<byte> buffer, out ulong result)
    {
        const int MaxBytesWithoutOverflow = 9;
        result = 0;
        int index = 0;
        byte byteReadJustNow;
        for (int shift = 0; shift < MaxBytesWithoutOverflow * 7; shift += 7)
        {
            if (index >= buffer.Length)
            {
                return false; // truncated
            }

            byteReadJustNow = buffer[index++];
            result |= (byteReadJustNow & 0x7Ful) << shift;

            if (byteReadJustNow <= 0x7Fu)
            {
                buffer = buffer.Slice(index);
                return true;
            }
        }

        if (index >= buffer.Length)
        {
            return false;
        }

        byteReadJustNow = buffer[index++];
        if (byteReadJustNow > 0b_1u)
        {
            return false;
        }

        result |= (ulong)byteReadJustNow << (MaxBytesWithoutOverflow * 7);
        buffer = buffer.Slice(index);
        return true;
    }
}
