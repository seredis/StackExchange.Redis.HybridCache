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
        public Parsed(ArraySegment<byte> payload, TimeSpan remainingTime, long creationTime, string[] tags)
        {
            Payload = payload;
            RemainingTime = remainingTime;
            CreationTime = creationTime;
            Tags = tags;
        }

        public ArraySegment<byte> Payload { get; }

        public TimeSpan RemainingTime { get; }

        public long CreationTime { get; }

        public string[] Tags { get; }
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
    /// Parses an envelope, validating framing, key echo and entry expiry. Tag and wildcard
    /// expiration are NOT checked here; the caller checks <see cref="Parsed.Tags"/> and
    /// <see cref="Parsed.CreationTime"/> against the tag-timestamp keys.
    /// </summary>
    public static ParseResult TryParse(ArraySegment<byte> source, string key, long nowTicks, out Parsed parsed, out Exception? fault)
    {
        fault = null;
        parsed = default;

        ReadOnlySpan<byte> bytes = new(source.Array!, source.Offset, source.Count);
        if (bytes.Length < 19)
        {
            return ParseResult.FormatNotRecognized;
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
                    return ParseResult.FormatNotRecognized;
            }

            long creationTime = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(4)); // after 2-byte entropy
            bytes = bytes.Slice(12);

            if (!TryRead7BitEncodedInt64(ref bytes, out ulong u64)) // flags
            {
                return ParseResult.InvalidData;
            }

            uint flags = (uint)u64;

            if (!TryRead7BitEncodedInt64(ref bytes, out u64) || u64 > int.MaxValue) // payload length
            {
                return ParseResult.InvalidData;
            }

            int payloadLength = (int)u64;

            if (!TryRead7BitEncodedInt64(ref bytes, out ulong duration))
            {
                return ParseResult.InvalidData;
            }

            long remainingTicks = (creationTime + (long)duration) - nowTicks;
            if (remainingTicks <= 0)
            {
                return ParseResult.ExpiredByEntry;
            }

            if ((flags & FlagHasLocalSize) != 0)
            {
                if (!TryRead7BitEncodedInt64(ref bytes, out _)) // local cache size: parsed, unused here
                {
                    return ParseResult.InvalidData;
                }
            }

            if (!TryRead7BitEncodedInt64(ref bytes, out u64) || u64 > int.MaxValue) // tag count
            {
                return ParseResult.InvalidData;
            }

            int tagCount = (int)u64;

            // key echo: compare as bytes (bitwise), protecting against case-insensitive/colliding backends
            if (!TryReadLengthPrefixed(ref bytes, out ReadOnlySpan<byte> keyBytes))
            {
                return ParseResult.InvalidData;
            }

            byte[] expectedKey = Encoding.GetBytes(key);
            if (!keyBytes.SequenceEqual(expectedKey))
            {
                return ParseResult.InvalidKey;
            }

            string[] tags = tagCount == 0 ? [] : new string[tagCount];
            for (int i = 0; i < tagCount; i++)
            {
                if (!TryReadLengthPrefixed(ref bytes, out ReadOnlySpan<byte> tagBytes))
                {
                    return ParseResult.InvalidData;
                }

                byte[] tagArray = tagBytes.ToArray();
                tags[i] = Encoding.GetString(tagArray, 0, tagArray.Length);
            }

            if (bytes.Length != payloadLength + 2
                || BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(payloadLength)) != sentinel)
            {
                return ParseResult.InvalidData;
            }

            int start = source.Offset + source.Count - (payloadLength + 2);
            parsed = new Parsed(
                new ArraySegment<byte>(source.Array!, start, payloadLength),
                new TimeSpan(remainingTicks),
                creationTime,
                tags);
            return ParseResult.Success;
        }
        catch (Exception ex)
        {
            fault = ex;
            return ParseResult.ParseFault;
        }

        static bool TryReadLengthPrefixed(ref ReadOnlySpan<byte> buffer, out ReadOnlySpan<byte> value)
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

        static bool TryRead7BitEncodedInt64(ref ReadOnlySpan<byte> buffer, out ulong result)
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
}
