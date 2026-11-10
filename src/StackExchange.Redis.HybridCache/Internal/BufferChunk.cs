// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/BufferChunk.cs);
// the struct is non-readonly here so the recycle/flag mutations need no Unsafe.AsRef (which
// would need an extra package reference on netstandard2.0/net472).

using System;
using System.Buffers;
using System.Diagnostics;

namespace StackExchange.Redis.HybridCache.Internal;

// Used to convey buffer status; like ArraySegment<byte>, but we use the most significant bit
// of the length (usually the sign flag, but we do not need to support negative length) to
// track whether or not to recycle this value.
internal struct BufferChunk
{
    private const int FlagReturnToPool = 1 << 31;
    private int _lengthAndPoolFlag;

    public byte[]? OversizedArray { get; private set; } // null for default

    public readonly bool HasValue => OversizedArray is not null;

    public int Offset { get; private set; }

    public readonly int Length => _lengthAndPoolFlag & ~FlagReturnToPool;

    public readonly bool ReturnToPool => (_lengthAndPoolFlag & FlagReturnToPool) != 0;

    public BufferChunk(byte[] array)
    {
        Debug.Assert(array is not null, "expected valid array input");
        OversizedArray = array;
        _lengthAndPoolFlag = array!.Length;
        Offset = 0;

        // assume not pooled, if exact-sized
        Debug.Assert(!ReturnToPool, "do not return right-sized arrays");
        Debug.Assert(Length == array.Length, "array length not respected");
    }

    public BufferChunk(byte[] array, int offset, int length, bool returnToPool)
    {
        Debug.Assert(array is not null, "expected valid array input");
        Debug.Assert(length >= 0, "expected valid length");
        OversizedArray = array;
        Offset = offset;
        _lengthAndPoolFlag = length | (returnToPool ? FlagReturnToPool : 0);
        Debug.Assert(ReturnToPool == returnToPool, "return-to-pool not respected");
        Debug.Assert(Length == length, "length not respected");
    }

    public readonly byte[] ToArray()
    {
        int length = Length;
        if (length == 0)
        {
            return [];
        }

        byte[] copy = new byte[length];
        OversizedArray.AsSpan(Offset, length).CopyTo(copy);
        return copy;
    }

    internal void RecycleIfAppropriate()
    {
        if (ReturnToPool)
        {
            ArrayPool<byte>.Shared.Return(OversizedArray!);
        }

        this = default; // anti foot-shotgun double-return guard; not 100%, but worth doing
        Debug.Assert(OversizedArray is null && !ReturnToPool, "expected clean slate after recycle");
    }

    internal readonly ArraySegment<byte> AsArraySegment() => Length == 0 ? default! : new(OversizedArray!, Offset, Length);

    internal readonly ReadOnlySpan<byte> AsSpan() => Length == 0 ? default : new(OversizedArray!, Offset, Length);

    internal readonly ReadOnlySequence<byte> AsSequence() => Length == 0 ? default : new ReadOnlySequence<byte>(OversizedArray!, Offset, Length);

    internal readonly BufferChunk DoNotReturnToPool()
    {
        BufferChunk copy = this;
        copy._lengthAndPoolFlag &= ~FlagReturnToPool;
        Debug.Assert(copy.Length == Length, "same length expected");
        Debug.Assert(!copy.ReturnToPool, "do not return to pool");
        return copy;
    }
}
