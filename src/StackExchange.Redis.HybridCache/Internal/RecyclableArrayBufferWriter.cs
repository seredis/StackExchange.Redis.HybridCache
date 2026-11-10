// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/RecyclableArrayBufferWriter.cs).

using System;
using System.Buffers;
using System.Diagnostics;
using System.Threading;

namespace StackExchange.Redis.HybridCache.Internal;

// this is effectively a cut-down re-implementation of ArrayBufferWriter, except it uses
// the array pool for allocations, enforces a quota (MaximumPayloadBytes), and allows the
// committed buffer to be detached (for retention in a CacheItem / write to the backend)
internal sealed class RecyclableArrayBufferWriter<T> : IBufferWriter<T>, IDisposable
{
    // Usage note: *normally* you might want to use "using" for this, and that is fine;
    // however, caution should be exercised in exception scenarios where we don't 100%
    // know that the caller has stopped touching the buffer; in particular, this means
    // scenarios involving a combination of external code and (for example) "async".
    // In those cases, it may be preferable to manually dispose in the success case,
    // and just drop the buffers in the failure case.

    private const int ArrayMaxLength = 0x7FFFFFC7; // copy of Array.MaxLength, for netfx

    private const int DefaultInitialBufferSize = 256;

    private static RecyclableArrayBufferWriter<T>? _spare;

    private T[] _buffer;
    private int _index;
    private int _maxLength;

    private RecyclableArrayBufferWriter()
    {
        _buffer = [];
    }

    public int CommittedBytes => _index;

    public int FreeCapacity => _buffer.Length - _index;

    public bool QuotaExceeded { get; private set; }

    public static RecyclableArrayBufferWriter<T> Create(int maxLength)
    {
        RecyclableArrayBufferWriter<T> obj = Interlocked.Exchange(ref _spare, null) ?? new();
        obj.Initialize(maxLength);
        return obj;
    }

    public void Dispose()
    {
        // attempt to reuse everything via "spare"; if that isn't possible,
        // recycle the buffers instead
        _index = 0;
        if (Interlocked.CompareExchange(ref _spare, this, null) != null)
        {
            T[] tmp = _buffer;
            _buffer = [];
            if (tmp.Length != 0)
            {
                ArrayPool<T>.Shared.Return(tmp);
            }
        }
    }

    public void Advance(int count)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        if (_index > _buffer.Length - count)
        {
            ThrowCount();
        }

        if (_index + count > _maxLength)
        {
            QuotaExceeded = true;
            ThrowQuota();
        }

        _index += count;

        static void ThrowCount()
            => throw new ArgumentOutOfRangeException(nameof(count));

        static void ThrowQuota()
            => throw new InvalidOperationException("Max length exceeded");
    }

    public ReadOnlyMemory<T> GetCommittedMemory() => new(_buffer, 0, _index);

    public Memory<T> GetMemory(int sizeHint = 0)
    {
        CheckAndResizeBuffer(sizeHint);
        Debug.Assert(_buffer.Length > _index, "should have some space");
        return _buffer.AsMemory(_index);
    }

    public Span<T> GetSpan(int sizeHint = 0)
    {
        CheckAndResizeBuffer(sizeHint);
        Debug.Assert(_buffer.Length > _index, "should have some space");
        return _buffer.AsSpan(_index);
    }

    // create a standalone isolated copy of the buffer
    public T[] ToArray() => _buffer.AsSpan(0, _index).ToArray();

    public ReadOnlySequence<T> AsSequence() => new(_buffer, 0, _index);

    /// <summary>
    /// Disconnect the current buffer so that we can store it without it being recycled.
    /// </summary>
    internal T[] DetachCommitted(out int length)
    {
        T[] tmp = _index == 0 ? [] : _buffer;
        length = _index;

        _buffer = [];
        _index = 0;

        return tmp;
    }

    private void CheckAndResizeBuffer(int sizeHint)
    {
        if (sizeHint <= 0)
        {
            sizeHint = 1;
        }

        if (sizeHint > FreeCapacity)
        {
            int currentLength = _buffer.Length;

            // Attempt to grow by the larger of the sizeHint and double the current size.
            int growBy = Math.Max(sizeHint, currentLength);

            if (currentLength == 0)
            {
                growBy = Math.Max(growBy, DefaultInitialBufferSize);
            }

            int newSize = currentLength + growBy;

            if ((uint)newSize > int.MaxValue)
            {
                // Attempt to grow to ArrayMaxLength.
                uint needed = (uint)(currentLength - FreeCapacity + sizeHint);
                Debug.Assert(needed > currentLength, "should need to grow");

                if (needed > ArrayMaxLength)
                {
                    ThrowOutOfMemoryException();
                }

                newSize = ArrayMaxLength;
            }

            // resize the backing buffer
            T[] oldArray = _buffer;
            _buffer = ArrayPool<T>.Shared.Rent(newSize);
            oldArray.AsSpan(0, _index).CopyTo(_buffer);
            if (oldArray.Length != 0)
            {
                ArrayPool<T>.Shared.Return(oldArray);
            }
        }

        Debug.Assert(FreeCapacity > 0 && FreeCapacity >= sizeHint, "should be space");

        static void ThrowOutOfMemoryException() => throw new InvalidOperationException("Unable to grow buffer as requested");
    }

    private void Initialize(int maxLength)
    {
        // think .ctor, but with pooled object re-use
        _index = 0;
        _maxLength = maxLength;
        QuotaExceeded = false;
    }
}
