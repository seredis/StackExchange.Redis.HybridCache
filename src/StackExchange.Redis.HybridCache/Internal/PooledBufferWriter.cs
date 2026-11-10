using System;
using System.Buffers;

namespace StackExchange.Redis.HybridCache.Internal;

/// <summary>
/// A minimal ArrayPool-backed <see cref="IBufferWriter{T}"/> for serialization
/// (ArrayBufferWriter is not available on netstandard2.0).
/// </summary>
internal sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(512);
    private int _written;

    public long Length => _written;

    public ReadOnlySequence<byte> WrittenSequence => new(_buffer, 0, _written);

    public void Advance(int count) => _written += count;

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    public void Dispose()
    {
        byte[] buffer = _buffer;
        _buffer = [];
        _written = 0;
        if (buffer.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint < 1)
        {
            sizeHint = 1;
        }

        if (_buffer.Length - _written < sizeHint)
        {
            byte[] newBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(_buffer.Length * 2, _written + sizeHint));
            _buffer.AsSpan(0, _written).CopyTo(newBuffer);
            byte[] old = _buffer;
            _buffer = newBuffer;
            ArrayPool<byte>.Shared.Return(old);
        }
    }
}
