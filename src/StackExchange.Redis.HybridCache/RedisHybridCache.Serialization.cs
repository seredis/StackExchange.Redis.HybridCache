using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis.HybridCache.Internal;

namespace StackExchange.Redis.HybridCache;

public sealed partial class RedisHybridCache
{
    private readonly IServiceProvider? _services;
    private readonly ConcurrentDictionary<Type, object> _serializers = new();

    internal int MaximumPayloadBytes { get; }

    internal IHybridCacheSerializer<T> GetSerializer<T>()
        => _serializers.TryGetValue(typeof(T), out object? existing)
            ? (IHybridCacheSerializer<T>)existing // the hot path: no delegate, no allocation
            : (IHybridCacheSerializer<T>)_serializers.GetOrAdd(typeof(T), _ => ResolveSerializer<T>()); // netstandard2.0: no TArg overload

    internal bool TrySerialize<T>(T value, out BufferChunk buffer, out IHybridCacheSerializer<T>? serializer)
    {
        // note: also returns the serializer we resolved, because most-any time we want to serialize, we'll also want
        // to make sure we use that same instance later (without needing to re-resolve)

        RecyclableArrayBufferWriter<byte>? writer = null;
        buffer = default;
        try
        {
            writer = RecyclableArrayBufferWriter<byte>.Create(MaximumPayloadBytes); // note this lifetime spans the backend write
            serializer = GetSerializer<T>();

            serializer.Serialize(value, writer);

            buffer = new(writer.DetachCommitted(out int length), 0, length, returnToPool: true); // remove buffer ownership from the writer
            writer.Dispose(); // we're done with the writer
            return true;
        }
        catch (Exception ex)
        {
            bool knownCause = false;

            // ^^^ if we know what happened, we can record directly via cause-specific events
            // and treat as a handled failure (i.e. return false) - otherwise, we'll bubble
            // the fault up a few layers *in addition to* logging in a failure event

            if (writer is not null)
            {
                if (writer.QuotaExceeded)
                {
                    _logger.MaximumPayloadBytesExceeded(ex, MaximumPayloadBytes);
                    knownCause = true;
                }

                writer.Dispose();
            }

            if (!knownCause)
            {
                _logger.SerializationFailure(ex);
                throw;
            }

            buffer = default;
            serializer = null;
            return false;
        }
    }

    private bool TryDeserialize<T>(ArraySegment<byte> payload, out T value)
    {
        try
        {
            value = GetSerializer<T>().Deserialize(new ReadOnlySequence<byte>(payload.Array!, payload.Offset, payload.Count));
            return true;
        }
        catch (Exception ex)
        {
            // an undeserializable payload (foreign serializer, schema change) is a logged miss
            _logger.DeserializationFailure(ex);
            value = default!;
            return false;
        }
    }

    private object ResolveSerializer<T>()
    {
        // resolution order matches the stock implementation: an explicit per-type registration,
        // then factories (last registered wins), then the inbuilt string/byte[]/JSON handling
        if (_services is not null)
        {
            if (_services.GetService<IHybridCacheSerializer<T>>() is { } configured)
            {
                return configured;
            }

            if (_services.GetService<IEnumerable<IHybridCacheSerializerFactory>>() is { } factories)
            {
                IHybridCacheSerializer<T>? fromFactory = null;
                foreach (IHybridCacheSerializerFactory factory in factories)
                {
                    if (factory.TryCreateSerializer<T>(out IHybridCacheSerializer<T>? candidate) && candidate is not null)
                    {
                        fromFactory = candidate; // keep going: last registered wins
                    }
                }

                if (fromFactory is not null)
                {
                    return fromFactory;
                }
            }
        }

        if (typeof(T) == typeof(string))
        {
            return InbuiltSerializers.StringSerializer.Instance;
        }

        if (typeof(T) == typeof(byte[]))
        {
            return InbuiltSerializers.ByteArraySerializer.Instance;
        }

        return new InbuiltSerializers.DefaultJsonSerializer<T>();
    }
}
