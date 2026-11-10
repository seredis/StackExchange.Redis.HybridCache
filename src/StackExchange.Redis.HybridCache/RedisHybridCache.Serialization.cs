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

    internal IHybridCacheSerializer<T> GetSerializer<T>()
        => (IHybridCacheSerializer<T>)_serializers.GetOrAdd(typeof(T), _ => ResolveSerializer<T>()); // netstandard2.0: no TArg overload

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
            _logger.LogError(ex, "Cache payload deserialization failure; treating as a miss.");
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
