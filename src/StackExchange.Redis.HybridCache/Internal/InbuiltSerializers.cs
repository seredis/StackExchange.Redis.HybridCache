using System;
using System.Buffers;
using System.Text.Json;
using Microsoft.Extensions.Caching.Hybrid;

namespace StackExchange.Redis.HybridCache.Internal;

/// <summary>
/// Serializers used when nothing is registered: UTF-8 for <see cref="string"/>, raw bytes for
/// <c>byte[]</c>, System.Text.Json for everything else — matching the default implementation's
/// choices, so payload bytes stay interchangeable.
/// </summary>
internal static class InbuiltSerializers
{
    public sealed class StringSerializer : IHybridCacheSerializer<string>
    {
        public static StringSerializer Instance { get; } = new();

        public string Deserialize(ReadOnlySequence<byte> source)
        {
            byte[] arr = source.ToArray(); // netstandard2.0-friendly; optimize later
            return HybridCachePayload.Encoding.GetString(arr, 0, arr.Length);
        }

        public void Serialize(string value, IBufferWriter<byte> target)
        {
            byte[] arr = HybridCachePayload.Encoding.GetBytes(value);
            target.Write(arr);
        }
    }

    public sealed class ByteArraySerializer : IHybridCacheSerializer<byte[]>
    {
        public static ByteArraySerializer Instance { get; } = new();

        public byte[] Deserialize(ReadOnlySequence<byte> source) => source.ToArray();

        public void Serialize(byte[] value, IBufferWriter<byte> target) => target.Write(value);
    }

    public sealed class DefaultJsonSerializer<T> : IHybridCacheSerializer<T>
    {
        private readonly JsonSerializerOptions? _options;

        public DefaultJsonSerializer(JsonSerializerOptions? options = null) => _options = options;

        public T Deserialize(ReadOnlySequence<byte> source)
        {
            var reader = new Utf8JsonReader(source);
            return JsonSerializer.Deserialize<T>(ref reader, _options)!;
        }

        public void Serialize(T value, IBufferWriter<byte> target)
        {
            using var writer = new Utf8JsonWriter(target);
            JsonSerializer.Serialize(writer, value, _options);
        }
    }
}
