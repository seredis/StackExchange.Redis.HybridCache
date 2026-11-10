// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/TagSet.cs).

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace StackExchange.Redis.HybridCache.Internal;

/// <summary>
/// Represents zero (null), one (string) or more (string[]) tags, avoiding the additional array overhead when necessary.
/// </summary>
[DebuggerDisplay("{ToString()}")]
internal readonly struct TagSet
{
    public static readonly TagSet Empty = default!;

    internal const string WildcardTag = "*";

    // this array is used in CopyTo to efficiently copy out of collections
    [ThreadStatic]
    private static string[]? _perThreadSingleLengthArray;

    private readonly object? _tagOrTags;

    internal TagSet(string tag)
    {
        Validate(tag);
        _tagOrTags = tag;
    }

    internal TagSet(string[] tags)
    {
        Debug.Assert(tags is { Length: > 1 }, "should be non-trivial array");
        foreach (string tag in tags)
        {
            Validate(tag);
        }

        Array.Sort(tags, StringComparer.InvariantCulture);
        _tagOrTags = tags;
    }

    public bool IsEmpty => _tagOrTags is null;

    public int Count => _tagOrTags switch
    {
        null => 0,
        string => 1,
        string[] arr => arr.Length,
        _ => 0, // should never happen, but treat as empty
    };

    internal bool IsArray => _tagOrTags is string[];

    public string this[int index] => _tagOrTags switch
    {
        string tag when index == 0 => tag,
        string[] tags => tags[index],
        _ => throw new IndexOutOfRangeException(nameof(index)),
    };

    public string GetSinglePrechecked() => (string)_tagOrTags!; // we expect this to fail if used on incorrect types

    public Span<string> GetSpanPrechecked() => (string[])_tagOrTags!; // we expect this to fail if used on incorrect types

    public override bool Equals(object? obj) => throw new NotSupportedException();

    public override int GetHashCode() => throw new NotSupportedException();

    public override string ToString() => _tagOrTags switch
    {
        string tag => tag,
        string[] tags => string.Join(", ", tags),
        _ => "(no tags)",
    };

    public void CopyTo(Span<string> target)
    {
        switch (_tagOrTags)
        {
            case string tag:
                target[0] = tag;
                break;
            case string[] tags:
                tags.CopyTo(target);
                break;
        }
    }

    internal static TagSet Create(IEnumerable<string>? tags)
    {
        if (tags is null)
        {
            return Empty;
        }

        // note that in multi-tag scenarios we always create a defensive copy
        if (tags is ICollection<string> collection)
        {
            switch (collection.Count)
            {
                case 0:
                    return Empty;
                case 1 when collection is IList<string> list:
                    return new TagSet(list[0]);
                case 1:
                    // avoid the GetEnumerator() alloc
                    string[] arr = _perThreadSingleLengthArray ??= new string[1];
                    collection.CopyTo(arr, 0);
                    return new TagSet(arr[0]);
                default:
                    arr = new string[collection.Count];
                    collection.CopyTo(arr, 0);
                    return new TagSet(arr);
            }
        }

        // perhaps overkill, but: avoid as much as possible when unrolling
        using IEnumerator<string> iterator = tags.GetEnumerator();
        if (!iterator.MoveNext())
        {
            return Empty;
        }

        string firstTag = iterator.Current;
        if (!iterator.MoveNext())
        {
            return new TagSet(firstTag);
        }

        string[] oversized = ArrayPool<string>.Shared.Rent(8);
        oversized[0] = firstTag;
        int count = 1;
        do
        {
            if (count == oversized.Length)
            {
                // grow
                string[] bigger = ArrayPool<string>.Shared.Rent(count * 2);
                oversized.CopyTo(bigger, 0);
                ArrayPool<string>.Shared.Return(oversized);
                oversized = bigger;
            }

            oversized[count++] = iterator.Current;
        }
        while (iterator.MoveNext());

        if (count == oversized.Length)
        {
            return new TagSet(oversized);
        }
        else
        {
            string[] final = oversized.AsSpan(0, count).ToArray();
            ArrayPool<string>.Shared.Return(oversized);
            return new TagSet(final);
        }
    }

    internal string[] ToArray()
    {
        string[] arr = new string[Count];
        CopyTo(arr);
        return arr;
    }

    internal bool TryFind(ReadOnlySpan<char> span, [NotNullWhen(true)] out string? tag)
    {
        switch (_tagOrTags)
        {
            case string single when span.SequenceEqual(single.AsSpan()):
                tag = single;
                return true;
            case string[] tags:
                foreach (string test in tags)
                {
                    if (span.SequenceEqual(test.AsSpan()))
                    {
                        tag = test;
                        return true;
                    }
                }

                break;
        }

        tag = null;
        return false;
    }

    internal int MaxLength()
    {
        switch (_tagOrTags)
        {
            case string single:
                return single.Length;
            case string[] tags:
                int max = 0;
                foreach (string test in tags)
                {
                    max = Math.Max(max, test.Length);
                }

                return max;
            default:
                return 0;
        }
    }

    private static void Validate(string tag)
    {
        if (string.IsNullOrWhiteSpace(tag))
        {
            ThrowEmpty();
        }

        if (tag == WildcardTag)
        {
            ThrowReserved();
        }

        static void ThrowEmpty() => throw new ArgumentException("Tags cannot be empty.", "tags");
        static void ThrowReserved() => throw new ArgumentException($"The tag '{WildcardTag}' is reserved and cannot be used in this context.", "tags");
    }
}
