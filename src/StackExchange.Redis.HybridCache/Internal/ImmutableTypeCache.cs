// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/ImmutableTypeCache.cs
// and ImmutableTypeCache.T.cs).

using System;
using System.ComponentModel;
using System.Reflection;

#if NET8_0_OR_GREATER
using System.Runtime.CompilerServices;
#else
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
#endif

namespace StackExchange.Redis.HybridCache.Internal;

/// <summary>
/// Utility type for determining whether a type is blittable; the logic for this is very
/// TFM dependent.
/// </summary>
internal static class ImmutableTypeCache
{
    internal static bool IsBlittable<T>() // minimize the generic portion (twinned with IsTypeImmutable)
    {
#if NET8_0_OR_GREATER
        return !RuntimeHelpers.IsReferenceOrContainsReferences<T>();
#else
        // down-level: only blittable types can be pinned
        try
        {
            // get a typed, zeroed, non-null boxed instance of the appropriate type
            // (can't use (object)default(T), as that would box to null for nullable types)
            object obj = FormatterServices.GetUninitializedObject(Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T));
            GCHandle.Alloc(obj, GCHandleType.Pinned).Free();
            return true;
        }
        catch
        {
            return false;
        }
#endif
    }

    internal static bool IsTypeImmutable(Type type)
    {
        // check for known types
        if (type == typeof(string))
        {
            return true;
        }

        if (type.IsValueType)
        {
            // switch from Foo? to Foo if necessary
            if (Nullable.GetUnderlyingType(type) is { } nullable)
            {
                type = nullable;
            }
        }

        if (type.IsValueType || (type.IsClass & type.IsSealed))
        {
            // check for [ImmutableObject(true)]; note we're looking at this as a statement about
            // the overall immutability; for example, a type could contain a private int[] field,
            // where the field is mutable and the list is mutable; but if the type is annotated:
            // we're trusting that the API and use-case is such that the type is immutable
            return type.GetCustomAttribute<ImmutableObjectAttribute>() is { Immutable: true };
        }

        // don't trust interfaces and non-sealed types; we might have any concrete
        // type that has different behaviour
        return false;
    }
}

/// <summary>
/// Simple memoize storage for whether the type <typeparamref name="T"/> requires defensive copies.
/// The JIT is very effective at accessing this memoized value.
/// </summary>
/// <typeparam name="T">The type being processed.</typeparam>
internal static class ImmutableTypeCache<T> // lazy memoize; T doesn't change per cache instance
{
    // note for blittable types: a pure struct will be a full copy every time - nothing shared to mutate
    public static readonly bool IsImmutable = (typeof(T).IsValueType && ImmutableTypeCache.IsBlittable<T>()) || ImmutableTypeCache.IsTypeImmutable(typeof(T));
}
