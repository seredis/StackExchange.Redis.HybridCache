// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/DefaultJsonSerializerFactory.cs).

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;

namespace StackExchange.Redis.HybridCache.Internal;

internal sealed class DefaultJsonSerializerFactory : IHybridCacheSerializerFactory
{
    private readonly IServiceProvider _serviceProvider;

    internal static JsonSerializerOptions FieldEnabledJsonOptions { get; } = new() { IncludeFields = true };

    internal JsonSerializerOptions Options { get; }

    public DefaultJsonSerializerFactory(IServiceProvider serviceProvider)
    {
        // store the service provider and obtain the default JSON options, keyed by the **open** generic interface type
        _serviceProvider = serviceProvider;

        Options = (serviceProvider as IKeyedServiceProvider)?.GetKeyedService<JsonSerializerOptions>(typeof(IHybridCacheSerializer<>)) ?? SystemDefaultJsonOptions;
    }

    private static JsonSerializerOptions SystemDefaultJsonOptions => JsonSerializerOptions.Default;

    public bool TryCreateSerializer<T>([NotNullWhen(true)] out IHybridCacheSerializer<T>? serializer)
    {
        // no restriction - accept any type (i.e. always return true)

        // see if there is a per-type options registered (keyed by the **closed** generic type), otherwise use the default
        JsonSerializerOptions options = (_serviceProvider as IKeyedServiceProvider)?.GetKeyedService<JsonSerializerOptions>(typeof(IHybridCacheSerializer<T>)) ?? Options;
        if (!options.IncludeFields && ReferenceEquals(options, SystemDefaultJsonOptions) && IsFieldOnlyType(typeof(T)))
        {
            // value-tuples expose fields, not properties; special-case this as a common scenario
            options = FieldEnabledJsonOptions;
        }

        serializer = new InbuiltSerializers.DefaultJsonSerializer<T>(options);
        return true;
    }

    internal static bool IsFieldOnlyType(Type type)
    {
        Dictionary<Type, FieldOnlyResult>? state = null; // only needed for complex types
        return IsFieldOnlyType(type, ref state) == FieldOnlyResult.FieldOnly;
    }

    private static FieldOnlyResult IsFieldOnlyType(
        Type type, ref Dictionary<Type, FieldOnlyResult>? state)
    {
        if (type is null || type.IsPrimitive || type == typeof(string))
        {
            return FieldOnlyResult.NotFieldOnly;
        }

        // re-use existing results, and more importantly: prevent infinite recursion
        if (state is not null && state.TryGetValue(type, out FieldOnlyResult existingResult))
        {
            return existingResult;
        }

        // check for collection types; start at IEnumerable and then look for IEnumerable<T>
        // (this is broadly comparable to STJ)
        if (typeof(IEnumerable).IsAssignableFrom(type))
        {
            PrepareStateForDepth(type, ref state);
            foreach (Type iType in type.GetInterfaces())
            {
                if (iType.IsGenericType && iType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    if (IsFieldOnlyType(iType.GetGenericArguments()[0], ref state) == FieldOnlyResult.FieldOnly)
                    {
                        return SetState(type, state, true);
                    }
                }
            }

            // no problems detected
            return SetState(type, state, false);
        }

        // not a collection; check for field-only scenario - look for properties first
        PropertyInfo[] props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        if (props.Length != 0)
        {
            PrepareStateForDepth(type, ref state);
            foreach (PropertyInfo prop in props)
            {
                if (IsFieldOnlyType(prop.PropertyType, ref state) == FieldOnlyResult.FieldOnly)
                {
                    return SetState(type, state, true);
                }
            }

            // then we *do* have public instance properties, that aren't themselves problems; we're good
            return SetState(type, state, false);
        }

        // no properties; if there are fields, this is the problem scenario we're trying to detect
        bool haveFields = type.GetFields(BindingFlags.Public | BindingFlags.Instance).Length != 0;
        return SetState(type, state, haveFields);

        static void PrepareStateForDepth(Type type, ref Dictionary<Type, FieldOnlyResult>? state)
        {
            state ??= [];
            if (!state.ContainsKey(type))
            {
                state.Add(type, FieldOnlyResult.Incomplete);
            }
        }

        static FieldOnlyResult SetState(Type type, Dictionary<Type, FieldOnlyResult>? state, bool result)
        {
            FieldOnlyResult value = result ? FieldOnlyResult.FieldOnly : FieldOnlyResult.NotFieldOnly;
            if (state is not null)
            {
                state[type] = value;
            }

            return value;
        }
    }

    // used to store intermediate state when calculating IsFieldOnlyType
    private enum FieldOnlyResult
    {
        Incomplete = 0,
        FieldOnly = 1,
        NotFieldOnly = 2,
    }
}
