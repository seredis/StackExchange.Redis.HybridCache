// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Adapted from dotnet/extensions (Microsoft.Extensions.Caching.Hybrid, Internal/DefaultHybridCache.Stampede.cs).
// Difference: the stock implementation re-checks its L1 inside the contested lock (a completing
// operation may have just written it); there is no L1 dictionary here - instead, a stampede created
// through the contested path performs the backend read itself (see newStateCanSkipRead), because
// concurrent activity on the key means a fresh value may just have landed.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Caching.Hybrid;
using StackExchange.Redis.HybridCache.Internal;

// the namespace shadows the base type's name; alias it once
using HybridCacheBase = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace StackExchange.Redis.HybridCache;

public sealed partial class RedisHybridCache
{
    private readonly ConcurrentDictionary<StampedeKey, StampedeState> _currentOperations = new();

    // returns true for a new session (in which case: we need to start the work), false for a pre-existing session;
    // for a new session, newStateCanSkipRead says whether the caller's own fresh backend miss is still good evidence
    internal bool GetOrCreateStampedeState<TState, T>(
        string key,
        HybridCacheEntryFlags flags,
        out StampedeState<TState, T> stampedeState,
        bool canBeCanceled,
        IEnumerable<string>? tags,
        out bool newStateCanSkipRead)
    {
        var stampedeKey = new StampedeKey(key, flags);
        newStateCanSkipRead = true;

        // Double-checked locking to try to avoid unnecessary sessions in race conditions,
        // while avoiding the lock completely whenever possible.
        if (TryJoinExistingSession(this, stampedeKey, out StampedeState<TState, T>? existing))
        {
            stampedeState = existing;
            return false; // someone ELSE is running the work
        }

        // Most common scenario here, then, is that we're not fighting with anyone else;
        // go ahead and create a placeholder state object and *try* to add it.
        stampedeState = new StampedeState<TState, T>(this, stampedeKey, TagSet.Create(tags), canBeCanceled);
        if (_currentOperations.TryAdd(stampedeKey, stampedeState))
        {
            // successfully added; indeed, no-one else was fighting: we're done
            return true; // the CURRENT caller is responsible for making the work happen
        }

        // Hmmm, failed to add - there's concurrent activity on the same key; we're now
        // in very rare race condition territory; go ahead and take a lock while we
        // collect our thoughts.

        // see notes in RedisHybridCache.SyncLock.cs
        lock (GetPartitionedSyncLock(in stampedeKey))
        {
            // check again while we hold the lock
            if (TryJoinExistingSession(this, stampedeKey, out existing))
            {
                // we found an existing state we can join; do that
                stampedeState.SetCanceled(); // to be thorough: mark our speculative one as doomed (no-one has seen it, though)
                stampedeState = existing; // and replace with the one we found
                return false; // someone ELSE is running the work
            }

            // Concurrent activity on this key means a completing operation may have *just* written a
            // fresh value; where the stock implementation re-checks its L1 here, we instead tell the
            // new stampede to perform the backend read itself rather than trusting the caller's miss.
            newStateCanSkipRead = false;

            // Otherwise, either nothing existed - or the thing that already exists can't be joined;
            // in that case, go ahead and use the state that we invented a moment ago (outside of the lock).
            _currentOperations[stampedeKey] = stampedeState;
            return true; // the CURRENT caller is responsible for making the work happen
        }

        static bool TryJoinExistingSession(RedisHybridCache @this, in StampedeKey stampedeKey,
            [NotNullWhen(true)] out StampedeState<TState, T>? stampedeState)
        {
            if (@this._currentOperations.TryGetValue(stampedeKey, out StampedeState? found))
            {
                if (found is not StampedeState<TState, T> tmp)
                {
                    ThrowWrongType(stampedeKey.Key, found.Type, typeof(T));
                }

                if (tmp.TryAddCaller())
                {
                    // we joined an existing session
                    stampedeState = tmp;
                    return true;
                }
            }

            stampedeState = null;
            return false;
        }

        [DoesNotReturn]
        static void ThrowWrongType(string key, Type existingType, Type newType)
        {
            Debug.Assert(existingType != newType, "should be different types");
            throw new InvalidOperationException(
                $"All calls to {nameof(HybridCacheBase)} with the same key should use the same data type; the same key is being used for '{existingType.FullName}' and '{newType.FullName}' data")
            {
                Data = { { "CacheKey", key } },
            };
        }
    }

    internal int DebugGetCallerCount(string key, HybridCacheEntryFlags? flags = null)
    {
        var stampedeKey = new StampedeKey(key, flags ?? _defaultFlags);
        return _currentOperations.TryGetValue(stampedeKey, out StampedeState? state) ? state.DebugCallerCount : 0;
    }
}
