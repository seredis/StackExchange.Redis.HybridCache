// The event tests listen to the PROCESS-GLOBAL HybridCacheEventSource.Log (see the notes in
// TestEventListener for why a private same-named instance is not an option), so while they
// listen, every cache operation in the process emits into their capture. This collection runs
// them exclusively: no other tests execute concurrently, so single-event asserts hold and the
// zero-alloc tests (which skip while a listener is active) never lose their window to one.

using Xunit;

namespace StackExchange.Redis.HybridCache.Tests;

[CollectionDefinition(nameof(EventSourceCollection), DisableParallelization = true)]
public sealed class EventSourceCollection;
