using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace StackExchange.Redis.HybridCache.MixedMode.Tests;

public sealed record LogEntry(string Category, LogLevel Level, EventId EventId, string Message, Exception? Exception);

public sealed class LogCollector : ILoggerProvider
{
    public ConcurrentQueue<LogEntry> Entries { get; } = new();

    public ILogger CreateLogger(string categoryName) => new Logger(categoryName, Entries);

    public void Dispose()
    {
    }

    private sealed class Logger(string category, ConcurrentQueue<LogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => entries.Enqueue(new(category, logLevel, eventId, formatter(state, exception), exception));
    }
}
