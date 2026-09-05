using Microsoft.Extensions.Logging;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Collects the level of every log entry so a test can assert that something was reported.</summary>
public sealed class ListLoggerProvider(List<LogLevel> levels) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new ListLogger(levels);

    public void Dispose()
    {
    }

    private sealed class ListLogger(List<LogLevel> levels) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (levels)
                levels.Add(logLevel);
        }
    }
}
