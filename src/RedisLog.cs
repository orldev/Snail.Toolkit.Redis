using Microsoft.Extensions.Logging;

namespace Snail.Toolkit.Redis;

/// <summary>Every log line the library writes, generated so a disabled level costs nothing.</summary>
internal static partial class RedisLog
{
    [LoggerMessage(1, LogLevel.Information, "Redis connection established: {Configuration}")]
    public static partial void Connected(this ILogger logger, string configuration);

    [LoggerMessage(2, LogLevel.Warning, "Redis is not reachable at start: {Configuration}; reconnecting in the background")]
    public static partial void NotReachableAtStart(this ILogger logger, string configuration);

    [LoggerMessage(3, LogLevel.Error, "Redis configuration reloaded with invalid settings; the previous settings stay in effect")]
    public static partial void InvalidReload(this ILogger logger, Exception exception);

    [LoggerMessage(4, LogLevel.Information, "Redis password rotated; reconnects will use the new credentials")]
    public static partial void PasswordRotated(this ILogger logger);

    [LoggerMessage(5, LogLevel.Information, "Redis endpoints changed in configuration; the connection moved to {Configuration}")]
    public static partial void EndpointsReplaced(this ILogger logger, string configuration);

    [LoggerMessage(14, LogLevel.Error, "Redis endpoints changed in configuration but {Configuration} could not be opened; the previous connection stays in use")]
    public static partial void ReplacementFailed(this ILogger logger, Exception exception, string configuration);

    [LoggerMessage(15, LogLevel.Error, "Subscription to {Channel} could not be moved to the new connection; its readers were completed")]
    public static partial void ReopenFailed(this ILogger logger, Exception exception, string channel);

    [LoggerMessage(16, LogLevel.Warning, "Skipped the entry {Id} of stream {Stream} because it could not be read as {Type}")]
    public static partial void SkippedUnreadableEntry(this ILogger logger, Exception exception, string stream, string id, string type);

    [LoggerMessage(6, LogLevel.Warning, "Evicted the cache entry {Key} because it could not be read as {Type}")]
    public static partial void EvictedUnreadableEntry(this ILogger logger, Exception exception, string key, string type);

    [LoggerMessage(7, LogLevel.Warning, "Redis is unreachable; the cache reports a miss for {Key} and skips writes until it is back")]
    public static partial void CacheUnavailable(this ILogger logger, Exception exception, string key);

    [LoggerMessage(8, LogLevel.Warning, "Dropped a message from channel {Channel} that could not be deserialized as {Type}")]
    public static partial void DroppedUnreadableMessage(this ILogger logger, Exception exception, string channel, string type);

    [LoggerMessage(9, LogLevel.Warning, "Subscriber of {Channel} is too slow: {Dropped} message(s) dropped so far, buffer capacity {Capacity}")]
    public static partial void SubscriberTooSlow(this ILogger logger, string channel, long dropped, int capacity);

    [LoggerMessage(10, LogLevel.Debug, "Subscription to {Channel} was released after the connection had closed")]
    public static partial void ReleasedAfterClose(this ILogger logger, Exception exception, string channel);

    [LoggerMessage(11, LogLevel.Warning, "Redis connection to {EndPoint} failed: {FailureType}")]
    public static partial void ConnectionFailed(this ILogger logger, Exception? exception, string? endPoint, string failureType);

    [LoggerMessage(12, LogLevel.Information, "Redis connection to {EndPoint} restored")]
    public static partial void ConnectionRestored(this ILogger logger, string? endPoint);

    [LoggerMessage(13, LogLevel.Warning, "Redis server {EndPoint} reported: {Message}")]
    public static partial void ServerError(this ILogger logger, string? endPoint, string message);

    [LoggerMessage(17, LogLevel.Warning, "Left the key {Key} alone: it holds another type than an entry for {Type}, and without an instance name the cache cannot call it its own")]
    public static partial void LeftForeignEntry(this ILogger logger, Exception exception, string key, string type);

    [LoggerMessage(18, LogLevel.Warning, "Reading stream {Stream} after {Position} failed; the follower reads again after the next wake-up")]
    public static partial void StreamReadFailed(this ILogger logger, Exception exception, string stream, string position);

    [LoggerMessage(19, LogLevel.Warning, "The entry {Id} of stream {Stream} was appended but could not be announced; followers pick it up at their next poll")]
    public static partial void AnnouncementFailed(this ILogger logger, Exception exception, string stream, string id);
}
