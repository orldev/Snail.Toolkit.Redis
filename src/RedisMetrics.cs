using System.Diagnostics.Metrics;

namespace Snail.Toolkit.Redis;

/// <summary>Counters the library publishes; subscribe to <see cref="MeterName"/> with AddMeter or a MeterListener.</summary>
public static class RedisMetrics
{
    /// <summary>Name of the meter every counter below belongs to.</summary>
    public const string MeterName = "Snail.Toolkit.Redis";

    private static readonly Meter Meter = new(MeterName);

    internal static readonly Counter<long> CacheHits = Meter.CreateCounter<long>("snail.redis.cache.hits", description: "Cache reads that returned a value.");
    internal static readonly Counter<long> CacheMisses = Meter.CreateCounter<long>("snail.redis.cache.misses", description: "Cache reads that found nothing readable.");
    internal static readonly Counter<long> CacheFailures = Meter.CreateCounter<long>("snail.redis.cache.failures", description: "Cache reads and writes degraded because Redis was unreachable.");
    internal static readonly Counter<long> Published = Meter.CreateCounter<long>("snail.redis.pubsub.published", description: "Messages published to a channel.");
    internal static readonly Counter<long> Received = Meter.CreateCounter<long>("snail.redis.pubsub.received", description: "Messages received from Redis, counted once per channel feed.");
    internal static readonly Counter<long> Dropped = Meter.CreateCounter<long>("snail.redis.pubsub.dropped", description: "Messages dropped because a reader's buffer was full.");
    internal static readonly Counter<long> ConnectionFailures = Meter.CreateCounter<long>("snail.redis.connection.failures", description: "Connection failures reported by the multiplexer.");
    internal static readonly Counter<long> ConnectionRestores = Meter.CreateCounter<long>("snail.redis.connection.restores", description: "Connections restored after a failure.");
    internal static readonly Counter<long> ConnectionReplacements = Meter.CreateCounter<long>("snail.redis.connection.replacements", description: "Connections moved to new endpoints after a configuration reload.");
}
