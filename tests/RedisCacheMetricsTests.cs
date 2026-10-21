using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.Cache;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>The cache's counters are process-wide, so the tests that read them run alone.</summary>
[CollectionDefinition(nameof(RedisCacheMetricsTests), DisableParallelization = true)]
public sealed class CacheMetricsCollection;

/// <summary>What the hit and miss counters say about entries that read back as nothing.</summary>
[Collection(nameof(RedisCacheMetricsTests))]
public class RedisCacheMetricsTests
{
    public sealed class Payload
    {
        public string? Field1 { get; set; }
    }

    /// <summary>An entry whose payload is the JSON literal null reads back as a miss and must be counted as one, or the hit ratio lies.</summary>
    [Fact]
    public async Task GetAsync_PayloadIsJsonNull_IsCountedAsAMiss()
    {
        var database = new Mock<IDatabase>();
        database.Setup(d => d.HashGetAsync("key", It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync([-1, -1, "null"u8.ToArray()]);
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(database.Object);
        var connection = new Mock<IRedisConnection>();
        connection.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).ReturnsAsync(multiplexer.Object);
        var cache = new RedisCache(connection.Object, Options.Create(new RedisOptions()), Options.Create(new RedisCacheOptions()), NullLogger<RedisCache>.Instance);
        long hits = 0, misses = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, subscribed) =>
        {
            if (instrument.Meter.Name == RedisMetrics.MeterName)
                subscribed.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "snail.redis.cache.hits")
                Interlocked.Add(ref hits, value);
            if (instrument.Name == "snail.redis.cache.misses")
                Interlocked.Add(ref misses, value);
        });
        listener.Start();

        var value = await cache.GetAsync<Payload>("key");

        Assert.Null(value);
        Assert.Equal((0, 1), (Interlocked.Read(ref hits), Interlocked.Read(ref misses)));
    }
}
