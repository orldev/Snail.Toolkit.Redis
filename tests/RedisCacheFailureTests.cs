using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.Cache;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>The fail-open policy of the cache against the failures Redis itself reports, with Redis replaced by mocks.</summary>
public class RedisCacheFailureTests
{
    public sealed class Payload
    {
        public string? Field1 { get; set; }
    }

    private static readonly RedisKey Key = "app:key";

    private readonly Mock<IRedisConnection> _connection = new();
    private readonly Mock<IDatabase> _database = new();
    private readonly Mock<IBatch> _batch = new();
    private readonly List<LogLevel> _logs = [];

    public RedisCacheFailureTests()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(c => c.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(_database.Object);
        _connection.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).ReturnsAsync(multiplexer.Object);
        _database.Setup(d => d.CreateBatch(It.IsAny<object?>())).Returns(_batch.Object);
        _batch.Setup(b => b.HashSetAsync(It.IsAny<RedisKey>(), It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>())).Returns(Task.CompletedTask);
        _batch.Setup(b => b.KeyExpireAsync(It.IsAny<RedisKey>(), It.IsAny<TimeSpan?>(), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>())).ReturnsAsync(true);
        _batch.Setup(b => b.KeyPersistAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>())).ReturnsAsync(true);
        _database.Setup(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>())).ReturnsAsync(true);
    }

    /// <summary>Redis is full (OOM): under the miss policy the store is skipped, and the caller must still get what the factory built.</summary>
    [Fact]
    public async Task GetAsync_WithFactory_WhenRedisIsOutOfMemory_ReturnsTheFactoryValueUnderMissPolicy()
    {
        Missing();
        _batch.Setup(b => b.HashSetAsync(It.IsAny<RedisKey>(), It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisServerException("OOM command not allowed when used memory > 'maxmemory'."));
        var cache = Cache("app:");

        var value = await cache.GetAsync("key", _ => Task.FromResult<Payload?>(new Payload { Field1 = "built" }));

        Assert.Equal("built", value?.Field1);
    }

    /// <summary>Redis refuses writes after a failed snapshot (MISCONF): under the miss policy the store must be skipped with a warning.</summary>
    [Fact]
    public async Task SetAsync_WhenRedisRefusesWritesAfterAFailedSnapshot_IsSkippedUnderMissPolicy()
    {
        _batch.Setup(b => b.HashSetAsync(It.IsAny<RedisKey>(), It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisServerException("MISCONF Redis is configured to save RDB snapshots, but it is currently not able to persist on disk."));
        var cache = Cache("app:");

        await cache.SetAsync("key", new Payload());

        Assert.Contains(LogLevel.Warning, _logs);
    }

    /// <summary>Redis is loading its dataset after a restart (LOADING): under the miss policy that refusal must read as a miss.</summary>
    [Fact]
    public async Task GetAsync_WhenRedisIsLoadingItsDataset_ReportsMissUnderMissPolicy()
    {
        _database.Setup(d => d.HashGetAsync(Key, It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisServerException("LOADING Redis is loading the dataset in memory"));
        var cache = Cache("app:");

        Assert.Null(await cache.GetAsync<Payload>("key"));
    }

    /// <summary>Without an instance name the cache owns no keyspace: a key of another type may be anybody's and must not be deleted on read.</summary>
    [Fact]
    public async Task GetAsync_WrongTypeWithoutInstanceName_DoesNotDeleteTheForeignKey()
    {
        _database.Setup(d => d.HashGetAsync("user:1", It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisServerException("WRONGTYPE Operation against a key holding the wrong kind of value"));
        var cache = Cache(instanceName: null);

        await cache.GetAsync<Payload>("user:1");

        _database.Verify(d => d.KeyDeleteAsync("user:1", It.IsAny<CommandFlags>()), Times.Never);
    }

    /// <summary>Redis drops between the WRONGTYPE read and the eviction: under the miss policy the failed eviction must still read as a miss.</summary>
    [Fact]
    public async Task GetAsync_WrongTypeAndRedisDropsBeforeTheEviction_ReportsMissUnderMissPolicy()
    {
        _database.Setup(d => d.HashGetAsync(Key, It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisServerException("WRONGTYPE Operation against a key holding the wrong kind of value"));
        _database.Setup(d => d.KeyDeleteAsync(Key, It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.SocketFailure, "dropped"));
        var cache = Cache("app:");

        Assert.Null(await cache.GetAsync<Payload>("key"));
    }

    /// <summary>An absolute expiry given with an offset must be stored as the UTC instant, since it is read back as UTC.</summary>
    [Fact]
    public async Task SetAsync_AbsoluteExpirationInAnotherZone_StoresTheUtcInstant()
    {
        HashEntry[]? stored = null;
        _batch.Setup(b => b.HashSetAsync(It.IsAny<RedisKey>(), It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, HashEntry[], CommandFlags>((_, entries, _) => stored = entries)
            .Returns(Task.CompletedTask);
        var expiry = DateTimeOffset.UtcNow.AddMinutes(10).ToOffset(TimeSpan.FromHours(3));
        var cache = Cache("app:");

        await cache.SetAsync("key", new Payload(), new DistributedCacheEntryOptions { AbsoluteExpiration = expiry });

        Assert.Equal(expiry.UtcTicks, (long)stored![0].Value);
    }

    private RedisCache Cache(string? instanceName) =>
        new(
            _connection.Object,
            Options.Create(new RedisOptions { InstanceName = instanceName }),
            Options.Create(new RedisCacheOptions()),
            new Logger<RedisCache>(new LoggerFactory([new ListLoggerProvider(_logs)])));

    private void Missing() =>
        _database.Setup(d => d.HashGetAsync(Key, It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync([RedisValue.Null, RedisValue.Null, RedisValue.Null]);
}
