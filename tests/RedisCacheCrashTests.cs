using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.Cache;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Hostile inputs, poisoned entries and storms against the cache, with Redis replaced by mocks.</summary>
public class RedisCacheCrashTests
{
    public sealed class Payload
    {
        public string? Field1 { get; set; }
    }

    public sealed class Other
    {
        public int Number { get; set; }
    }

    private static readonly RedisKey Key = "app:key";

    private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
    private readonly Mock<IRedisConnection> _connection = new();
    private readonly Mock<IDatabase> _database = new();
    private readonly Mock<IBatch> _batch = new();
    private readonly RedisCacheOptions _options = new();
    private readonly List<LogLevel> _logs = [];
    private readonly RedisCache _cache;

    public RedisCacheCrashTests()
    {
        _multiplexer.Setup(c => c.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(_database.Object);
        _connection.Setup(c => c.Multiplexer).Returns(_multiplexer.Object);
        _database.Setup(d => d.CreateBatch(It.IsAny<object?>())).Returns(_batch.Object);
        _batch.Setup(b => b.HashSetAsync(It.IsAny<RedisKey>(), It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>())).Returns(Task.CompletedTask);
        _batch.Setup(b => b.KeyExpireAsync(It.IsAny<RedisKey>(), It.IsAny<TimeSpan?>(), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>())).ReturnsAsync(true);
        _batch.Setup(b => b.KeyPersistAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>())).ReturnsAsync(true);
        _database.Setup(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>())).ReturnsAsync(true);

        _cache = new RedisCache(
            _connection.Object,
            Options.Create(new RedisOptions { InstanceName = "app:" }),
            Options.Create(_options),
            new Logger<RedisCache>(new LoggerFactory([new ListLoggerProvider(_logs)])));
    }

    /// <summary>A foreign writer stored an absolute expiry outside the DateTimeOffset range; the metadata must be ignored, not thrown.</summary>
    [Fact]
    public async Task GetAsync_AbsoluteExpiryOutOfRange_IgnoresTheMetadata()
    {
        Stored(long.MaxValue, TimeSpan.FromMinutes(2).Ticks, "{}"u8.ToArray());

        var value = await _cache.GetAsync<Payload>("key");

        Assert.NotNull(value);
    }

    /// <summary>Deserialization fails with NotSupportedException for an interface, not with JsonException; that is still an unreadable entry.</summary>
    [Fact]
    public async Task GetAsync_UnsupportedType_IsTreatedAsUnreadable()
    {
        Stored(-1, -1, "{}"u8.ToArray());

        var value = await _cache.GetAsync<IDisposable>("key");

        Assert.Null(value);
        _database.Verify(d => d.KeyDeleteAsync(Key, It.IsAny<CommandFlags>()), Times.Once);
    }

    /// <summary>A sliding window of zero would become PEXPIRE 0 and delete the entry at once; Microsoft's options type already refuses it at assignment.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void DistributedCacheEntryOptions_NonPositiveSlidingExpiration_IsRefusedBeforeItReachesTheCache(int seconds)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(seconds) });
    }

    /// <summary>The same key requested as two types at once: the follower must not cast the leader's result into its own type.</summary>
    [Fact]
    public async Task GetAsync_WithFactory_SameKeyAsTwoTypes_DoesNotShareTheFlight()
    {
        Stored(RedisValue.Null, RedisValue.Null, RedisValue.Null);
        var release = new TaskCompletionSource();

        var leader = _cache.GetAsync("key", async _ =>
        {
            await release.Task;
            return (Payload?)new Payload { Field1 = "payload" };
        });
        var follower = _cache.GetAsync("key", _ => Task.FromResult<Other?>(new Other { Number = 7 }));
        release.SetResult();

        Assert.Equal("payload", (await leader)?.Field1);
        Assert.Equal(7, (await follower)?.Number);
    }

    /// <summary>A thousand concurrent misses over ten keys must produce exactly ten factory calls.</summary>
    [Fact]
    public async Task GetAsync_WithFactory_ThousandParallelMisses_CallEachFactoryOnce()
    {
        _database.Setup(d => d.HashGetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync([RedisValue.Null, RedisValue.Null, RedisValue.Null]);
        var calls = 0;
        var release = new TaskCompletionSource();

        var reads = Enumerable.Range(0, 1000).Select(i => _cache.GetAsync($"key-{i % 10}", async _ =>
        {
            Interlocked.Increment(ref calls);
            await release.Task;
            return (Payload?)new Payload();
        })).ToArray();
        release.SetResult();
        await Task.WhenAll(reads);

        Assert.Equal(10, calls);
    }

    /// <summary>A timeout is an outage too and must degrade to a miss like a refused connection.</summary>
    [Fact]
    public async Task GetAsync_WhenRedisTimesOut_ReportsMiss()
    {
        _database.Setup(d => d.HashGetAsync(Key, It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisTimeoutException("timeout", CommandStatus.Unknown));

        Assert.Null(await _cache.GetAsync<Payload>("key"));
    }

    /// <summary>Ten thousand parallel reads during an outage must log one warning, not ten thousand.</summary>
    [Fact]
    public async Task GetAsync_OutageUnderLoad_WarnsOnce()
    {
        _database.Setup(d => d.HashGetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));

        await Task.WhenAll(Enumerable.Range(0, 10_000).Select(i => _cache.GetAsync<Payload>($"key-{i}")));

        Assert.Single(_logs, level => level == LogLevel.Warning);
    }

    /// <summary>The connection itself may refuse to open (abortConnect with Redis down); under the miss policy that is an outage, not an exception.</summary>
    [Fact]
    public async Task GetAsync_WhenTheConnectionCannotOpen_ReportsMiss()
    {
        _connection.Setup(c => c.Multiplexer).Throws(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "refused"));

        Assert.Null(await _cache.GetAsync<Payload>("key"));
        Assert.Contains(LogLevel.Warning, _logs);
    }

    /// <summary>Keys are binary safe in Redis; the prefix must simply concatenate.</summary>
    [Theory]
    [InlineData("🐌 snail")]
    [InlineData("with spaces and\ttabs")]
    [InlineData("colons:and:*:globs")]
    public async Task GetAsync_HostileKey_IsPrefixedVerbatim(string key)
    {
        RedisKey? requested = null;
        _database.Setup(d => d.HashGetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue[], CommandFlags>((k, _, _) => requested = k)
            .ReturnsAsync([RedisValue.Null, RedisValue.Null, RedisValue.Null]);

        await _cache.GetAsync<Payload>(key);

        Assert.Equal("app:" + key, (string?)requested);
    }

    private void Stored(RedisValue absolute, RedisValue sliding, RedisValue data) =>
        _database.Setup(d => d.HashGetAsync(Key, It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync([absolute, sliding, data]);
}
