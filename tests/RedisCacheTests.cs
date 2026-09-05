using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.Cache;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

public class RedisCacheTests
{
    public sealed class Payload
    {
        public string? Field1 { get; set; }
        public int Field2 { get; set; }
    }

    private static readonly RedisKey Key = "app:key";

    private readonly Mock<IDatabase> _database = new();
    private readonly Mock<IBatch> _batch = new();
    private readonly RedisCacheOptions _options = new();
    private readonly List<LogLevel> _logs = [];
    private readonly RedisCache _cache;

    public RedisCacheTests()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(c => c.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(_database.Object);
        var connection = new Mock<IRedisConnection>();
        connection.Setup(c => c.Multiplexer).Returns(multiplexer.Object);
        _database.Setup(d => d.CreateBatch(It.IsAny<object?>())).Returns(_batch.Object);
        _batch.Setup(b => b.HashSetAsync(Key, It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>())).Returns(Task.CompletedTask);
        _batch.Setup(b => b.KeyExpireAsync(Key, It.IsAny<TimeSpan?>(), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>())).ReturnsAsync(true);
        _batch.Setup(b => b.KeyPersistAsync(Key, It.IsAny<CommandFlags>())).ReturnsAsync(true);
        _database.Setup(d => d.KeyDeleteAsync(Key, It.IsAny<CommandFlags>())).ReturnsAsync(true);

        _cache = new RedisCache(
            connection.Object,
            Options.Create(new RedisOptions { InstanceName = "app:" }),
            Options.Create(_options),
            new Logger<RedisCache>(new LoggerFactory([new ListLoggerProvider(_logs)])));
    }

    [Fact]
    public async Task GetAsync_MissingKey_ReturnsNull()
    {
        Stored(RedisValue.Null, RedisValue.Null, RedisValue.Null);

        var value = await _cache.GetAsync<Payload>("key");

        Assert.Null(value);
    }

    [Fact]
    public async Task GetAsync_StoredJson_DeserializesCaseInsensitively()
    {
        Stored(-1, -1, "{\"Field1\":\"a\",\"field2\":2}"u8.ToArray());

        var value = await _cache.GetAsync<Payload>("key");

        Assert.NotNull(value);
        Assert.Equal("a", value.Field1);
        Assert.Equal(2, value.Field2);
    }

    [Fact]
    public async Task GetAsync_HitsAndMisses_AreCounted()
    {
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

        Stored(-1, -1, "{}"u8.ToArray());
        await _cache.GetAsync<Payload>("key");
        Stored(RedisValue.Null, RedisValue.Null, RedisValue.Null);
        await _cache.GetAsync<Payload>("key");

        Assert.True(Interlocked.Read(ref hits) >= 1);
        Assert.True(Interlocked.Read(ref misses) >= 1);
    }

    [Fact]
    public async Task GetAsync_SlidingEntry_RefreshesTheExpiry()
    {
        Stored(-1, TimeSpan.FromMinutes(2).Ticks, "{}"u8.ToArray());

        await _cache.GetAsync<Payload>("key");

        _database.Verify(d => d.KeyExpire(Key, TimeSpan.FromMinutes(2), CommandFlags.FireAndForget), Times.Once);
    }

    [Fact]
    public async Task GetAsync_SlidingEntryNearItsAbsoluteEnd_RefreshesOnlyUpToTheEnd()
    {
        Stored(DateTimeOffset.UtcNow.AddSeconds(10).Ticks, TimeSpan.FromMinutes(2).Ticks, "{}"u8.ToArray());

        await _cache.GetAsync<Payload>("key");

        _database.Verify(d => d.KeyExpire(Key, It.Is<TimeSpan?>(t => t <= TimeSpan.FromSeconds(10)), CommandFlags.FireAndForget), Times.Once);
    }

    [Fact]
    public async Task GetAsync_UnreadableEntry_EvictsItAndReturnsNull()
    {
        Stored(-1, -1, "{\"field2\":\"not a number\""u8.ToArray());

        var value = await _cache.GetAsync<Payload>("key");

        Assert.Null(value);
        _database.Verify(d => d.KeyDeleteAsync(Key, It.IsAny<CommandFlags>()), Times.Once);
        Assert.Contains(LogLevel.Warning, _logs);
    }

    [Fact]
    public async Task GetAsync_WhenRedisIsUnreachable_ReportsMissAndWarnsOnce()
    {
        _database.Setup(d => d.HashGetAsync(Key, It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));

        var first = await _cache.GetAsync<Payload>("key");
        var second = await _cache.GetAsync<Payload>("key");

        Assert.Null(first);
        Assert.Null(second);
        Assert.Single(_logs, level => level == LogLevel.Warning);
    }

    [Fact]
    public async Task GetAsync_WhenRedisIsUnreachableAndPolicyThrows_Throws()
    {
        _options.OnFailure = CacheFailure.Throw;
        _database.Setup(d => d.HashGetAsync(Key, It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));

        await Assert.ThrowsAsync<RedisConnectionException>(() => _cache.GetAsync<Payload>("key"));
    }

    [Fact]
    public async Task GetAsync_WithFactory_OnMissStoresFactoryValueAsCamelCase()
    {
        Stored(RedisValue.Null, RedisValue.Null, RedisValue.Null);
        HashEntry[]? stored = null;
        _batch.Setup(b => b.HashSetAsync(Key, It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, HashEntry[], CommandFlags>((_, entries, _) => stored = entries)
            .Returns(Task.CompletedTask);

        var value = await _cache.GetAsync("key", _ => Task.FromResult<Payload?>(new Payload { Field1 = "b", Field2 = 3 }));

        Assert.Equal("b", value?.Field1);
        Assert.NotNull(stored);
        Assert.Equal("{\"field1\":\"b\",\"field2\":3}", Encoding.UTF8.GetString((byte[])stored.Single(e => e.Name == "data").Value!));
        Assert.Equal(TimeSpan.FromMinutes(2).Ticks, (long)stored.Single(e => e.Name == "sldexp").Value);
        Assert.True((long)stored.Single(e => e.Name == "absexp").Value > DateTimeOffset.UtcNow.Ticks);
    }

    [Fact]
    public async Task GetAsync_WithFactory_ConcurrentMissesShareOneFactoryCall()
    {
        Stored(RedisValue.Null, RedisValue.Null, RedisValue.Null);
        var calls = 0;
        var release = new TaskCompletionSource();

        var first = _cache.GetAsync("key", async _ =>
        {
            Interlocked.Increment(ref calls);
            await release.Task;
            return (Payload?)new Payload { Field1 = "one" };
        });
        var second = _cache.GetAsync("key", _ =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult<Payload?>(new Payload { Field1 = "two" });
        });
        release.SetResult();

        Assert.Equal("one", (await first)?.Field1);
        Assert.Equal("one", (await second)?.Field1);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task GetAsync_WithFactory_FollowerOfACancelledLeaderRunsItsOwnFactory()
    {
        Stored(RedisValue.Null, RedisValue.Null, RedisValue.Null);
        using var leaderCancellation = new CancellationTokenSource();
        var leaderStarted = new TaskCompletionSource();

        var leader = _cache.GetAsync("key", async token =>
        {
            leaderStarted.SetResult();
            await Task.Delay(Timeout.Infinite, token);
            return (Payload?)null;
        }, cancellationToken: leaderCancellation.Token);
        await leaderStarted.Task;
        var follower = _cache.GetAsync("key", _ => Task.FromResult<Payload?>(new Payload { Field1 = "own" }));
        leaderCancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leader);
        Assert.Equal("own", (await follower)?.Field1);
    }

    [Fact]
    public async Task GetAsync_WithFactory_PassesCancellationTokenToFactory()
    {
        Stored(RedisValue.Null, RedisValue.Null, RedisValue.Null);
        using var cts = new CancellationTokenSource();
        CancellationToken observed = default;

        await _cache.GetAsync("key", token =>
        {
            observed = token;
            return Task.FromResult<Payload?>(new Payload());
        }, cancellationToken: cts.Token);

        Assert.Equal(cts.Token, observed);
    }

    [Fact]
    public async Task GetAsync_WithFactory_OnHitSkipsFactory()
    {
        Stored(-1, -1, "{\"field1\":\"cached\"}"u8.ToArray());
        var factoryCalled = false;

        var value = await _cache.GetAsync("key", _ =>
        {
            factoryCalled = true;
            return Task.FromResult<Payload?>(new Payload());
        });

        Assert.Equal("cached", value?.Field1);
        Assert.False(factoryCalled);
        _batch.Verify(b => b.HashSetAsync(It.IsAny<RedisKey>(), It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task SetAsync_WithoutOptions_AppliesConfiguredDefaults()
    {
        _options.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(30);
        _options.SlidingExpiration = TimeSpan.FromMinutes(5);

        await _cache.SetAsync("key", new Payload());

        _batch.Verify(b => b.KeyExpireAsync(Key, TimeSpan.FromMinutes(5), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task SetAsync_WithOptions_UsesTheShorterOfSlidingAndAbsolute()
    {
        var explicitOptions = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30),
            SlidingExpiration = TimeSpan.FromMinutes(5)
        };

        await _cache.SetAsync("key", new Payload(), explicitOptions);

        _batch.Verify(b => b.KeyExpireAsync(Key, It.Is<TimeSpan?>(t => t <= TimeSpan.FromSeconds(30)), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task SetAsync_WithoutAnyExpiry_PersistsTheKey()
    {
        await _cache.SetAsync("key", new Payload(), new DistributedCacheEntryOptions());

        _batch.Verify(b => b.KeyPersistAsync(Key, It.IsAny<CommandFlags>()), Times.Once);
        _batch.Verify(b => b.KeyExpireAsync(It.IsAny<RedisKey>(), It.IsAny<TimeSpan?>(), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task SetAsync_PastAbsoluteExpiration_Throws()
    {
        var explicitOptions = new DistributedCacheEntryOptions { AbsoluteExpiration = DateTimeOffset.UtcNow.AddMinutes(-1) };

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _cache.SetAsync("key", new Payload(), explicitOptions));
    }

    [Fact]
    public async Task SetAsync_NullValue_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _cache.SetAsync<Payload>("key", null!));
    }

    [Fact]
    public async Task SetAsync_WhenRedisIsUnreachable_IsSkippedWithAWarning()
    {
        _batch.Setup(b => b.HashSetAsync(Key, It.IsAny<HashEntry[]>(), It.IsAny<CommandFlags>()))
            .Returns(Task.FromException(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down")));

        await _cache.SetAsync("key", new Payload());

        Assert.Contains(LogLevel.Warning, _logs);
    }

    [Fact]
    public async Task RemoveAsync_DeletesThePrefixedKey()
    {
        await _cache.RemoveAsync("key");

        _database.Verify(d => d.KeyDeleteAsync(Key, It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task RemoveAsync_WhenRedisIsUnreachable_ThrowsWhateverThePolicySays()
    {
        _database.Setup(d => d.KeyDeleteAsync(Key, It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));

        await Assert.ThrowsAsync<RedisConnectionException>(() => _cache.RemoveAsync("key"));
    }

    private void Stored(RedisValue absolute, RedisValue sliding, RedisValue data) =>
        _database.Setup(d => d.HashGetAsync(Key, It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync([absolute, sliding, data]);
}
