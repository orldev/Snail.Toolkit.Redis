using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.Extensions;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

public class RedisConnectionTests
{
    private const string Unreachable = "localhost:1,connectTimeout=200";
    private const string FirstClosedPort = "127.0.0.1:1,connectTimeout=300,connectRetry=1";
    private const string SecondClosedPort = "127.0.0.1:2,connectTimeout=300,connectRetry=1";

    [Fact]
    public void Reload_WithEmptyConnection_KeepsPreviousSettingsAndLogsError()
    {
        var (configuration, source) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = Unreachable,
            [$"{RedisOptions.SectionName}:Password"] = "first"
        });
        var logs = new List<LogLevel>();
        using var provider = new ServiceCollection()
            .AddLogging(builder => builder.AddProvider(new ListLoggerProvider(logs)))
            .AddRedis(configuration)
            .BuildServiceProvider();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();

        source.Set($"{RedisOptions.SectionName}:Connection", "");

        Assert.Contains(LogLevel.Error, logs);
        Assert.Equal("first", ConfigurationOptions.Parse(connection.Configuration).Password);
    }

    /// <summary>With abortConnect a refused connect must not be repeated by every caller for the whole connect timeout.</summary>
    [Fact]
    public void Multiplexer_AfterARefusedConnect_DoesNotRetryWithinTheBackoff()
    {
        var (configuration, _) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = "localhost:1,connectTimeout=300",
            [$"{RedisOptions.SectionName}:AbortOnConnectFail"] = "true"
        });
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedis(configuration)
            .BuildServiceProvider();
        var connection = provider.GetRequiredService<IRedisConnection>();
        Assert.Throws<RedisConnectionException>(() => connection.Multiplexer);

        var stopwatch = Stopwatch.StartNew();
        var refused = Assert.Throws<RedisConnectionException>(() => connection.Multiplexer);

        Assert.True(stopwatch.ElapsedMilliseconds < 200, $"The second attempt took {stopwatch.ElapsedMilliseconds} ms.");
        Assert.NotNull(refused.InnerException);
    }

    /// <summary>The cache, streams, pub/sub and health check come through ConnectAsync, which must keep the back-off as well.</summary>
    [Fact]
    public async Task ConnectAsync_AfterARefusedConnect_DoesNotRetryWithinTheBackoff()
    {
        var (configuration, _) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = "localhost:1,connectTimeout=300",
            [$"{RedisOptions.SectionName}:AbortOnConnectFail"] = "true"
        });
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedis(configuration)
            .BuildServiceProvider();
        var connection = provider.GetRequiredService<IRedisConnection>();
        await Assert.ThrowsAsync<RedisConnectionException>(() => connection.ConnectAsync(CancellationToken.None));

        var stopwatch = Stopwatch.StartNew();
        var refused = await Assert.ThrowsAsync<RedisConnectionException>(() => connection.ConnectAsync(CancellationToken.None));

        Assert.True(stopwatch.ElapsedMilliseconds < 200, $"The second attempt took {stopwatch.ElapsedMilliseconds} ms.");
        Assert.NotNull(refused.InnerException);
    }

    /// <summary>Every cache read asks for the connection: once it is open the answer must be a task already made.</summary>
    [Fact]
    public async Task ConnectAsync_OnceOpen_AnswersTheSameCompletedTask()
    {
        var (configuration, _) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = Unreachable
        });
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedis(configuration)
            .BuildServiceProvider();
        var connection = provider.GetRequiredService<IRedisConnection>();
        var multiplexer = await connection.ConnectAsync(CancellationToken.None);

        var first = connection.ConnectAsync(CancellationToken.None);
        var second = connection.ConnectAsync(CancellationToken.None);

        Assert.True(first.IsCompletedSuccessfully);
        Assert.Same(first, second);
        Assert.Same(multiplexer, connection.Multiplexer);
    }

    [Fact]
    public void Reload_WithNewPassword_AppliesItToTheConfiguration()
    {
        var (configuration, source) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = Unreachable,
            [$"{RedisOptions.SectionName}:Password"] = "first"
        });
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedis(configuration)
            .BuildServiceProvider();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();

        source.Set($"{RedisOptions.SectionName}:Password", "second");

        Assert.Equal("second", ConfigurationOptions.Parse(connection.Configuration).Password);
    }

    /// <summary>A file provider raises the reload twice for one save; the second must find the endpoints already moved and open nothing.</summary>
    [Fact]
    public async Task Reload_RaisedTwiceForOneEndpointChange_ReplacesTheConnectionOnce()
    {
        var (configuration, source) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = FirstClosedPort
        });
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedis(configuration)
            .BuildServiceProvider();
        var connection = provider.GetRequiredService<IRedisConnection>();
        await connection.ConnectAsync(CancellationToken.None);
        var replacements = 0;
        connection.Replaced += _ => Interlocked.Increment(ref replacements);

        source.Set($"{RedisOptions.SectionName}:Connection", SecondClosedPort);
        source.Set($"{RedisOptions.SectionName}:Connection", SecondClosedPort);
        await Task.Delay(TimeSpan.FromSeconds(3));

        Assert.Equal(1, replacements);
    }

    /// <summary>The same endpoints listed in another order are the same servers and must not open a second connection.</summary>
    [Fact]
    public async Task Reload_SameEndpointsInAnotherOrder_KeepsTheConnection()
    {
        var (configuration, source) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = "127.0.0.1:1,127.0.0.1:2,connectTimeout=300,connectRetry=1"
        });
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedis(configuration)
            .BuildServiceProvider();
        var connection = provider.GetRequiredService<IRedisConnection>();
        await connection.ConnectAsync(CancellationToken.None);
        var replacements = 0;
        connection.Replaced += _ => Interlocked.Increment(ref replacements);

        source.Set($"{RedisOptions.SectionName}:Connection", "127.0.0.1:2,127.0.0.1:1,connectTimeout=300,connectRetry=1");
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.Equal(0, replacements);
    }

    /// <summary>The host stops while the first connect is in flight: what the connect opens after Dispose must be closed, not kept.</summary>
    [Fact]
    public async Task Dispose_WhileTheFirstConnectIsInFlight_KeepsNoMultiplexer()
    {
        var settings = new RedisOptions { Connection = FirstClosedPort };
        var connection = new RedisConnection(Options.Create(settings), new OptionsFactory<RedisOptions>([], []), [], NullLogger<RedisConnection>.Instance);

        var connecting = connection.ConnectAsync(CancellationToken.None);
        connection.Dispose();

        await Assert.ThrowsAnyAsync<ObjectDisposedException>(() => connecting);
        Assert.Null(connection.Opened);
    }
}
