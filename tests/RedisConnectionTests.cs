using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.Redis.Extensions;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

public class RedisConnectionTests
{
    private const string Unreachable = "localhost:1,connectTimeout=200";

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
}
