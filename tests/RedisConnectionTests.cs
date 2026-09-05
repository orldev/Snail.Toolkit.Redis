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
