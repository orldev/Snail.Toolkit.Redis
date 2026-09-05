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
