using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Snail.Toolkit.Redis.Extensions;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Uses its own container: the password is changed on the server during the test.</summary>
public class RedisRotationTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    [IntegrationFact]
    public async Task PasswordRotation_AppliesToReconnects()
    {
        var connectionString = await fixture.GetConnectionStringAsync();
        await using var admin = await ConnectionMultiplexer.ConnectAsync(connectionString + ",allowAdmin=true");
        var server = admin.GetServer(admin.GetEndPoints()[0]);
        await server.ExecuteAsync("CONFIG", "SET", "requirepass", "first");

        var (configuration, source) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = connectionString,
            [$"{RedisOptions.SectionName}:Password"] = "first"
        });
        await using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedisHealthCheck(configuration)
            .BuildServiceProvider();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var clientBefore = await connection.GetDatabase().ExecuteAsync("CLIENT", "ID");
        Assert.True(connection.IsConnected);

        await server.ExecuteAsync("CONFIG", "SET", "requirepass", "second");
        source.Set($"{RedisOptions.SectionName}:Password", "second");
        await server.ExecuteAsync("CLIENT", "KILL", "TYPE", "normal");

        var deadline = DateTime.UtcNow.AddSeconds(15);
        long clientAfter = 0;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                clientAfter = (long)await connection.GetDatabase().ExecuteAsync("CLIENT", "ID");
                break;
            }
            catch (RedisException)
            {
                await Task.Delay(200);
            }
        }

        Assert.NotEqual(0, clientAfter);
        Assert.NotEqual((long)clientBefore, clientAfter);
        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();
        Assert.Equal(HealthStatus.Healthy, report.Status);
    }
}
