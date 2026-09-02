using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis;

/// <summary>Opens the shared Redis connection at host start so the first request never pays for the connect.</summary>
internal sealed class RedisConnectionWarmup(IConnectionMultiplexer connection, ILogger<RedisConnectionWarmup> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (connection.IsConnected)
            logger.LogInformation("Redis connection established: {Configuration}", connection.Configuration);
        else
            logger.LogWarning("Redis is not reachable at start: {Configuration}; reconnecting in the background", connection.Configuration);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
