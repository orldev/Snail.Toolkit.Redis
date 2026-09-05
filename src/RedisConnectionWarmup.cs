using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Snail.Toolkit.Redis;

/// <summary>Opens the shared Redis connection at host start so the first request never pays for the connect.</summary>
internal sealed class RedisConnectionWarmup(RedisConnection connection, ILogger<RedisConnectionWarmup> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var multiplexer = await connection.ConnectAsync(cancellationToken).ConfigureAwait(false);

        if (multiplexer.IsConnected)
            logger.Connected(multiplexer.Configuration);
        else
            logger.NotReachableAtStart(multiplexer.Configuration);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
