using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Health;

/// <summary>Reports the shared Redis connection as healthy when a PING succeeds.</summary>
internal sealed class RedisHealthCheck(IConnectionMultiplexer connection) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            if (!connection.IsConnected)
                return new HealthCheckResult(context.Registration.FailureStatus, "Redis connection is not established");

            var latency = await connection.GetDatabase().PingAsync().ConfigureAwait(false);

            return HealthCheckResult.Healthy(
                $"PING {latency.TotalMilliseconds:F1} ms",
                new Dictionary<string, object> { ["latencyMs"] = latency.TotalMilliseconds });
        }
        catch (Exception exception)
        {
            return new HealthCheckResult(context.Registration.FailureStatus, "Redis PING failed", exception);
        }
    }
}
