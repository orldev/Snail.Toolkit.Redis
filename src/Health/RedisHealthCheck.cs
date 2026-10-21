using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Health;

/// <summary>Reports the shared Redis connection as healthy when it can serve every key: a PING to a single server, or to every primary of a cluster.</summary>
/// <remarks>
/// A PING without a key reaches any connected node, so against a cluster it answered PONG while a primary the client
/// could not reach, or slots no primary served, failed every key that hashed there (measured 2026-10-21 on a two-node
/// cluster whose second node announced an unreachable address: half the keys failed and the check stayed healthy).
/// A cluster is therefore healthy only when each of its hash slots belongs to a primary that answers the PING. The map
/// is the one the client routes keys by, which StackExchange.Redis refreshes on a MOVED and on its periodic
/// configuration check: the check judges what the cache will meet, and asking the cluster itself would need admin
/// mode on a multiplexer every consumer shares.
/// </remarks>
internal sealed class RedisHealthCheck(IRedisConnection connection) : IHealthCheck
{
    private const int SlotCount = 16384;

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var multiplexer = await connection.ConnectAsync(cancellationToken).ConfigureAwait(false);

            if (!multiplexer.IsConnected)
                return new HealthCheckResult(context.Registration.FailureStatus, "Redis connection is not established");

            if (ClusterSeed(multiplexer) is { } seed)
                return await CheckClusterAsync(context, multiplexer, seed).ConfigureAwait(false);

            var latency = await multiplexer.GetDatabase().PingAsync().ConfigureAwait(false);

            return Healthy($"PING {latency.TotalMilliseconds:F1} ms", latency);
        }
        catch (Exception exception)
        {
            return new HealthCheckResult(context.Registration.FailureStatus, "Redis PING failed", exception);
        }
    }

    /// <summary>A connected node of a cluster to ask for the map, or null when the connection is not to a cluster.</summary>
    private static IServer? ClusterSeed(IConnectionMultiplexer multiplexer) =>
        Array.Find(multiplexer.GetServers(), server => server.IsConnected && server.ServerType is ServerType.Cluster);

    private static async Task<HealthCheckResult> CheckClusterAsync(HealthCheckContext context, IConnectionMultiplexer multiplexer, IServer seed)
    {
        var primaries = seed.ClusterConfiguration?.Nodes.Where(node => !node.IsReplica && node.Slots.Count > 0).ToArray() ?? [];
        var servers = multiplexer.GetServers();

        var answers = await Task.WhenAll(primaries.Select(primary => PingAsync(primary, servers))).ConfigureAwait(false);

        var served = answers.Where(answer => answer.Latency is not null).Sum(answer => answer.Slots);
        var unreachable = answers.Where(answer => answer.Latency is null).Select(answer => answer.EndPoint).ToArray();

        if (served < SlotCount)
            return Unserved(context, SlotCount - served, unreachable);

        var slowest = answers.Max(answer => answer.Latency!.Value);

        return Healthy($"PING {slowest.TotalMilliseconds:F1} ms, slowest of {answers.Length} cluster primaries", slowest);
    }

    /// <summary>The primary's answer to a PING through the client's own connection to it, or no latency when it has none or it does not answer.</summary>
    private static async Task<PrimaryAnswer> PingAsync(ClusterNode primary, IServer[] servers)
    {
        var slots = primary.Slots.Sum(range => range.To - range.From + 1);
        var endPoint = primary.EndPoint?.ToString() ?? primary.NodeId;
        var server = Array.Find(servers, candidate => Equals(candidate.EndPoint, primary.EndPoint));

        if (server is not { IsConnected: true })
            return new PrimaryAnswer(endPoint, slots, null);

        try
        {
            var latency = await server.PingAsync().ConfigureAwait(false);

            return new PrimaryAnswer(endPoint, slots, latency);
        }
        catch (Exception exception) when (exception is RedisException or RedisTimeoutException)
        {
            return new PrimaryAnswer(endPoint, slots, null);
        }
    }

    private static HealthCheckResult Healthy(string description, TimeSpan latency) =>
        HealthCheckResult.Healthy(description, new Dictionary<string, object> { ["latencyMs"] = latency.TotalMilliseconds });

    private static HealthCheckResult Unserved(HealthCheckContext context, int slots, string[] unreachable)
    {
        var data = new Dictionary<string, object> { ["unservedSlots"] = slots, ["unreachablePrimaries"] = unreachable };
        var cause = unreachable.Length > 0 ? $"unreachable primaries: {string.Join(", ", unreachable)}" : "no primary owns them";

        return new HealthCheckResult(context.Registration.FailureStatus, $"{slots} of {SlotCount} hash slots are not served; {cause}", data: data);
    }

    /// <summary>A cluster primary, the number of slots it owns and how long its PING took, or no latency when it did not answer.</summary>
    private sealed record PrimaryAnswer(string EndPoint, int Slots, TimeSpan? Latency);
}
