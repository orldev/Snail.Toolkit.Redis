using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Snail.Toolkit.Redis.Extensions;
using Testcontainers.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>
/// The health check against a one-node Redis cluster. The node announces the loopback address and the published port,
/// so the client reaches what the cluster tells it about wherever Docker runs; a broken cluster is made by leaving
/// slots unassigned or by announcing an address nothing can reach.
/// </summary>
public class RedisClusterHealthTests
{
    private const int SlotCount = 16384;
    private const string Unreachable = "10.255.255.1";

    [IntegrationFact]
    public async Task HealthCheck_EverySlotServedByAReachablePrimary_ReportsHealthy()
    {
        await using var node = await StartNodeAsync();
        await AnnounceAsync(node, await LoopbackAsync(node));
        await AssignSlotsAsync(node, SlotCount - 1);
        await WaitForClusterAsync(node);

        var report = await CheckAsync(node);

        Assert.True(report.Status == HealthStatus.Healthy, Described(report));
    }

    /// <summary>Half the slots have no primary: the cluster refuses every command, and a PING to the one node still answers PONG.</summary>
    [IntegrationFact]
    public async Task HealthCheck_SlotsLeftWithoutAPrimary_ReportsUnhealthy()
    {
        await using var node = await StartNodeAsync();
        await AnnounceAsync(node, await LoopbackAsync(node));
        await AssignSlotsAsync(node, SlotCount / 2 - 1);

        var report = await CheckAsync(node);

        Assert.True(report.Status == HealthStatus.Unhealthy, Described(report));
    }

    /// <summary>The primary announces an address the client cannot reach, as a node behind NAT does: every key fails, and the seed endpoint still answers PING.</summary>
    [IntegrationFact]
    public async Task HealthCheck_PrimaryAnnouncesAnUnreachableAddress_ReportsUnhealthy()
    {
        await using var node = await StartNodeAsync();
        await AnnounceAsync(node, Unreachable);
        await AssignSlotsAsync(node, SlotCount - 1);
        await WaitForClusterAsync(node);

        var report = await CheckAsync(node);

        Assert.True(report.Status == HealthStatus.Unhealthy, Described(report));
    }

    private static async Task<RedisContainer> StartNodeAsync()
    {
        var node = new RedisBuilder("redis:7-alpine")
            .WithCommand("--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf")
            .Build();
        await node.StartAsync();

        return node;
    }

    private static async Task<string> LoopbackAsync(RedisContainer node)
    {
        var addresses = await Dns.GetHostAddressesAsync(node.Hostname);

        return addresses.First(address => address.AddressFamily == AddressFamily.InterNetwork).ToString();
    }

    private static async Task AnnounceAsync(RedisContainer node, string address)
    {
        var port = node.GetMappedPublicPort(6379).ToString(CultureInfo.InvariantCulture);

        await ExecAsync(node, "CONFIG", "SET", "cluster-announce-ip", address);
        await ExecAsync(node, "CONFIG", "SET", "cluster-announce-port", port);
    }

    private static Task AssignSlotsAsync(RedisContainer node, int last) =>
        ExecAsync(node, "CLUSTER", "ADDSLOTSRANGE", "0", last.ToString(CultureInfo.InvariantCulture));

    private static async Task WaitForClusterAsync(RedisContainer node)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);

        while (!(await ExecAsync(node, "CLUSTER", "INFO")).Contains("cluster_state:ok", StringComparison.Ordinal))
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The one-node cluster did not reach cluster_state:ok in time.");

            await Task.Delay(100);
        }
    }

    private static async Task<string> ExecAsync(RedisContainer node, params string[] command)
    {
        var result = await node.ExecAsync(["redis-cli", .. command]);

        if (result.ExitCode != 0 || result.Stdout.StartsWith("ERR", StringComparison.Ordinal))
            throw new InvalidOperationException($"redis-cli {string.Join(' ', command)} failed: {result.Stdout}{result.Stderr}");

        return result.Stdout;
    }

    private static string Described(HealthReport report) =>
        string.Join("; ", report.Entries.Select(entry => $"{entry.Key}: {entry.Value.Status}, {entry.Value.Description} {entry.Value.Exception?.Message}"));

    private static async Task<HealthReport> CheckAsync(RedisContainer node)
    {
        var port = node.GetMappedPublicPort(6379).ToString(CultureInfo.InvariantCulture);
        var configuration = TestConfiguration.Get($"{node.Hostname}:{port},connectTimeout=1000", password: null);
        await using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedisHealthCheck(configuration)
            .BuildServiceProvider();

        return await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();
    }
}
