using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>What asking the shared connection for its multiplexer costs once one is held: every operation starts there.</summary>
/// <remarks>
/// The multiplexer is a real one that has nothing to talk to: with AbortOnConnectFail off a connect to a closed port
/// returns a multiplexer that keeps reconnecting in the background, which is all the connection holds on to.
/// </remarks>
[MemoryDiagnoser]
public class ConnectBenchmarks
{
    private RedisConnection _connection = null!;

    /// <summary>A connection that already holds its multiplexer.</summary>
    [GlobalSetup]
    public async Task Setup()
    {
        var options = new RedisOptions { Connection = "127.0.0.1:1,connectTimeout=200" };

        _connection = new RedisConnection(Options.Create(options), new OptionsFactory<RedisOptions>([], []), [], NullLogger<RedisConnection>.Instance);
        await _connection.ConnectAsync(CancellationToken.None);
    }

    /// <summary>Closes the multiplexer.</summary>
    [GlobalCleanup]
    public void Cleanup() => _connection.Dispose();

    /// <summary>The asynchronous way in, as the warm-up takes it.</summary>
    [Benchmark(Description = "ConnectAsync, connected")]
    public Task<IConnectionMultiplexer> ConnectAsync() => _connection.ConnectAsync(CancellationToken.None);

    /// <summary>The synchronous way in, as the cache, the streams, pub/sub and the health check take it.</summary>
    [Benchmark(Description = "Multiplexer, connected")]
    public IConnectionMultiplexer Multiplexer() => _connection.Multiplexer;
}
