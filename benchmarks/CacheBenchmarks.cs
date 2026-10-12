using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.Cache;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>The cache's own work per call: the key, the fields read, the sliding refresh, the JSON both ways.</summary>
/// <remarks>
/// A hit is the path every request takes; the hit through the factory overload is the one a caller usually writes,
/// and it pays for the single-flight bookkeeping only on a miss. A store builds the entry and sends it in one batch.
/// With an instance name every key is prefixed, which costs a string per call.
/// </remarks>
[MemoryDiagnoser]
public class CacheBenchmarks
{
    private const string Key = "order:6f1c2a43-9a51-4d8f-b0f1-3c5e7d2a9b10";

    private readonly Func<CancellationToken, Task<Order?>> _factory = _ => Task.FromResult<Order?>(Order.Sample());

    private readonly Order _order = Order.Sample();

    private RedisCache _cache = null!;

    /// <summary>The prefix of every key, or none.</summary>
    [Params(null, "shop:")]
    public string? InstanceName { get; set; }

    /// <summary>A cache over an in-process Redis holding one entry that expires in an hour and slides by two minutes.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var options = new RedisCacheOptions();
        var payload = JsonSerializer.SerializeToUtf8Bytes(_order, options.Json);
        RedisValue[] entry = [DateTimeOffset.UtcNow.AddHours(1).Ticks, TimeSpan.FromMinutes(2).Ticks, payload];
        var redis = new InProcessRedis(entry);
        var settings = new RedisOptions { Connection = "localhost", InstanceName = InstanceName };

        _cache = new RedisCache(redis, Options.Create(settings), Options.Create(options), NullLogger<RedisCache>.Instance);
    }

    /// <summary>A hit, refreshed and deserialized.</summary>
    [Benchmark(Baseline = true, Description = "GetAsync(key), a hit")]
    public async Task<int> Hit() => (await _cache.GetAsync<Order>(Key))!.Quantity;

    /// <summary>A hit through the overload that would call the factory on a miss.</summary>
    [Benchmark(Description = "GetAsync(key, factory), a hit")]
    public async Task<int> FactoryHit() => (await _cache.GetAsync(Key, _factory))!.Quantity;

    /// <summary>An entry serialized and stored with the default expiry.</summary>
    [Benchmark(Description = "SetAsync(key, value)")]
    public Task Store() => _cache.SetAsync(Key, _order);
}
