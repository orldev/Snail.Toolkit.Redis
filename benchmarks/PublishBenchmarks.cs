using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.PubSub;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>What a publish costs before the network: the message serialized, the channel named, the subscriber found.</summary>
[MemoryDiagnoser]
public class PublishBenchmarks
{
    private readonly Order _order = Order.Sample();

    private RedisPubSub _pubSub = null!;

    /// <summary>Pub/sub over an in-process Redis that takes every message.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var redis = new InProcessRedis([]);

        _pubSub = new RedisPubSub(redis, Options.Create(new RedisPubSubOptions()), NullLogger<RedisPubSub>.Instance);
    }

    /// <summary>One order published to a literal channel.</summary>
    [Benchmark(Description = "PublishAsync(channel, message)")]
    public Task<long> Publish() => _pubSub.PublishAsync("orders:created", _order);
}
