using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.PubSub;
using Snail.Toolkit.Redis.Streams;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>The streams' own work: an append with its announcement, and a page of entries read and handed out.</summary>
/// <remarks>
/// The page is what a late reader catches up through, entry by entry: the id kept as the position and handed out,
/// the data field found, the JSON read. The announcement of an append goes through the real pub/sub; a reader's
/// subscription to the announcements is a quiet one, since the page is read before it is ever asked for a message.
/// </remarks>
[MemoryDiagnoser]
public class StreamBenchmarks
{
    /// <summary>The entries of the page a reader takes in one round trip.</summary>
    public const int PageSize = 10;

    private const string Stream = "orders";

    private readonly Order _order = Order.Sample();

    private RedisStreams _streams = null!;

    /// <summary>Streams over an in-process Redis whose stream holds one page of orders.</summary>
    [GlobalSetup]
    public void Setup() => _streams = Create(new InProcessRedis([], Page()));

    /// <summary>The streams of the benchmark over the given Redis, announcing through the real pub/sub.</summary>
    internal static RedisStreams Create(InProcessRedis redis)
    {
        var pubSub = new RedisPubSub(redis, Options.Create(new RedisPubSubOptions()), NullLogger<RedisPubSub>.Instance);
        var options = new RedisStreamOptions { PageSize = PageSize };

        return new RedisStreams(redis, new QuietPubSub(pubSub), Options.Create(options), NullLogger<RedisStreams>.Instance);
    }

    /// <summary>A page of orders as a server hands it over: ids, field names and values as raw bytes.</summary>
    internal static StreamEntry[] Page()
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(Order.Sample(), new RedisStreamOptions().Json);
        var entries = new StreamEntry[PageSize];

        for (var index = 0; index < entries.Length; index++)
        {
            var id = Encoding.UTF8.GetBytes(string.Create(CultureInfo.InvariantCulture, $"1760258400000-{index}"));
            NameValueEntry[] values = [new(Encoding.UTF8.GetBytes("data"), payload)];

            entries[index] = new StreamEntry(id, values);
        }

        return entries;
    }

    /// <summary>One order appended and announced.</summary>
    [Benchmark(Description = "AppendAsync(stream, message)")]
    public Task<string> Append() => _streams.AppendAsync(Stream, _order);

    /// <summary>A reader that catches up through one page of entries from the start.</summary>
    [Benchmark(Description = "ReadAsync, a page of 10 entries")]
    public async Task<int> ReadPage()
    {
        var quantity = 0;
        var read = 0;

        await foreach (var entry in _streams.ReadAsync<Order>(Stream))
        {
            quantity += entry.Message.Quantity;

            if (++read == PageSize)
                break;
        }

        return quantity;
    }

    /// <summary>Publishes through the real pub/sub; subscribes to a channel that never says anything.</summary>
    private sealed class QuietPubSub(IRedisPubSub pubSub) : IRedisPubSub
    {
        public Task<long> PublishAsync<T>(string channel, T message, CancellationToken cancellationToken = default) =>
            pubSub.PublishAsync(channel, message, cancellationToken);

        public Task<IRedisSubscription<T>> SubscribeAsync<T>(string channel, CancellationToken cancellationToken = default) =>
            Task.FromResult<IRedisSubscription<T>>(new QuietSubscription<T>(channel));

        public Task<IRedisSubscription<T>> SubscribePatternAsync<T>(string pattern, CancellationToken cancellationToken = default) =>
            Task.FromResult<IRedisSubscription<T>>(new QuietSubscription<T>(pattern));

        public IAsyncEnumerable<T> StreamAsync<T>(string channel, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("The benchmark only publishes and subscribes.");
    }

    /// <summary>A subscription on which nothing ever arrives.</summary>
    private sealed class QuietSubscription<T>(string channel) : IRedisSubscription<T>
    {
        public string Channel { get; } = channel;

        public async IAsyncEnumerable<RedisMessage<T>> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);

            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
