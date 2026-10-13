using System.Reflection;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Snail.Toolkit.Redis.PubSub;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>What a received message costs a subscriber: read once by the feed, handed to the reader, taken out by it.</summary>
/// <remarks>
/// StackExchange.Redis hands a feed every message of its channel; the message is built here once, as the multiplexer
/// would build it from the bytes on the wire, and given to the feed the way the multiplexer does.
/// </remarks>
[MemoryDiagnoser]
public class ReceiveBenchmarks
{
    private Action<ChannelMessage> _receive = null!;
    private ChannelMessage _message;
    private IAsyncEnumerator<RedisMessage<Order>> _reader = null!;

    /// <summary>A feed of a literal channel with one reader attached, and an order as it arrives from Redis.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var channel = RedisChannel.Literal("orders:created");
        var options = new RedisPubSubOptions();
        var payload = JsonSerializer.SerializeToUtf8Bytes(Order.Sample(), options.Json);
        var feed = new ChannelFeed<Order>(channel, new InProcessRedis([]).Multiplexer, options, NullLogger.Instance, _ => { });
        var subscription = new RedisSubscription<Order>(feed, options, NullLogger.Instance);

        feed.TryAttach(subscription);

        _receive = typeof(ChannelFeed<Order>)
            .GetMethod("OnMessage", BindingFlags.Instance | BindingFlags.NonPublic)!
            .CreateDelegate<Action<ChannelMessage>>(feed);
        _message = (ChannelMessage)Activator.CreateInstance(
            typeof(ChannelMessage),
            BindingFlags.Instance | BindingFlags.NonPublic,
            null,
            [null, channel, (RedisValue)payload],
            null)!;
        _reader = subscription.ReadAllAsync().GetAsyncEnumerator();
    }

    /// <summary>One order received by the feed and read by its reader.</summary>
    [Benchmark(Description = "A message received and read")]
    public async Task<int> Receive()
    {
        _receive(_message);
        await _reader.MoveNextAsync();

        return _reader.Current.Message.Quantity;
    }
}
