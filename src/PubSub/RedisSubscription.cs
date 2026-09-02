using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary>Buffers a StackExchange.Redis message queue into a bounded channel of deserialized messages.</summary>
internal sealed class RedisSubscription<T> : IRedisSubscription<T>
{
    private const int DropLogInterval = 1000;

    private readonly ChannelMessageQueue _queue;
    private readonly Channel<RedisMessage<T>> _buffer;
    private readonly JsonSerializerOptions _json;
    private readonly ILogger _logger;
    private long _dropped;
    private int _disposed;

    public RedisSubscription(ChannelMessageQueue queue, RedisPubSubOptions options, ILogger logger)
    {
        _queue = queue;
        _json = options.Json;
        _logger = logger;
        Channel = queue.Channel.ToString();
        _buffer = System.Threading.Channels.Channel.CreateBounded<RedisMessage<T>>(
            new BoundedChannelOptions(options.BufferCapacity)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.DropOldest
            },
            OnDropped);

        queue.OnMessage(OnMessage);
    }

    /// <inheritdoc />
    public string Channel { get; }

    /// <inheritdoc />
    public async IAsyncEnumerable<RedisMessage<T>> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (await WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (_buffer.Reader.TryRead(out var message))
                yield return message;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        await _queue.UnsubscribeAsync().ConfigureAwait(false);
        _buffer.Writer.TryComplete();
    }

    private async ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _buffer.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }

    private void OnMessage(ChannelMessage message)
    {
        if (message.Message.IsNullOrEmpty)
            return;

        T? value;

        try
        {
            value = JsonSerializer.Deserialize<T>((byte[])message.Message!, _json);
        }
        catch (JsonException exception)
        {
            _logger.LogWarning(exception,
                "Dropped a message from channel {Channel} that could not be deserialized as {Type}",
                message.Channel.ToString(), typeof(T).Name);
            return;
        }

        if (value is not null)
            _buffer.Writer.TryWrite(new RedisMessage<T>(message.Channel.ToString(), value));
    }

    private void OnDropped(RedisMessage<T> dropped)
    {
        var count = Interlocked.Increment(ref _dropped);

        if (count == 1 || count % DropLogInterval == 0)
            _logger.LogWarning(
                "Subscriber of {Channel} is too slow: {Dropped} message(s) dropped so far, buffer capacity {Capacity}",
                Channel, count, _buffer.Reader.CanCount ? _buffer.Reader.Count : -1);
    }
}
