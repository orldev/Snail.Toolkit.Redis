using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary>A reader attached to a <see cref="ChannelFeed{T}"/>, buffering its messages in a bounded channel.</summary>
internal sealed class RedisSubscription<T> : IRedisSubscription<T>
{
    private const int DropLogInterval = 1000;

    private readonly ChannelFeed<T> _feed;
    private readonly Channel<RedisMessage<T>> _buffer;
    private readonly ILogger _logger;
    private readonly int _capacity;
    private long _dropped;
    private int _disposed;

    public RedisSubscription(ChannelFeed<T> feed, RedisPubSubOptions options, ILogger logger)
    {
        _feed = feed;
        _logger = logger;
        _capacity = options.BufferCapacity;
        _buffer = System.Threading.Channels.Channel.CreateBounded<RedisMessage<T>>(
            new BoundedChannelOptions(options.BufferCapacity)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.DropOldest
            },
            OnDropped);
    }

    /// <inheritdoc />
    public string Channel => _feed.Channel;

    public void Offer(RedisMessage<T> message) => _buffer.Writer.TryWrite(message);

    public void Complete() => _buffer.Writer.TryComplete();

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
    /// <remarks>
    /// The buffer completes before the feed is told, so a pending reader is released even when the multiplexer is
    /// already gone, as it is during host shutdown; the feed's unsubscribe cannot fail anything that still matters.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _buffer.Writer.TryComplete();
        await _feed.DetachAsync(this).ConfigureAwait(false);
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

    private void OnDropped(RedisMessage<T> dropped)
    {
        var count = Interlocked.Increment(ref _dropped);
        RedisMetrics.Dropped.Add(1);

        if (count == 1 || count % DropLogInterval == 0)
            _logger.SubscriberTooSlow(Channel, count, _capacity);
    }
}
