using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary>One Redis subscription to a channel, deserialized once and fanned out to every attached reader.</summary>
/// <remarks>
/// A reader per consumer used to mean a Redis subscription and a JSON parse per consumer, so a task channel watched
/// by many pages paid for each of them. The feed closes and unsubscribes when its last reader detaches; a reader that
/// finds it closed asks the registry for a fresh one, and the registry entry is removed under the same lock that
/// closes the feed so that retry never sees the closed one again. A subscription that cannot be opened, or opened
/// again on a replacement connection, ends every reader with that failure: a reader that ended quietly would look
/// like a stream that was over, and nobody would subscribe again.
/// </remarks>
internal sealed class ChannelFeed<T>(RedisChannel channel, IConnectionMultiplexer connection, RedisPubSubOptions options, ILogger logger, Action<ChannelFeed<T>> closed) : IChannelFeed
{
    private readonly RedisChannel _channel = channel;
    private IConnectionMultiplexer _connection = connection;
    private readonly JsonSerializerOptions _json = options.Json;
    private readonly ILogger _logger = logger;
    private readonly Action<ChannelFeed<T>> _closed = closed;
    private readonly Lock _gate = new();
    private readonly HashSet<RedisSubscription<T>> _readers = [];
    private RedisSubscription<T>[] _snapshot = [];
    private ChannelMessageQueue? _queue;
    private Task? _opening;
    private int _generation;
    private bool _isClosed;

    public string Channel { get; } = channel.ToString();

    public bool TryAttach(RedisSubscription<T> reader)
    {
        lock (_gate)
        {
            if (_isClosed)
                return false;

            _readers.Add(reader);
            _snapshot = [.. _readers];
            return true;
        }
    }

    public Task OpenAsync(CancellationToken cancellationToken)
    {
        Task opening;

        lock (_gate)
            opening = _opening ??= OpenCoreAsync(_generation);

        return opening.WaitAsync(cancellationToken);
    }

    /// <summary>Subscribes again on the replacement connection; readers keep their buffers and miss only what was published in between.</summary>
    /// <remarks>
    /// Each open carries a generation: an older open that completes after a newer one releases its queue instead of
    /// recording it. The queue on the previous connection is released as soon as the new one is current, rather than
    /// when that connection is disposed: until then a message published to both servers would reach every reader twice.
    /// The release runs on its own, so a new reader never waits for a server that is going away.
    /// </remarks>
    public async Task ReopenAsync(IConnectionMultiplexer connection)
    {
        Task opening;

        lock (_gate)
        {
            if (_isClosed)
                return;

            _connection = connection;
            opening = _opening = OpenCoreAsync(++_generation);
        }

        try
        {
            await opening.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.ReopenFailed(exception, Channel);
        }
    }

    public async ValueTask DetachAsync(RedisSubscription<T> reader)
    {
        lock (_gate)
        {
            _readers.Remove(reader);
            _snapshot = [.. _readers];

            if (_readers.Count > 0 || _isClosed)
                return;

            _isClosed = true;
            _closed(this);
        }

        await UnsubscribeAsync().ConfigureAwait(false);
    }

    private async Task OpenCoreAsync(int generation)
    {
        ChannelMessageQueue queue;

        try
        {
            queue = await _connection.GetSubscriber().SubscribeAsync(_channel).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            RedisSubscription<T>[] readers;

            lock (_gate)
            {
                _isClosed = true;
                _closed(this);
                readers = _snapshot;
            }

            foreach (var reader in readers)
                reader.Fail(exception);

            throw;
        }

        bool isCurrent;
        ChannelMessageQueue? replaced = null;

        lock (_gate)
        {
            isCurrent = generation == _generation && !_isClosed;

            if (isCurrent)
            {
                queue.OnMessage(OnMessage);
                replaced = _queue;
                _queue = queue;
            }
        }

        if (!isCurrent)
        {
            await ReleaseAsync(queue).ConfigureAwait(false);
            return;
        }

        if (replaced is not null)
            _ = ReleaseAsync(replaced);
    }

    private async Task ReleaseAsync(ChannelMessageQueue queue)
    {
        try
        {
            await queue.UnsubscribeAsync().ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is ObjectDisposedException or RedisException or RedisTimeoutException)
        {
            _logger.ReleasedAfterClose(exception, Channel);
        }
    }

    private async Task UnsubscribeAsync()
    {
        Task? opening;

        lock (_gate)
            opening = _opening;

        if (opening is null)
            return;

        try
        {
            await opening.ConfigureAwait(false);
        }
        catch (Exception)
        {
            return;
        }

        if (_queue is { } queue)
            await ReleaseAsync(queue).ConfigureAwait(false);
    }

    private void OnMessage(ChannelMessage message)
    {
        if (message.Message.IsNullOrEmpty)
            return;

        T? value;

        try
        {
            value = JsonPayload.Read<T>((byte[])message.Message!, _json);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            _logger.DroppedUnreadableMessage(exception, message.Channel.ToString(), typeof(T).Name);
            return;
        }

        if (value is null)
            return;

        RedisMetrics.Received.Add(1);
        var received = new RedisMessage<T>(_channel.IsPattern ? message.Channel.ToString() : Channel, value);

        foreach (var reader in Volatile.Read(ref _snapshot))
            reader.Offer(received);
    }
}
