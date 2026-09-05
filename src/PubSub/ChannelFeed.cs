using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary>One Redis subscription to a channel, deserialized once and fanned out to every attached reader.</summary>
/// <remarks>
/// A reader per consumer used to mean a Redis subscription and a JSON parse per consumer, so a task channel watched
/// by many pages paid for each of them. The feed closes and unsubscribes when its last reader detaches; a reader that
/// finds it closed asks the registry for a fresh one, and the registry entry is removed under the same lock that
/// closes the feed so that retry never sees the closed one again.
/// </remarks>
internal sealed class ChannelFeed<T> : IChannelFeed
{
    private readonly RedisChannel _channel;
    private IConnectionMultiplexer _connection;
    private readonly JsonSerializerOptions _json;
    private readonly ILogger _logger;
    private readonly Action<ChannelFeed<T>> _closed;
    private readonly Lock _gate = new();
    private readonly HashSet<RedisSubscription<T>> _readers = [];
    private RedisSubscription<T>[] _snapshot = [];
    private ChannelMessageQueue? _queue;
    private Task? _opening;
    private int _generation;
    private bool _isClosed;

    public ChannelFeed(RedisChannel channel, IConnectionMultiplexer connection, RedisPubSubOptions options, ILogger logger, Action<ChannelFeed<T>> closed)
    {
        _channel = channel;
        _connection = connection;
        _json = options.Json;
        _logger = logger;
        _closed = closed;
        Channel = channel.ToString();
    }

    public string Channel { get; }

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
    /// <remarks>Each open carries a generation: an older open that completes after a newer one releases its queue instead of recording it.</remarks>
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
        catch
        {
            RedisSubscription<T>[] readers;

            lock (_gate)
            {
                _isClosed = true;
                _closed(this);
                readers = _snapshot;
            }

            foreach (var reader in readers)
                reader.Complete();

            throw;
        }

        bool isCurrent;

        lock (_gate)
        {
            isCurrent = generation == _generation && !_isClosed;

            if (isCurrent)
            {
                queue.OnMessage(OnMessage);
                _queue = queue;
            }
        }

        if (!isCurrent)
            await ReleaseAsync(queue).ConfigureAwait(false);
    }

    private async Task ReleaseAsync(ChannelMessageQueue queue)
    {
        try
        {
            await queue.UnsubscribeAsync().ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is ObjectDisposedException or RedisException)
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
