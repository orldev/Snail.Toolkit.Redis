using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary><see cref="IRedisPubSub"/> over the shared connection with System.Text.Json payloads.</summary>
internal sealed class RedisPubSub : IRedisPubSub
{
    private const int ChannelLimit = 256;

    private readonly IRedisConnection _connection;
    private readonly ILogger<RedisPubSub> _logger;
    private readonly RedisPubSubOptions _options;
    private readonly JsonSerializerOptions _json;
    private readonly ConcurrentDictionary<(RedisChannel Channel, Type Type), IChannelFeed> _feeds = new();
    private readonly NameCache<RedisChannel> _channels = new(RedisChannel.Literal, ChannelLimit);
    private HeldSubscriber? _subscriber;

    public RedisPubSub(IRedisConnection connection, IOptions<RedisPubSubOptions> options, ILogger<RedisPubSub> logger)
    {
        _connection = connection;
        _logger = logger;
        _options = options.Value;
        _json = JsonPayload.Prepared(_options.Json);
        connection.Replaced += OnReplaced;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Once the connection is open the publish's own task is handed back, so no state machine is built around it. The
    /// subscriber is kept per multiplexer: StackExchange.Redis builds a new one on every GetSubscriber. The channel's
    /// encoded name is kept for the first 256 names, which StackExchange.Redis would otherwise encode on every publish.
    /// </remarks>
    public Task<long> PublishAsync<T>(string channel, T message, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        var payload = JsonPayload.Write(message, _json);
        RedisMetrics.Published.Add(1);

        if (_connection.Opened is not { } multiplexer)
            return PublishWhenConnectedAsync(channel, payload, cancellationToken);

        return Subscriber(multiplexer).PublishAsync(_channels.For(channel), payload);
    }

    /// <inheritdoc />
    public Task<IRedisSubscription<T>> SubscribeAsync<T>(string channel, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);

        return SubscribeCoreAsync<T>(RedisChannel.Literal(channel), cancellationToken);
    }

    /// <inheritdoc />
    public Task<IRedisSubscription<T>> SubscribePatternAsync<T>(string pattern, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        return SubscribeCoreAsync<T>(RedisChannel.Pattern(pattern), cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<T> StreamAsync<T>(
        string channel,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);

        if (cancellationToken.IsCancellationRequested)
            yield break;

        await using var subscription = await SubscribeAsync<T>(channel, cancellationToken).ConfigureAwait(false);

        await foreach (var message in subscription.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            yield return message.Message;
    }

    private async Task<IRedisSubscription<T>> SubscribeCoreAsync<T>(RedisChannel channel, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (true)
        {
            var multiplexer = await _connection.ConnectAsync(cancellationToken).ConfigureAwait(false);
            var feed = (ChannelFeed<T>)_feeds.GetOrAdd(
                (channel, typeof(T)),
                key => new ChannelFeed<T>(channel, multiplexer, _options, _logger, closed => _feeds.TryRemove(new(key, closed))));
            var reader = new RedisSubscription<T>(feed, _options, _logger);

            if (!feed.TryAttach(reader))
                continue;

            try
            {
                await feed.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await reader.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            return reader;
        }
    }

    private async Task<long> PublishWhenConnectedAsync(string channel, byte[] payload, CancellationToken cancellationToken)
    {
        var multiplexer = await _connection.ConnectAsync(cancellationToken).ConfigureAwait(false);

        return await Subscriber(multiplexer).PublishAsync(_channels.For(channel), payload).ConfigureAwait(false);
    }

    /// <summary>The subscriber of the multiplexer; made again only when the multiplexer was replaced.</summary>
    private ISubscriber Subscriber(IConnectionMultiplexer multiplexer)
    {
        if (Volatile.Read(ref _subscriber) is { } held && ReferenceEquals(held.Multiplexer, multiplexer))
            return held.Subscriber;

        var subscriber = multiplexer.GetSubscriber();

        Volatile.Write(ref _subscriber, new HeldSubscriber(multiplexer, subscriber));

        return subscriber;
    }

    private void OnReplaced(IConnectionMultiplexer multiplexer)
    {
        foreach (var feed in _feeds.Values)
            _ = feed.ReopenAsync(multiplexer);
    }

    /// <summary>The subscriber publishes go through and the multiplexer it belongs to.</summary>
    private sealed record HeldSubscriber(IConnectionMultiplexer Multiplexer, ISubscriber Subscriber);
}
