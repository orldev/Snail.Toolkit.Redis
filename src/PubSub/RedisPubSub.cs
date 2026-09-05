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
    private readonly IRedisConnection _connection;
    private readonly ILogger<RedisPubSub> _logger;
    private readonly RedisPubSubOptions _options;
    private readonly JsonSerializerOptions _json;
    private readonly ConcurrentDictionary<(RedisChannel Channel, Type Type), IChannelFeed> _feeds = new();

    public RedisPubSub(IRedisConnection connection, IOptions<RedisPubSubOptions> options, ILogger<RedisPubSub> logger)
    {
        _connection = connection;
        _logger = logger;
        _options = options.Value;
        _json = JsonPayload.Prepared(_options.Json);
        connection.Replaced += OnReplaced;
    }

    /// <inheritdoc />
    public Task<long> PublishAsync<T>(string channel, T message, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        var payload = JsonPayload.Write(message, _json);
        RedisMetrics.Published.Add(1);

        return _connection.Multiplexer.GetSubscriber().PublishAsync(RedisChannel.Literal(channel), payload);
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
            var feed = (ChannelFeed<T>)_feeds.GetOrAdd(
                (channel, typeof(T)),
                key => new ChannelFeed<T>(channel, _connection.Multiplexer, _options, _logger, closed => _feeds.TryRemove(new(key, closed))));
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

    private void OnReplaced(IConnectionMultiplexer multiplexer)
    {
        foreach (var feed in _feeds.Values)
            _ = feed.ReopenAsync(multiplexer);
    }
}
