using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary><see cref="IRedisPubSub"/> over the shared <see cref="IConnectionMultiplexer"/> with System.Text.Json payloads.</summary>
public sealed class RedisPubSub(
    IConnectionMultiplexer connection,
    IOptions<RedisPubSubOptions> options,
    ILogger<RedisPubSub> logger) : IRedisPubSub
{
    private readonly RedisPubSubOptions _options = options.Value;

    /// <inheritdoc />
    public Task PublishAsync<T>(string channel, T message, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        var payload = JsonSerializer.SerializeToUtf8Bytes(message, _options.Json);

        return connection.GetSubscriber().PublishAsync(RedisChannel.Literal(channel), payload, CommandFlags.FireAndForget);
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

        var queue = await connection.GetSubscriber().SubscribeAsync(channel).ConfigureAwait(false);

        return new RedisSubscription<T>(queue, _options, logger);
    }
}
