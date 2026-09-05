namespace Snail.Toolkit.Redis.PubSub;

/// <summary>Typed best-effort publish/subscribe over Redis channels: no persistence and no delivery to late subscribers.</summary>
public interface IRedisPubSub
{
    /// <summary>Serializes the message as JSON, publishes it to the channel and returns how many subscribers received it.</summary>
    /// <remarks>
    /// The publish waits for the broker on purpose: a fire-and-forget publish lost messages silently while the
    /// connection was down, and the receiver count is the cheapest way to learn that nobody is listening.
    /// </remarks>
    Task<long> PublishAsync<T>(string channel, T message, CancellationToken cancellationToken = default);

    /// <summary>Subscribes to one channel; when the returned task completes the subscription is active and no later message is missed.</summary>
    Task<IRedisSubscription<T>> SubscribeAsync<T>(string channel, CancellationToken cancellationToken = default);

    /// <summary>Subscribes to every channel matching a glob pattern (e.g. "task:*:events"); each message carries its channel name.</summary>
    Task<IRedisSubscription<T>> SubscribePatternAsync<T>(string pattern, CancellationToken cancellationToken = default);

    /// <summary>Subscribes to one channel and yields each message as it arrives; cancelling the token unsubscribes and completes the stream.</summary>
    IAsyncEnumerable<T> StreamAsync<T>(string channel, CancellationToken cancellationToken = default);
}
