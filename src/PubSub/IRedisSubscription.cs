namespace Snail.Toolkit.Redis.PubSub;

/// <summary>An active Redis subscription; disposing it unsubscribes and completes the message stream.</summary>
public interface IRedisSubscription<T> : IAsyncDisposable
{
    /// <summary>The literal channel or the glob pattern this subscription listens to.</summary>
    string Channel { get; }

    /// <summary>Yields messages as they arrive; completes when the token is cancelled or the subscription is disposed.</summary>
    IAsyncEnumerable<RedisMessage<T>> ReadAllAsync(CancellationToken cancellationToken = default);
}
