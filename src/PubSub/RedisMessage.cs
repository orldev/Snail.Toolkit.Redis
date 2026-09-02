namespace Snail.Toolkit.Redis.PubSub;

/// <summary>A message received from a subscription together with the channel it was published to.</summary>
public sealed record RedisMessage<T>(string Channel, T Message);
