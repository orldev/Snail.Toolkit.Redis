using StackExchange.Redis;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary>What the registry needs from a feed regardless of its message type.</summary>
internal interface IChannelFeed
{
    /// <summary>Subscribes again on the replacement connection.</summary>
    Task ReopenAsync(IConnectionMultiplexer connection);
}
