namespace Snail.Toolkit.Redis.Streams;

/// <summary>An entry read from a stream together with the id Redis assigned to it.</summary>
public sealed record RedisStreamEntry<T>(string Id, T Message);
