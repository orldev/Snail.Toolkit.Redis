namespace Snail.Toolkit.Redis.Streams;

/// <summary>Append-only history over a Redis stream: entries outlive their publication, so a late reader catches up before it follows.</summary>
/// <remarks>
/// Pub/sub loses what was published before a page opened; the README used to suggest a cache snapshot as a
/// workaround. A stream keeps the entries, and a reader that subscribes before it reads history misses nothing.
/// </remarks>
public interface IRedisStream
{
    /// <summary>Appends the message and returns its entry id; with a maximum length the stream is trimmed to about that many entries.</summary>
    Task<string> AppendAsync<T>(string stream, T message, int? maxLength = null, CancellationToken cancellationToken = default);

    /// <summary>Yields every entry after the id, or every entry when it is null, then follows new ones until the token is cancelled.</summary>
    /// <remarks>New entries are announced over pub/sub and picked up at once; the poll interval only covers a lost announcement.</remarks>
    IAsyncEnumerable<RedisStreamEntry<T>> ReadAsync<T>(string stream, string? after = null, CancellationToken cancellationToken = default);
}
