using System.Collections.Concurrent;

namespace Snail.Toolkit.Redis;

/// <summary>What is made from a name, kept for the first names asked for so that it is made once rather than per call.</summary>
/// <remarks>
/// Channel and stream names are usually a handful repeated on every call, but nothing stops an application from
/// naming one per user or per order. The cache therefore stops growing at its limit: a name beyond it is made on
/// every call, as if there were no cache, and nothing already kept is ever evicted.
/// </remarks>
internal sealed class NameCache<T>(Func<string, T> make, int limit)
{
    private readonly ConcurrentDictionary<string, T> _made = new();
    private int _count;

    /// <summary>What the name makes, from the cache when it holds the name.</summary>
    public T For(string name)
    {
        if (_made.TryGetValue(name, out var made))
            return made;

        made = make(name);

        if (Volatile.Read(ref _count) < limit && _made.TryAdd(name, made))
            Interlocked.Increment(ref _count);

        return made;
    }
}
