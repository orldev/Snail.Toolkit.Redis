using Microsoft.Extensions.Caching.Distributed;

namespace Snail.Toolkit.Redis.Cache;

/// <summary>Cache of JSON-serialized values over Redis with an optional fallback factory.</summary>
/// <remarks>
/// Null is the miss marker, so only reference types are cached and a null value is never stored: storing it would
/// read back as a miss and run the factory on every call.
/// </remarks>
public interface IRedisCache
{
    /// <summary>Returns the cached value for the key, or null when the key is absent or unreadable.</summary>
    Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class;

    /// <summary>Returns the cached value for the key; on a miss calls the factory with the same token, stores a non-null result and returns it.</summary>
    /// <remarks>
    /// Concurrent misses of one key inside the process share a single factory call; a caller whose leader was
    /// cancelled runs its own.
    /// </remarks>
    Task<T?> GetAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>Stores the value under the key; without options the defaults from <see cref="RedisCacheOptions"/> apply.</summary>
    Task SetAsync<T>(
        string key,
        T value,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>Removes the value stored under the key.</summary>
    /// <remarks>A failed removal always throws, whatever <see cref="RedisCacheOptions.OnFailure"/> says: stale data left behind is a fact the caller has to know.</remarks>
    Task RemoveAsync(string key, CancellationToken cancellationToken = default);
}
