using Microsoft.Extensions.Caching.Distributed;

namespace Snail.Toolkit.Redis.Cache;

/// <summary>Distributed cache of JSON-serialized values with an optional fallback factory.</summary>
public interface ICacheService
{
    /// <summary>Returns the cached value for the key, or null when the key is absent; null is the miss marker, hence reference types only.</summary>
    Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class?;

    /// <summary>Returns the cached value for the key; on a miss calls the factory with the same token, stores its result and returns it.</summary>
    Task<T?> GetAsync<T>(
        string key,
        Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class?;

    /// <summary>Stores the value under the key; without options the defaults from <see cref="CacheServiceOptions"/> apply.</summary>
    Task SetAsync<T>(
        string key,
        T value,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class?;

    /// <summary>Removes the value stored under the key.</summary>
    Task RemoveAsync(string key, CancellationToken cancellationToken = default);
}
