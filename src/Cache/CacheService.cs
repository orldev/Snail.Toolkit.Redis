using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;

namespace Snail.Toolkit.Redis.Cache;

/// <summary><see cref="ICacheService"/> over <see cref="IDistributedCache"/> with System.Text.Json payloads.</summary>
public sealed class CacheService(IDistributedCache distributedCache, IOptions<CacheServiceOptions> options) : ICacheService
{
    private readonly CacheServiceOptions _options = options.Value;

    /// <inheritdoc />
    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class?
    {
        var payload = await distributedCache.GetAsync(key, cancellationToken).ConfigureAwait(false);

        return payload is null ? null : JsonSerializer.Deserialize<T>(payload, _options.Json);
    }

    /// <inheritdoc />
    public async Task<T?> GetAsync<T>(
        string key,
        Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class?
    {
        var cachedValue = await GetAsync<T>(key, cancellationToken).ConfigureAwait(false);

        if (cachedValue is not null)
            return cachedValue;

        cachedValue = await factory(cancellationToken).ConfigureAwait(false);

        if (cachedValue is not null)
            await SetAsync(key, cachedValue, options, cancellationToken).ConfigureAwait(false);

        return cachedValue;
    }

    /// <inheritdoc />
    public Task SetAsync<T>(
        string key,
        T value,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class?
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(value, _options.Json);

        options ??= new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = _options.AbsoluteExpirationRelativeToNow,
            SlidingExpiration = _options.SlidingExpiration
        };

        return distributedCache.SetAsync(key, payload, options, cancellationToken);
    }

    /// <inheritdoc />
    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        return distributedCache.RemoveAsync(key, cancellationToken);
    }
}
