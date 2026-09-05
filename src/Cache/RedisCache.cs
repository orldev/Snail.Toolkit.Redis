using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Cache;

/// <summary><see cref="IRedisCache"/> over the shared connection with System.Text.Json payloads.</summary>
/// <remarks>
/// Entries are hashes with the fields Microsoft's distributed cache writes (absexp, sldexp, data), so a value stored
/// through either side reads back through the other. The library keeps its own implementation because Microsoft's
/// disposes the multiplexer it is handed, and here that multiplexer belongs to everyone.
/// </remarks>
internal sealed class RedisCache(
    IRedisConnection connection,
    IOptions<RedisOptions> redis,
    IOptions<RedisCacheOptions> options,
    ILogger<RedisCache> logger) : IRedisCache
{
    private const long None = -1;
    private static readonly long DateTimeOffsetMaxTicks = DateTimeOffset.MaxValue.Ticks;
    private static readonly RedisValue AbsoluteField = "absexp";
    private static readonly RedisValue SlidingField = "sldexp";
    private static readonly RedisValue DataField = "data";
    private static readonly RedisValue[] Fields = [AbsoluteField, SlidingField, DataField];

    private readonly string _prefix = redis.Value.InstanceName ?? string.Empty;
    private readonly RedisCacheOptions _options = options.Value;
    private readonly JsonSerializerOptions _json = JsonPayload.Prepared(options.Value.Json);
    private readonly ConcurrentDictionary<(string Key, Type Type), Lazy<Task<object?>>> _flights = new();
    private readonly LogThrottle _throttle = new(TimeSpan.FromMinutes(1));

    private IDatabase Database => connection.Multiplexer.GetDatabase();

    /// <inheritdoc />
    /// <remarks>
    /// An entry that no longer deserializes, typically after a deploy changed the shape of the type, would otherwise
    /// fail every reader until its expiry. It is evicted and reported as a miss so the factory rebuilds it. A type the
    /// serializer cannot handle at all fails with NotSupportedException rather than JsonException and counts the same.
    /// </remarks>
    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        cancellationToken.ThrowIfCancellationRequested();

        RedisValue[] fields;

        try
        {
            fields = await Database.HashGetAsync(Prefixed(key), Fields).ConfigureAwait(false);
        }
        catch (Exception exception) when (Misses(exception))
        {
            ReportUnavailable(exception, key);
            return null;
        }
        catch (RedisServerException exception) when (IsWrongType(exception))
        {
            RedisMetrics.CacheMisses.Add(1);
            logger.EvictedUnreadableEntry(exception, key, typeof(T).Name);
            await RemoveAsync(key, cancellationToken).ConfigureAwait(false);
            return null;
        }

        if (fields[2].IsNull)
        {
            RedisMetrics.CacheMisses.Add(1);
            return null;
        }

        Refresh(key, fields[0], fields[1]);

        try
        {
            var value = JsonPayload.Read<T>((byte[])fields[2]!, _json);
            RedisMetrics.CacheHits.Add(1);
            return value;
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            RedisMetrics.CacheMisses.Add(1);
            logger.EvictedUnreadableEntry(exception, key, typeof(T).Name);
            await RemoveAsync(key, cancellationToken).ConfigureAwait(false);
            return null;
        }
    }

    /// <inheritdoc />
    public async Task<T?> GetAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(factory);

        var cached = await GetAsync<T>(key, cancellationToken).ConfigureAwait(false);

        if (cached is not null)
            return cached;

        var flight = _flights.GetOrAdd((key, typeof(T)), _ => new Lazy<Task<object?>>(() => LoadAsync(key, factory, options, cancellationToken)));

        try
        {
            return (T?)await flight.Value.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return (T?)await LoadAsync(key, factory, options, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _flights.TryRemove(new KeyValuePair<(string, Type), Lazy<Task<object?>>>((key, typeof(T)), flight));
        }
    }

    /// <inheritdoc />
    public async Task SetAsync<T>(
        string key,
        T value,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();

        var payload = JsonPayload.Write(value, _json);
        var now = DateTimeOffset.UtcNow;
        var (absolute, sliding) = Expiry(options, now);
        var timeToLive = TimeToLive(absolute, sliding, now);
        HashEntry[] entry =
        [
            new(AbsoluteField, absolute?.Ticks ?? None),
            new(SlidingField, sliding?.Ticks ?? None),
            new(DataField, payload)
        ];

        try
        {
            var prefixed = Prefixed(key);
            var batch = Database.CreateBatch();
            var stored = batch.HashSetAsync(prefixed, entry);
            var expires = timeToLive is { } lifetime ? batch.KeyExpireAsync(prefixed, lifetime) : batch.KeyPersistAsync(prefixed);
            batch.Execute();
            await Task.WhenAll(stored, expires).ConfigureAwait(false);
        }
        catch (Exception exception) when (Misses(exception))
        {
            ReportUnavailable(exception, key);
        }
    }

    /// <inheritdoc />
    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        cancellationToken.ThrowIfCancellationRequested();

        return Database.KeyDeleteAsync(Prefixed(key));
    }

    private static TimeSpan? TimeToLive(DateTimeOffset? absolute, TimeSpan? sliding, DateTimeOffset now)
    {
        if (absolute is not { } deadline)
            return sliding;

        var remaining = deadline - now;

        if (remaining <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(absolute), deadline, "The absolute expiration has already passed.");

        return sliding is { } window && window < remaining ? window : remaining;
    }

    private RedisKey Prefixed(string key) => _prefix + key;

    /// <remarks>A value of another type under the cache's own prefix is somebody else's mistake; it is evicted like any entry the cache cannot read.</remarks>
    private static bool IsWrongType(RedisServerException exception) =>
        exception.Message.StartsWith("WRONGTYPE", StringComparison.Ordinal);

    private bool Misses(Exception exception) =>
        exception is RedisConnectionException or RedisTimeoutException && _options.OnFailure is CacheFailure.Miss;

    private (DateTimeOffset? Absolute, TimeSpan? Sliding) Expiry(DistributedCacheEntryOptions? options, DateTimeOffset now)
    {
        if (options is null)
            return (_options.AbsoluteExpirationRelativeToNow is { } relative ? now + relative : null, _options.SlidingExpiration);

        var absolute = options.AbsoluteExpiration ?? (options.AbsoluteExpirationRelativeToNow is { } window ? now + window : null);

        return (absolute, options.SlidingExpiration);
    }

    /// <remarks>Metadata written by another client is not trusted: an absolute expiry outside the DateTimeOffset range is ignored rather than thrown.</remarks>
    private void Refresh(string key, RedisValue absolute, RedisValue sliding)
    {
        if (!sliding.TryParse(out long slidingTicks) || slidingTicks <= 0)
            return;

        var timeToLive = TimeSpan.FromTicks(slidingTicks);

        if (absolute.TryParse(out long absoluteTicks) && absoluteTicks > 0 && absoluteTicks <= DateTimeOffsetMaxTicks)
        {
            var remaining = new DateTimeOffset(absoluteTicks, TimeSpan.Zero) - DateTimeOffset.UtcNow;
            timeToLive = remaining < timeToLive ? remaining : timeToLive;
        }

        if (timeToLive > TimeSpan.Zero)
            Database.KeyExpire(Prefixed(key), timeToLive, CommandFlags.FireAndForget);
    }

    private void ReportUnavailable(Exception exception, string key)
    {
        RedisMetrics.CacheFailures.Add(1);

        if (_throttle.Allows())
            logger.CacheUnavailable(exception, key);
    }

    private async Task<object?> LoadAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        DistributedCacheEntryOptions? options,
        CancellationToken cancellationToken) where T : class
    {
        var value = await factory(cancellationToken).ConfigureAwait(false);

        if (value is not null)
            await SetAsync(key, value, options, cancellationToken).ConfigureAwait(false);

        return value;
    }
}
