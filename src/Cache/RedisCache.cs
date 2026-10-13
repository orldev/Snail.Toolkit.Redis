using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using StackExchange.Redis.KeyspaceIsolation;

namespace Snail.Toolkit.Redis.Cache;

/// <summary><see cref="IRedisCache"/> over the shared connection with System.Text.Json payloads.</summary>
/// <remarks>
/// Entries are hashes with the fields Microsoft's distributed cache writes (absexp, sldexp, data), so a value stored
/// through either side reads back through the other. The library keeps its own implementation because Microsoft's
/// disposes the multiplexer it is handed, and here that multiplexer belongs to everyone. The instance name reaches
/// Redis through a key-prefixed view of the database, made once per multiplexer, which writes the prefix and the key
/// one after the other instead of joining them into a new string on every call.
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
    private KeyedDatabase? _database;

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

        IDatabase database;
        RedisValue[] fields;

        try
        {
            database = Database(await connection.ConnectAsync(cancellationToken).ConfigureAwait(false));
            fields = await database.HashGetAsync(key, Fields).ConfigureAwait(false);
        }
        catch (Exception exception) when (Misses(exception))
        {
            ReportUnavailable(exception, key);
            return null;
        }
        catch (RedisServerException exception) when (IsWrongType(exception))
        {
            await EvictAsync<T>(exception, key, cancellationToken).ConfigureAwait(false);
            return null;
        }

        if (Stored(database, key, fields) is not { } payload)
            return null;

        try
        {
            return Hit<T>(payload);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            await EvictAsync<T>(exception, key, cancellationToken).ConfigureAwait(false);
            return null;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Reads the entry itself rather than awaiting the overload without a factory: against a server every hit would
    /// otherwise pay for two state machines and a second continuation. Every way the other overload answers null —
    /// a missing, unreadable or wrongly typed entry, or an unreachable Redis — runs the factory here.
    /// </remarks>
    public async Task<T?> GetAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        cancellationToken.ThrowIfCancellationRequested();

        IDatabase database;
        RedisValue[] fields;

        try
        {
            database = Database(await connection.ConnectAsync(cancellationToken).ConfigureAwait(false));
            fields = await database.HashGetAsync(key, Fields).ConfigureAwait(false);
        }
        catch (Exception exception) when (Misses(exception))
        {
            ReportUnavailable(exception, key);
            return await LoadOnceAsync(key, factory, options, cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException exception) when (IsWrongType(exception))
        {
            await EvictAsync<T>(exception, key, cancellationToken).ConfigureAwait(false);
            return await LoadOnceAsync(key, factory, options, cancellationToken).ConfigureAwait(false);
        }

        if (Stored(database, key, fields) is { } payload)
        {
            try
            {
                if (Hit<T>(payload) is { } value)
                    return value;
            }
            catch (Exception exception) when (exception is JsonException or NotSupportedException)
            {
                await EvictAsync<T>(exception, key, cancellationToken).ConfigureAwait(false);
            }
        }

        return await LoadOnceAsync(key, factory, options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Redis answers the batch in order, so once the expiry is answered the store is too: the two are awaited one
    /// after the other without throwing, with a single wait in practice, and only a failure goes through Task.WhenAll,
    /// which reports it as before. A store that succeeds builds no combined task.
    /// </remarks>
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
            var multiplexer = await connection.ConnectAsync(cancellationToken).ConfigureAwait(false);
            var batch = Database(multiplexer).CreateBatch();
            var stored = batch.HashSetAsync(key, entry);
            var expires = timeToLive is { } lifetime ? batch.KeyExpireAsync(key, lifetime) : batch.KeyPersistAsync(key);
            batch.Execute();
            await ((Task)expires).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            await stored.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

            if (!stored.IsCompletedSuccessfully || !expires.IsCompletedSuccessfully)
                await Task.WhenAll(stored, expires).ConfigureAwait(false);
        }
        catch (Exception exception) when (Misses(exception))
        {
            ReportUnavailable(exception, key);
        }
    }

    /// <inheritdoc />
    /// <remarks>Once the connection is open the delete's own task is handed back, so no state machine is built around it.</remarks>
    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        cancellationToken.ThrowIfCancellationRequested();

        if (connection.Opened is not { } multiplexer)
            return RemoveWhenConnectedAsync(key, cancellationToken);

        return Database(multiplexer).KeyDeleteAsync(key);
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

    /// <summary>The database of the multiplexer, seen through the instance name when there is one; made again only when the multiplexer was replaced.</summary>
    private IDatabase Database(IConnectionMultiplexer multiplexer)
    {
        if (Volatile.Read(ref _database) is { } held && ReferenceEquals(held.Multiplexer, multiplexer))
            return held.Database;

        var database = multiplexer.GetDatabase();
        var keyed = _prefix.Length == 0 ? database : database.WithKeyPrefix(_prefix);

        Volatile.Write(ref _database, new KeyedDatabase(multiplexer, keyed));

        return keyed;
    }

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
    private void Refresh(IDatabase database, string key, RedisValue absolute, RedisValue sliding)
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
            database.KeyExpire(key, timeToLive, CommandFlags.FireAndForget);
    }

    private void ReportUnavailable(Exception exception, string key)
    {
        RedisMetrics.CacheFailures.Add(1);

        if (_throttle.Allows())
            logger.CacheUnavailable(exception, key);
    }

    /// <summary>The payload of the entry read, its expiry slid forward; null for a missing entry, counted as a miss.</summary>
    private byte[]? Stored(IDatabase database, string key, RedisValue[] fields)
    {
        if (fields[2].IsNull)
        {
            RedisMetrics.CacheMisses.Add(1);
            return null;
        }

        Refresh(database, key, fields[0], fields[1]);

        return (byte[])fields[2]!;
    }

    private T? Hit<T>(byte[] payload)
    {
        var value = JsonPayload.Read<T>(payload, _json);
        RedisMetrics.CacheHits.Add(1);

        return value;
    }

    private Task EvictAsync<T>(Exception exception, string key, CancellationToken cancellationToken)
    {
        RedisMetrics.CacheMisses.Add(1);
        logger.EvictedUnreadableEntry(exception, key, typeof(T).Name);

        return RemoveAsync(key, cancellationToken);
    }

    private async Task RemoveWhenConnectedAsync(string key, CancellationToken cancellationToken)
    {
        var multiplexer = await connection.ConnectAsync(cancellationToken).ConfigureAwait(false);

        await Database(multiplexer).KeyDeleteAsync(key).ConfigureAwait(false);
    }

    /// <summary>Runs the factory for a missed key once for all concurrent callers of the same key and type, and stores what it built.</summary>
    /// <remarks>
    /// A method of its own because the single-flight lambda captures the arguments: in the public method the compiler
    /// would allocate that capture on entry, so every hit would pay for a closure only a miss uses.
    /// </remarks>
    private async Task<T?> LoadOnceAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        DistributedCacheEntryOptions? options,
        CancellationToken cancellationToken) where T : class
    {
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

    /// <summary>The database the cache talks to and the multiplexer it belongs to.</summary>
    private sealed record KeyedDatabase(IConnectionMultiplexer Multiplexer, IDatabase Database);
}
