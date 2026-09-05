using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.PubSub;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Streams;

/// <summary><see cref="IRedisStream"/> over the shared connection, announcing every append on a pub/sub channel.</summary>
/// <remarks>
/// StackExchange.Redis multiplexes one connection and therefore never blocks on XREAD, so following a stream means
/// reading again when something arrived. The announcement makes that immediate; the poll interval is the safety net.
/// </remarks>
internal sealed class RedisStreams(
    IRedisConnection connection,
    IRedisPubSub pubSub,
    IOptions<RedisStreamOptions> options,
    ILogger<RedisStreams> logger) : IRedisStream
{
    private const string Start = "0-0";
    private static readonly RedisValue DataField = "data";

    private readonly RedisStreamOptions _options = options.Value;
    private readonly JsonSerializerOptions _json = JsonPayload.Prepared(options.Value.Json);

    private IDatabase Database => connection.Multiplexer.GetDatabase();

    /// <inheritdoc />
    public async Task<string> AppendAsync<T>(string stream, T message, int? maxLength = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        var payload = JsonPayload.Write(message, _json);
        var id = await Database.StreamAddAsync(stream, DataField, payload, maxLength: maxLength, useApproximateMaxLength: true).ConfigureAwait(false);
        await pubSub.PublishAsync(Announcements(stream), id.ToString(), cancellationToken).ConfigureAwait(false);

        return id.ToString();
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RedisStreamEntry<T>> ReadAsync<T>(
        string stream,
        string? after = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);

        await using var announcements = await pubSub.SubscribeAsync<string>(Announcements(stream), cancellationToken).ConfigureAwait(false);
        await using var arrivals = announcements.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
        var position = after ?? Start;
        Task<bool>? arrival = null;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var entries = await Database.StreamReadAsync(stream, position, _options.PageSize).ConfigureAwait(false);

                if (entries.Length > 0)
                {
                    foreach (var entry in entries)
                    {
                        position = entry.Id.ToString();

                        if (Read<T>(stream, entry) is { } message)
                            yield return new RedisStreamEntry<T>(position, message);
                    }

                    continue;
                }

                arrival ??= arrivals.MoveNextAsync().AsTask();
                var completed = await Task.WhenAny(arrival, Task.Delay(_options.PollInterval, cancellationToken)).ConfigureAwait(false);

                if (completed != arrival)
                    continue;

                var announced = await arrival.ConfigureAwait(false);
                arrival = null;

                if (!announced)
                    yield break;
            }
        }
        finally
        {
            if (arrival is not null)
                await arrival.ConfigureAwait(false);
        }
    }

    private static string Announcements(string stream) => $"stream:{stream}:appended";

    private T? Read<T>(string stream, StreamEntry entry)
    {
        var data = entry.Values.FirstOrDefault(value => value.Name == DataField).Value;

        if (data.IsNullOrEmpty)
            return default;

        try
        {
            return JsonPayload.Read<T>((byte[])data!, _json);
        }
        catch (JsonException exception)
        {
            logger.SkippedUnreadableEntry(exception, stream, entry.Id.ToString(), typeof(T).Name);
            return default;
        }
    }
}
