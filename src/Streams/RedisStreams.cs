using System.Globalization;
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
/// reading again when something arrived. The announcement makes that immediate; the poll interval is the safety net,
/// and it also covers an append whose announcement failed and a read that failed while the connection was being
/// restored: a follower runs until it is cancelled, as promised, and a transient failure costs it one poll interval.
/// The announcement channel's name is kept for the first 256 streams instead of being built on every append.
/// </remarks>
internal sealed class RedisStreams(
    IRedisConnection connection,
    IRedisPubSub pubSub,
    IOptions<RedisStreamOptions> options,
    ILogger<RedisStreams> logger) : IRedisStream
{
    private const string Start = "0-0";
    private const string OnlyNew = "$";
    private const int StreamLimit = 256;
    private static readonly RedisValue DataField = "data"u8.ToArray();

    private readonly RedisStreamOptions _options = options.Value;
    private readonly JsonSerializerOptions _json = JsonPayload.Prepared(options.Value.Json);
    private readonly NameCache<string> _announcements = new(Announcements, StreamLimit);
    private readonly LogThrottle _throttle = new(TimeSpan.FromMinutes(1));

    /// <inheritdoc />
    public async Task<string> AppendAsync<T>(string stream, T message, int? maxLength = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        var payload = JsonPayload.Write(message, _json);
        var database = await DatabaseAsync(cancellationToken).ConfigureAwait(false);
        var added = await database.StreamAddAsync(stream, DataField, payload, maxLength: maxLength, useApproximateMaxLength: true).ConfigureAwait(false);
        var id = added.ToString();
        await AnnounceAsync(stream, id, cancellationToken).ConfigureAwait(false);

        return id;
    }

    /// <inheritdoc />
    /// <remarks>
    /// "$" is accepted as the Redis idiom for "only what comes next": XREAD refuses it without BLOCK, so it is turned
    /// into the id of the last entry before the first read. Any other id is checked up front, as the two numbers of
    /// 64 bits Redis accepts, because a malformed one would otherwise surface as a server error from inside the follow
    /// loop. The position stays the id as Redis handed it over and the data field is matched as bytes, so an entry
    /// costs one string: the id it is handed out with.
    /// </remarks>
    public async IAsyncEnumerable<RedisStreamEntry<T>> ReadAsync<T>(
        string stream,
        string? after = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);

        if (after is not null && after != OnlyNew && !IsEntryId(after))
            throw new ArgumentException($"'{after}' is not a stream entry id; expected '<milliseconds>-<sequence>' or '$'.", nameof(after));

        if (cancellationToken.IsCancellationRequested)
            yield break;

        await using var appends = await AppendSignal.SubscribeAsync(pubSub, _announcements.For(stream), _options.PollInterval, cancellationToken).ConfigureAwait(false);
        RedisValue position = after == OnlyNew ? await LastIdAsync(stream, cancellationToken).ConfigureAwait(false) : after ?? Start;

        while (!cancellationToken.IsCancellationRequested)
        {
            var entries = await PageAsync(stream, position, cancellationToken).ConfigureAwait(false);

            foreach (var entry in entries)
            {
                position = entry.Id;

                if (Read<T>(stream, entry) is { } read)
                    yield return read;
            }

            if (entries.Length == 0)
                await appends.WaitAsync().ConfigureAwait(false);
        }
    }

    private static string Announcements(string stream) => $"stream:{stream}:appended";

    /// <summary>Whether the text is two numbers of 64 bits joined by a dash, the only shape of id XREAD accepts.</summary>
    private static bool IsEntryId(string id)
    {
        var dash = id.AsSpan().IndexOf('-');

        if (dash < 0)
            return false;

        var milliseconds = id.AsSpan(0, dash);
        var sequence = id.AsSpan(dash + 1);

        return ulong.TryParse(milliseconds, NumberStyles.None, CultureInfo.InvariantCulture, out _)
            && ulong.TryParse(sequence, NumberStyles.None, CultureInfo.InvariantCulture, out _);
    }

    private async ValueTask<IDatabase> DatabaseAsync(CancellationToken cancellationToken)
    {
        var multiplexer = await connection.ConnectAsync(cancellationToken).ConfigureAwait(false);

        return multiplexer.GetDatabase();
    }

    private async Task<RedisValue> LastIdAsync(string stream, CancellationToken cancellationToken)
    {
        var database = await DatabaseAsync(cancellationToken).ConfigureAwait(false);
        var last = await database.StreamRangeAsync(stream, "-", "+", count: 1, messageOrder: Order.Descending).ConfigureAwait(false);

        return last.Length == 0 ? Start : last[0].Id;
    }

    /// <remarks>A read that fails while the connection is being restored is answered as an empty page: the follower waits for the next wake-up and reads again.</remarks>
    private async Task<StreamEntry[]> PageAsync(string stream, RedisValue position, CancellationToken cancellationToken)
    {
        try
        {
            var database = await DatabaseAsync(cancellationToken).ConfigureAwait(false);

            return await database.StreamReadAsync(stream, position, _options.PageSize).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is RedisConnectionException or RedisTimeoutException)
        {
            if (_throttle.Allows())
                logger.StreamReadFailed(exception, stream, position.ToString());

            return [];
        }
    }

    /// <remarks>The entry is in the stream once XADD answered; a failed announcement is a late wake-up, covered by the poll, not a failed append.</remarks>
    private async Task AnnounceAsync(string stream, string id, CancellationToken cancellationToken)
    {
        try
        {
            await pubSub.PublishAsync(_announcements.For(stream), id, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is RedisException or RedisTimeoutException)
        {
            logger.AnnouncementFailed(exception, stream, id);
        }
    }

    private RedisStreamEntry<T>? Read<T>(string stream, StreamEntry entry)
    {
        var data = entry.Values.FirstOrDefault(value => value.Name == DataField).Value;

        if (data.IsNullOrEmpty)
            return null;

        try
        {
            var message = JsonPayload.Read<T>((byte[])data!, _json);

            return message is null ? null : new RedisStreamEntry<T>(entry.Id.ToString(), message);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            logger.SkippedUnreadableEntry(exception, stream, entry.Id.ToString(), typeof(T).Name);
            return null;
        }
    }
}
