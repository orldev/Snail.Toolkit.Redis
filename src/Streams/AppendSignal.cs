using Snail.Toolkit.Redis.PubSub;

namespace Snail.Toolkit.Redis.Streams;

/// <summary>Wakes a follower when an append is announced, or when the poll interval passes without one.</summary>
/// <remarks>
/// Announcements that arrived while the follower was reading are taken together: a burst of appends wakes it once,
/// and one read fetches what all of them announced. Once the announcements have ended the follower lives on the poll
/// alone, and a subscription that failed ends the wait with its failure. Disposal stops the announcements before the
/// pending wait is awaited, so a follower that leaves its loop never waits for an append that may not come.
/// </remarks>
internal sealed class AppendSignal : IAsyncDisposable
{
    private static readonly Task Never = new TaskCompletionSource().Task;

    private readonly IRedisSubscription<string> _announcements;
    private readonly IAsyncEnumerator<RedisMessage<string>> _arrivals;
    private readonly CancellationTokenSource _stop;
    private readonly TimeSpan _pollInterval;
    private Task<bool>? _pending;
    private bool _isEnded;

    private AppendSignal(IRedisSubscription<string> announcements, CancellationTokenSource stop, TimeSpan pollInterval)
    {
        _announcements = announcements;
        _stop = stop;
        _pollInterval = pollInterval;
        _arrivals = announcements.ReadAllAsync(stop.Token).GetAsyncEnumerator(stop.Token);
    }

    /// <summary>Subscribes to the announcements of a stream; the signal is live when the task completes.</summary>
    public static async Task<AppendSignal> SubscribeAsync(IRedisPubSub pubSub, string channel, TimeSpan pollInterval, CancellationToken cancellationToken)
    {
        var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        try
        {
            var announcements = await pubSub.SubscribeAsync<string>(channel, cancellationToken).ConfigureAwait(false);

            return new AppendSignal(announcements, stop, pollInterval);
        }
        catch
        {
            stop.Dispose();
            throw;
        }
    }

    /// <summary>Waits for an announcement or for the poll interval, whichever comes first.</summary>
    public async Task WaitAsync()
    {
        using var poll = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        var elapsed = Task.Delay(_pollInterval, poll.Token);
        var completed = await Task.WhenAny(Pending(), elapsed).ConfigureAwait(false);
        await poll.CancelAsync().ConfigureAwait(false);

        if (completed != elapsed)
            await DrainAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);

        if (_pending is { } pending)
            await ((Task)pending).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        await _arrivals.DisposeAsync().ConfigureAwait(false);
        await _announcements.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose();
    }

    /// <summary>The announcement being waited for, or one that never comes once the announcements have ended.</summary>
    private Task Pending()
    {
        if (_isEnded)
            return Never;

        return _pending ??= _arrivals.MoveNextAsync().AsTask();
    }

    /// <summary>Takes the announcement that arrived and every one already buffered behind it.</summary>
    private async Task DrainAsync()
    {
        var hasMore = await _pending!.ConfigureAwait(false);
        _pending = null;

        while (hasMore)
        {
            var next = _arrivals.MoveNextAsync();

            if (!next.IsCompleted)
            {
                _pending = next.AsTask();
                return;
            }

            hasMore = await next.ConfigureAwait(false);
        }

        _isEnded = true;
    }
}
