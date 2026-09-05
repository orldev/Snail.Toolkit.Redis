namespace Snail.Toolkit.Redis;

/// <summary>Lets a repeating warning through once per interval so an outage does not flood the log.</summary>
internal sealed class LogThrottle(TimeSpan interval)
{
    private long _last = long.MinValue;

    public bool Allows()
    {
        var now = Environment.TickCount64;
        var last = Volatile.Read(ref _last);

        if (last != long.MinValue && now - last < interval.TotalMilliseconds)
            return false;

        return Interlocked.CompareExchange(ref _last, now, last) == last;
    }
}
