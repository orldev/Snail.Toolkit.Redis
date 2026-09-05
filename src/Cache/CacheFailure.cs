namespace Snail.Toolkit.Redis.Cache;

/// <summary>What a read or write does when Redis cannot be reached.</summary>
public enum CacheFailure
{
    /// <summary>The connection error propagates to the caller.</summary>
    Throw,

    /// <summary>A read reports a miss and a write is skipped; the first failure and then one per minute are logged.</summary>
    Miss
}
