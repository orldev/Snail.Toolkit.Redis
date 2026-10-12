using StackExchange.Redis;

namespace Snail.Toolkit.Redis;

/// <summary>The library's view of the shared connection: the multiplexer of the moment and the settings it was opened with.</summary>
/// <remarks>
/// The <see cref="IConnectionMultiplexer"/> registered for consumers is a snapshot; the library's own components
/// await <see cref="ConnectAsync"/> on every call so that a replaced connection reaches them without a restart.
/// </remarks>
internal interface IRedisConnection
{
    /// <summary>The multiplexer currently in use, opened by the first caller who needs it; blocks while it opens.</summary>
    IConnectionMultiplexer Multiplexer { get; }

    /// <summary>The multiplexer currently in use, opened by the first caller who needs it without holding a thread while it opens.</summary>
    /// <remarks>Once the multiplexer is open the answer is a completed task kept for it, so the call allocates nothing.</remarks>
    Task<IConnectionMultiplexer> ConnectAsync(CancellationToken cancellationToken);

    /// <summary>The multiplexer in use when one is already open, otherwise null; never opens one.</summary>
    /// <remarks>Lets a method that hands back the command's own task skip the connect without awaiting it.</remarks>
    IConnectionMultiplexer? Opened { get; }

    /// <summary>The live settings the multiplexer was opened with; a rotated password lands here.</summary>
    ConfigurationOptions Configuration { get; }

    /// <summary>Raised with the new multiplexer after the endpoints changed and a connection to them opened.</summary>
    event Action<IConnectionMultiplexer>? Replaced;
}
