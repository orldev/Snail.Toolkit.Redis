using StackExchange.Redis;

namespace Snail.Toolkit.Redis;

/// <summary>The library's view of the shared connection: the multiplexer of the moment and the settings it was opened with.</summary>
/// <remarks>
/// The <see cref="IConnectionMultiplexer"/> registered for consumers is a snapshot; the library's own components
/// read <see cref="Multiplexer"/> on every call so that a replaced connection reaches them without a restart.
/// </remarks>
internal interface IRedisConnection
{
    /// <summary>The multiplexer currently in use.</summary>
    IConnectionMultiplexer Multiplexer { get; }

    /// <summary>The live settings the multiplexer was opened with; a rotated password lands here.</summary>
    ConfigurationOptions Configuration { get; }

    /// <summary>Raised with the new multiplexer after the endpoints changed and a connection to them opened.</summary>
    event Action<IConnectionMultiplexer>? Replaced;
}
