using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis;

/// <summary>Owns the shared multiplexer, applies a rotated password to future reconnects and moves to new endpoints.</summary>
/// <remarks>
/// The password is written into the very <see cref="ConfigurationOptions"/> instance the multiplexer was opened with:
/// StackExchange.Redis reads it again on every handshake, so a reconnect authenticates with the new secret while the
/// authenticated sockets stay open. Changed endpoints need a new multiplexer; it is opened first, published through
/// <see cref="Replaced"/>, and the previous one is disposed after a grace period so commands in flight complete.
/// </remarks>
internal sealed class RedisConnection : IRedisConnection, IDisposable
{
    private static readonly TimeSpan ReplacementGrace = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan RetryBackoff = TimeSpan.FromSeconds(5);

    private readonly IOptionsFactory<RedisOptions> _factory;
    private readonly ILogger<RedisConnection> _logger;
    private readonly IDisposable[] _changeSubscriptions;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Lock _state = new();
    private ConfigurationOptions _configuration;
    private IConnectionMultiplexer? _multiplexer;
    private Task<IConnectionMultiplexer>? _connected;
    private Exception? _lastFailure;
    private long _failedAt;
    private bool _isDisposed;

    /// <remarks>
    /// Reloads are watched through the change token sources rather than <see cref="IOptionsMonitor{TOptions}"/>:
    /// the monitor validates the reloaded options before any callback runs and throws on the thread of the
    /// configuration provider, where nothing catches and a file watcher's unhandled exception ends the process.
    /// Creating the options here keeps that validation inside a try block.
    /// </remarks>
    public RedisConnection(
        IOptions<RedisOptions> options,
        IOptionsFactory<RedisOptions> factory,
        IEnumerable<IOptionsChangeTokenSource<RedisOptions>> changeSources,
        ILogger<RedisConnection> logger)
    {
        _factory = factory;
        _logger = logger;
        _configuration = options.Value.ToConfigurationOptions();
        _changeSubscriptions = [.. changeSources.Select(source => ChangeToken.OnChange(source.GetChangeToken, OnOptionsChanged))];
    }

    /// <inheritdoc />
    public event Action<IConnectionMultiplexer>? Replaced;

    /// <inheritdoc />
    public ConfigurationOptions Configuration => _configuration;

    /// <inheritdoc />
    /// <remarks>
    /// Only the <see cref="IConnectionMultiplexer"/> registered for consumers comes this way; the library's own
    /// components await <see cref="ConnectAsync"/>, so a Redis that does not answer holds none of their threads.
    /// </remarks>
    public IConnectionMultiplexer Multiplexer => Volatile.Read(ref _multiplexer) ?? Connect();

    /// <inheritdoc />
    public IConnectionMultiplexer? Opened => Volatile.Read(ref _multiplexer);

    /// <inheritdoc />
    public Task<IConnectionMultiplexer> ConnectAsync(CancellationToken cancellationToken) =>
        Volatile.Read(ref _connected) ?? OpenAsync(cancellationToken);

    /// <remarks>
    /// A connect may still be in flight: it finds the connection disposed when it completes and closes what it opened,
    /// so no multiplexer and its reconnect loop outlive the container. Its caller gets an ObjectDisposedException.
    /// </remarks>
    public void Dispose()
    {
        IConnectionMultiplexer? held;

        lock (_state)
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            held = _multiplexer;
            Volatile.Write(ref _multiplexer, null);
            Volatile.Write(ref _connected, null);
            _gate.Dispose();
        }

        foreach (var subscription in _changeSubscriptions)
            subscription.Dispose();

        held?.Dispose();
    }

    /// <summary>Opens the multiplexer once; concurrent callers share the same attempt.</summary>
    private async Task<IConnectionMultiplexer> OpenAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_multiplexer is { } opened)
                return opened;

            ThrowIfRecentlyRefused();

            try
            {
                return Hold(Observed(await ConnectionMultiplexer.ConnectAsync(_configuration).ConfigureAwait(false)));
            }
            catch (RedisConnectionException exception)
            {
                Refused(exception);
                throw;
            }
        }
        finally
        {
            Leave();
        }
    }

    /// <remarks>
    /// With AbortOnConnectFail a refused connect throws instead of returning a reconnecting multiplexer, and every
    /// caller would repeat the blocking attempt; a refusal is therefore remembered for the back-off.
    /// </remarks>
    private IConnectionMultiplexer Connect()
    {
        _gate.Wait();

        try
        {
            if (_multiplexer is { } opened)
                return opened;

            ThrowIfRecentlyRefused();

            try
            {
                return Hold(Observed(ConnectionMultiplexer.Connect(_configuration)));
            }
            catch (RedisConnectionException exception)
            {
                Refused(exception);
                throw;
            }
        }
        finally
        {
            Leave();
        }
    }

    private void ThrowIfRecentlyRefused()
    {
        if (_lastFailure is not { } failure || Environment.TickCount64 - _failedAt >= RetryBackoff.TotalMilliseconds)
            return;

        throw new RedisConnectionException(
            ConnectionFailureType.UnableToConnect,
            $"Redis refused the connection less than {RetryBackoff.TotalSeconds:F0} seconds ago; the next attempt waits for the back-off.",
            failure);
    }

    private void Refused(Exception exception)
    {
        _lastFailure = exception;
        _failedAt = Environment.TickCount64;
    }

    /// <summary>Makes the multiplexer the one in use, with the completed task <see cref="ConnectAsync"/> answers from then on; a connection disposed meanwhile closes it instead.</summary>
    private IConnectionMultiplexer Hold(IConnectionMultiplexer multiplexer)
    {
        lock (_state)
        {
            if (!_isDisposed)
            {
                Volatile.Write(ref _connected, Task.FromResult(multiplexer));
                Volatile.Write(ref _multiplexer, multiplexer);

                return multiplexer;
            }
        }

        multiplexer.Dispose();

        throw new ObjectDisposedException(nameof(RedisConnection), "The connection was disposed while a multiplexer was opening; that multiplexer was closed.");
    }

    /// <summary>Releases the gate unless the connection was disposed while it was held, which disposed the gate as well.</summary>
    private void Leave()
    {
        lock (_state)
        {
            if (!_isDisposed)
                _gate.Release();
        }
    }

    /// <remarks>The same servers listed in another order are the same connection; only a server added or removed asks for a new one.</remarks>
    private static bool IsSameServerSet(EndPointCollection first, EndPointCollection second) =>
        first.Count == second.Count && first.All(second.Contains);

    private IConnectionMultiplexer Observed(IConnectionMultiplexer multiplexer)
    {
        multiplexer.ConnectionFailed += (_, args) =>
        {
            RedisMetrics.ConnectionFailures.Add(1);
            _logger.ConnectionFailed(args.Exception, args.EndPoint?.ToString(), args.FailureType.ToString());
        };
        multiplexer.ConnectionRestored += (_, args) =>
        {
            RedisMetrics.ConnectionRestores.Add(1);
            _logger.ConnectionRestored(args.EndPoint?.ToString());
        };
        multiplexer.ErrorMessage += (_, args) => _logger.ServerError(args.EndPoint?.ToString(), args.Message);

        return multiplexer;
    }

    private void OnOptionsChanged()
    {
        ConfigurationOptions next;

        try
        {
            next = _factory.Create(Options.DefaultName).ToConfigurationOptions();
        }
        catch (OptionsValidationException exception)
        {
            _logger.InvalidReload(exception);
            return;
        }

        if (!IsSameServerSet(next.EndPoints, _configuration.EndPoints))
        {
            _ = ReplaceAsync(next);
            return;
        }

        Rotate(next);
    }

    private void Rotate(ConfigurationOptions next)
    {
        if (string.Equals(_configuration.Password, next.Password, StringComparison.Ordinal))
            return;

        _configuration.Password = next.Password;
        _logger.PasswordRotated();
    }

    /// <remarks>
    /// A provider may raise one reload twice, and the second arrives while the first replacement is still connecting:
    /// under the gate it finds the endpoints already moved and only rotates the password. A container disposed while
    /// the replacement is in flight disposes the gate as well; that is the end of the connection, not an error.
    /// </remarks>
    private async Task ReplaceAsync(ConfigurationOptions next)
    {
        IConnectionMultiplexer? previous;
        IConnectionMultiplexer replacement;

        try
        {
            await _gate.WaitAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            return;
        }

        try
        {
            previous = _multiplexer;

            if (previous is null)
            {
                _configuration = next;
                return;
            }

            if (IsSameServerSet(next.EndPoints, _configuration.EndPoints))
            {
                Rotate(next);
                return;
            }

            replacement = Observed(await ConnectionMultiplexer.ConnectAsync(next).ConfigureAwait(false));
            _configuration = next;
            Hold(replacement);
        }
        catch (ObjectDisposedException)
        {
            return;
        }
        catch (Exception exception)
        {
            _logger.ReplacementFailed(exception, next.ToString());
            return;
        }
        finally
        {
            Leave();
        }

        RedisMetrics.ConnectionReplacements.Add(1);
        _logger.EndpointsReplaced(replacement.Configuration);
        Replaced?.Invoke(replacement);

        await Task.Delay(ReplacementGrace).ConfigureAwait(false);
        previous.Dispose();
    }
}
