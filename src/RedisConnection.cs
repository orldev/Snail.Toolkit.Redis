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
    private ConfigurationOptions _configuration;
    private IConnectionMultiplexer? _multiplexer;
    private Exception? _lastFailure;
    private long _failedAt;

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
    /// The hosted warm-up opens the connection asynchronously at start, so the blocking path only runs for a
    /// consumer that asks for the connection earlier, typically another hosted service registered before it.
    /// </remarks>
    public IConnectionMultiplexer Multiplexer => Volatile.Read(ref _multiplexer) ?? Connect();

    /// <summary>Opens the multiplexer once; concurrent callers share the same attempt.</summary>
    public async Task<IConnectionMultiplexer> ConnectAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _multiplexer) is { } open)
            return open;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_multiplexer is { } opened)
                return opened;

            ThrowIfRecentlyRefused();

            try
            {
                return _multiplexer = Observed(await ConnectionMultiplexer.ConnectAsync(_configuration).ConfigureAwait(false));
            }
            catch (RedisConnectionException exception)
            {
                Refused(exception);
                throw;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        foreach (var subscription in _changeSubscriptions)
            subscription.Dispose();

        _multiplexer?.Dispose();
        _gate.Dispose();
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
                return _multiplexer = Observed(ConnectionMultiplexer.Connect(_configuration));
            }
            catch (RedisConnectionException exception)
            {
                Refused(exception);
                throw;
            }
        }
        finally
        {
            _gate.Release();
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

        if (!next.EndPoints.SequenceEqual(_configuration.EndPoints))
        {
            _ = ReplaceAsync(next);
            return;
        }

        if (string.Equals(_configuration.Password, next.Password, StringComparison.Ordinal))
            return;

        _configuration.Password = next.Password;
        _logger.PasswordRotated();
    }

    /// <remarks>A container disposed while the replacement is in flight disposes the gate as well; that is the end of the connection, not an error.</remarks>
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

            replacement = Observed(await ConnectionMultiplexer.ConnectAsync(next).ConfigureAwait(false));
            _configuration = next;
            _multiplexer = replacement;
        }
        catch (Exception exception)
        {
            _logger.ReplacementFailed(exception, next.ToString());
            return;
        }
        finally
        {
            _gate.Release();
        }

        RedisMetrics.ConnectionReplacements.Add(1);
        _logger.EndpointsReplaced(replacement.Configuration);
        Replaced?.Invoke(replacement);

        await Task.Delay(ReplacementGrace).ConfigureAwait(false);
        previous.Dispose();
    }
}
