using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis;

/// <summary>Owns the shared multiplexer and applies a rotated password from <see cref="RedisOptions"/> to future reconnects.</summary>
internal sealed class RedisConnection : IDisposable
{
    private readonly ConfigurationOptions _configuration;
    private readonly ILogger<RedisConnection> _logger;
    private readonly IDisposable? _changeSubscription;

    public RedisConnection(IOptionsMonitor<RedisOptions> options, ILogger<RedisConnection> logger)
    {
        _logger = logger;
        _configuration = options.CurrentValue.ToConfigurationOptions();
        Multiplexer = ConnectionMultiplexer.Connect(_configuration);
        _changeSubscription = options.OnChange(OnOptionsChanged);
    }

    public IConnectionMultiplexer Multiplexer { get; }

    public void Dispose()
    {
        _changeSubscription?.Dispose();
        Multiplexer.Dispose();
    }

    private void OnOptionsChanged(RedisOptions options)
    {
        var next = options.ToConfigurationOptions();

        if (!string.Equals(_configuration.Password, next.Password, StringComparison.Ordinal))
        {
            _configuration.Password = next.Password;
            _logger.LogInformation("Redis password rotated; reconnects will use the new credentials");
        }

        if (!next.EndPoints.SequenceEqual(_configuration.EndPoints))
            _logger.LogWarning("Redis endpoints changed in configuration; a restart is required to apply them");
    }
}
