using StackExchange.Redis;

namespace Snail.Toolkit.Redis;

/// <summary>Connection settings bound from the "Redis" configuration section.</summary>
public sealed class RedisOptions
{
    /// <summary>Name of the configuration section the options are bound from.</summary>
    public const string SectionName = "Redis";

    /// <summary>StackExchange.Redis configuration string, e.g. "localhost:6379" or "host1:6379,host2:6379,ssl=true".</summary>
    public string Connection { get; set; } = string.Empty;

    /// <summary>Optional password applied to the connection.</summary>
    public string? Password { get; set; }

    /// <summary>Optional prefix for cache keys; none by default.</summary>
    public string? InstanceName { get; set; }

    /// <summary>Whether the first connect must succeed; false lets the host start while Redis is down and reconnect later.</summary>
    public bool AbortOnConnectFail { get; set; }

    /// <summary>Builds the StackExchange.Redis options from these settings.</summary>
    public ConfigurationOptions ToConfigurationOptions()
    {
        var options = ConfigurationOptions.Parse(Connection);

        if (!string.IsNullOrEmpty(Password))
            options.Password = Password;

        options.AbortOnConnectFail = AbortOnConnectFail;
        return options;
    }
}
