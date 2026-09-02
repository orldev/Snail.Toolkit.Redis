using Microsoft.Extensions.Configuration;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>In-memory configuration whose values can be changed at runtime, triggering a reload.</summary>
public sealed class MutableConfiguration : ConfigurationProvider, IConfigurationSource
{
    public MutableConfiguration(IDictionary<string, string?> initial)
    {
        foreach (var (key, value) in initial)
            Data[key] = value;
    }

    public IConfigurationProvider Build(IConfigurationBuilder builder) => this;

    public override void Set(string key, string? value)
    {
        Data[key] = value;
        OnReload();
    }

    public static (IConfiguration Configuration, MutableConfiguration Source) Create(IDictionary<string, string?> initial)
    {
        var source = new MutableConfiguration(initial);
        var configuration = new ConfigurationBuilder().Add(source).Build();
        return (configuration, source);
    }
}
