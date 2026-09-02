using Microsoft.Extensions.Configuration;

namespace Snail.Toolkit.Redis.Tests;

public static class TestConfiguration
{
    public static IConfiguration Get(
        string? connection = "localhost:6379",
        string? password = "123456789",
        string? instanceName = null)
    {
        var settings = new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = connection,
            [$"{RedisOptions.SectionName}:Password"] = password,
            [$"{RedisOptions.SectionName}:InstanceName"] = instanceName
        };

        return new ConfigurationBuilder()
            .AddInMemoryCollection(settings)
            .Build();
    }
}
