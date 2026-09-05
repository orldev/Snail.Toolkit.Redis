using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.Extensions;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Connection strings that try to smuggle the settings the library reserves for its own properties.</summary>
public class OptionsValidationCrashTests
{
    [Theory]
    [InlineData("localhost:6379, abortConnect = true")]
    [InlineData("localhost:6379,ABORTCONNECT=false")]
    [InlineData("localhost:6379, password = secret")]
    [InlineData("localhost:6379,PASSWORD=secret")]
    public void AddRedis_SmuggledSetting_FailsValidation(string connection)
    {
        using var provider = new ServiceCollection()
            .AddRedis(TestConfiguration.Get(connection))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<RedisOptions>>().Value);
    }

    [Theory]
    [InlineData("localhost:99999")]
    [InlineData("localhost:6379,connectTimeout=abc")]
    [InlineData("localhost:6379,ssl=maybe")]
    public void AddRedis_UnparsableConnection_FailsValidation(string connection)
    {
        using var provider = new ServiceCollection()
            .AddRedis(TestConfiguration.Get(connection))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<RedisOptions>>().Value);
    }
}
