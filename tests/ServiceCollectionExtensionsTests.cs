using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.Cache;
using Snail.Toolkit.Redis.Extensions;
using Snail.Toolkit.Redis.PubSub;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddRedisCache_RegistersCacheAndConnection()
    {
        var services = new ServiceCollection().AddRedisCache(TestConfiguration.Get());

        Assert.Contains(services, d => d.ServiceType == typeof(IRedisCache));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IDistributedCache));
        Assert.Contains(services, d => d.ServiceType == typeof(IConnectionMultiplexer));
    }

    [Fact]
    public void AddRedisDistributedCache_CalledTwice_RegistersMicrosoftCacheOnce()
    {
        var services = new ServiceCollection()
            .AddRedisDistributedCache(TestConfiguration.Get())
            .AddRedisDistributedCache(TestConfiguration.Get());

        Assert.Single(services, d => d.ServiceType == typeof(IDistributedCache));
        Assert.Single(services, d => d.ServiceType == typeof(IConnectionMultiplexer));
    }

    [Fact]
    public void AddRedisPubSub_RegistersPubSubAndConnection()
    {
        var services = new ServiceCollection().AddRedisPubSub(TestConfiguration.Get());

        Assert.Contains(services, d => d.ServiceType == typeof(IRedisPubSub));
        Assert.Contains(services, d => d.ServiceType == typeof(IConnectionMultiplexer));
    }

    [Fact]
    public void AddRedisCacheAndPubSub_ShareOneConnectionRegistration()
    {
        var services = new ServiceCollection()
            .AddRedisCache(TestConfiguration.Get())
            .AddRedisPubSub(TestConfiguration.Get());

        Assert.Single(services, d => d.ServiceType == typeof(IConnectionMultiplexer));
        Assert.Single(services, d => d.ServiceType == typeof(IHostedService));
        Assert.Single(services, d => d.ServiceType == typeof(IRedisCache));
        Assert.Single(services, d => d.ServiceType == typeof(IRedisPubSub));
    }

    [Fact]
    public void AddRedisHealthCheck_RegistersOneNamedCheck()
    {
        var configuration = TestConfiguration.Get();
        var services = new ServiceCollection()
            .AddRedisHealthCheck(configuration, tags: ["ready"])
            .AddRedisHealthCheck(configuration);

        using var provider = services.BuildServiceProvider();
        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;

        var registration = Assert.Single(registrations);
        Assert.Equal("redis", registration.Name);
        Assert.Equal(HealthStatus.Unhealthy, registration.FailureStatus);
        Assert.Contains("ready", registration.Tags);
        Assert.Single(services, d => d.ServiceType == typeof(IConnectionMultiplexer));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("   ")]
    [InlineData("localhost:6379,bogus=1")]
    [InlineData("localhost:6379,password=secret")]
    [InlineData("localhost:6379,abortConnect=true")]
    public void AddRedis_WithInvalidConnection_FailsValidation(string? connection)
    {
        using var provider = new ServiceCollection()
            .AddRedis(TestConfiguration.Get(connection))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<RedisOptions>>().Value);
    }

    [Fact]
    public void AddRedis_CalledTwice_KeepsTheFirstConfiguration()
    {
        var services = new ServiceCollection()
            .AddRedis(TestConfiguration.Get("host-a:6379"))
            .AddRedis(TestConfiguration.Get("host-b:6379"));

        using var provider = services.BuildServiceProvider();

        Assert.Equal("host-a:6379", provider.GetRequiredService<IOptions<RedisOptions>>().Value.Connection);
        Assert.Single(services, d => d.ServiceType == typeof(IConnectionMultiplexer));
    }

    [Fact]
    public void AddRedisPubSub_WithZeroBuffer_FailsValidation()
    {
        using var provider = new ServiceCollection()
            .AddRedisPubSub(TestConfiguration.Get(), o => o.BufferCapacity = 0)
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<RedisPubSubOptions>>().Value);
    }

    [Fact]
    public void AddRedisCache_WithNegativeExpiration_FailsValidation()
    {
        using var provider = new ServiceCollection()
            .AddRedisCache(TestConfiguration.Get(), o => o.SlidingExpiration = TimeSpan.FromSeconds(-1))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<RedisCacheOptions>>().Value);
    }

    [Fact]
    public void AddRedis_BindsOptions()
    {
        var services = new ServiceCollection()
            .AddRedis(TestConfiguration.Get("redis-host:6380", "secret", "app"));

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<RedisOptions>>().Value;

        Assert.Equal("redis-host:6380", options.Connection);
        Assert.Equal("secret", options.Password);
        Assert.Equal("app", options.InstanceName);
        Assert.False(options.AbortOnConnectFail);

        var configuration = options.ToConfigurationOptions();
        Assert.Equal("secret", configuration.Password);
        Assert.False(configuration.AbortOnConnectFail);
        Assert.Single(configuration.EndPoints);
    }

    [Fact]
    public void Resolve_CacheAndPubSub_UseSharedConnection()
    {
        var connection = Mock.Of<IConnectionMultiplexer>();
        var configuration = TestConfiguration.Get(instanceName: "app");
        var services = new ServiceCollection()
            .AddLogging()
            .AddRedisCache(configuration, o => o.SlidingExpiration = TimeSpan.FromMinutes(5))
            .AddRedisPubSub(configuration, o => o.BufferCapacity = 8);
        services.Replace(ServiceDescriptor.Singleton(connection));

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<IRedisCache>());
        Assert.NotNull(provider.GetRequiredService<IRedisPubSub>());
        Assert.Same(connection, provider.GetRequiredService<IConnectionMultiplexer>());
        Assert.Equal(8, provider.GetRequiredService<IOptions<RedisPubSubOptions>>().Value.BufferCapacity);
        Assert.Equal(TimeSpan.FromMinutes(5), provider.GetRequiredService<IOptions<RedisCacheOptions>>().Value.SlidingExpiration);
    }
}
