using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
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

        Assert.Contains(services, d => d.ServiceType == typeof(ICacheService));
        Assert.Contains(services, d => d.ServiceType == typeof(IDistributedCache));
        Assert.Contains(services, d => d.ServiceType == typeof(IConnectionMultiplexer));
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
        Assert.Single(services, d => d.ServiceType == typeof(ICacheService));
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

    [Fact]
    public async Task PublishAsync_NullMessage_Throws()
    {
        var pubSub = new RedisPubSub(Mock.Of<IConnectionMultiplexer>(), Options.Create(new RedisPubSubOptions()), NullLogger<RedisPubSub>.Instance);

        await Assert.ThrowsAsync<ArgumentNullException>(() => pubSub.PublishAsync<object>("channel", null!));
        await Assert.ThrowsAsync<ArgumentException>(() => pubSub.PublishAsync(" ", new object()));
        await Assert.ThrowsAsync<ArgumentException>(() => pubSub.SubscribeAsync<object>(""));
    }

    [Fact]
    public void AddRedis_WithoutConnection_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<InvalidOperationException>(() => services.AddRedis(TestConfiguration.Get(connection: null)));
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
    public async Task Resolve_CacheAndPubSub_UseSharedConnection()
    {
        var connection = Mock.Of<IConnectionMultiplexer>();
        var configuration = TestConfiguration.Get(instanceName: "app");
        var services = new ServiceCollection()
            .AddLogging()
            .AddRedisCache(configuration, o => o.SlidingExpiration = TimeSpan.FromMinutes(5))
            .AddRedisPubSub(configuration, o => o.BufferCapacity = 8);
        services.Replace(ServiceDescriptor.Singleton(connection));

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<ICacheService>());
        Assert.NotNull(provider.GetRequiredService<IRedisPubSub>());
        Assert.Same(connection, provider.GetRequiredService<IConnectionMultiplexer>());

        var cacheOptions = provider.GetRequiredService<IOptions<RedisCacheOptions>>().Value;
        Assert.Equal("app", cacheOptions.InstanceName);
        Assert.NotNull(cacheOptions.ConnectionMultiplexerFactory);
        Assert.Same(connection, await cacheOptions.ConnectionMultiplexerFactory!());

        Assert.Equal(8, provider.GetRequiredService<IOptions<RedisPubSubOptions>>().Value.BufferCapacity);
        Assert.Equal(TimeSpan.FromMinutes(5), provider.GetRequiredService<IOptions<CacheServiceOptions>>().Value.SlidingExpiration);
    }
}
