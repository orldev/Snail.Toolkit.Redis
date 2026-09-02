using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.Cache;
using Snail.Toolkit.Redis.Health;
using Snail.Toolkit.Redis.PubSub;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Extensions;

/// <summary>Registers the Redis connection, the distributed cache and pub/sub on top of one shared multiplexer.</summary>
public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        /// <summary>Binds <see cref="RedisOptions"/> from the "Redis" section, registers a singleton <see cref="IConnectionMultiplexer"/> (password rotation applies to reconnects) and a start-up warm-up; repeated calls must pass the same configuration, every call re-binds the options.</summary>
        public IServiceCollection AddRedis(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(configuration);

            var section = configuration.GetSection(RedisOptions.SectionName);

            if (string.IsNullOrWhiteSpace(section[nameof(RedisOptions.Connection)]))
                throw new InvalidOperationException(
                    $"Configuration key '{RedisOptions.SectionName}:{nameof(RedisOptions.Connection)}' is required.");

            services.AddOptions<RedisOptions>().Bind(section);
            services.TryAddSingleton<RedisConnection>();
            services.TryAddSingleton<IConnectionMultiplexer>(provider => provider.GetRequiredService<RedisConnection>().Multiplexer);
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RedisConnectionWarmup>());

            return services;
        }

        /// <summary>Registers a health check that PINGs the shared Redis connection; the name is registered once even when called repeatedly.</summary>
        public IServiceCollection AddRedisHealthCheck(IConfiguration configuration,
            string name = "redis",
            HealthStatus failureStatus = HealthStatus.Unhealthy,
            IEnumerable<string>? tags = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);

            services.AddRedis(configuration);
            services.AddHealthChecks();
            services.Configure<HealthCheckServiceOptions>(options =>
            {
                if (options.Registrations.Any(registration => registration.Name == name))
                    return;

                options.Registrations.Add(new HealthCheckRegistration(
                    name,
                    provider => new RedisHealthCheck(provider.GetRequiredService<IConnectionMultiplexer>()),
                    failureStatus,
                    tags));
            });

            return services;
        }

        /// <summary>Registers the distributed cache and <see cref="ICacheService"/> over the shared Redis connection.</summary>
        public IServiceCollection AddRedisCache(IConfiguration configuration,
            Action<CacheServiceOptions>? configure = null)
        {
            services.AddRedis(configuration);
            services.AddStackExchangeRedisCache(_ => { });
            services.AddOptions<RedisCacheOptions>()
                .Configure<IConnectionMultiplexer, IOptions<RedisOptions>>((cache, connection, redis) =>
                {
                    cache.ConnectionMultiplexerFactory = () => Task.FromResult(connection);
                    cache.InstanceName = redis.Value.InstanceName;
                });

            var builder = services.AddOptions<CacheServiceOptions>();

            if (configure is not null)
                builder.Configure(configure);

            services.TryAddSingleton<ICacheService, CacheService>();

            return services;
        }

        /// <summary>Registers <see cref="IRedisPubSub"/> over the shared Redis connection.</summary>
        public IServiceCollection AddRedisPubSub(IConfiguration configuration,
            Action<RedisPubSubOptions>? configure = null)
        {
            services.AddRedis(configuration);

            var builder = services.AddOptions<RedisPubSubOptions>();

            if (configure is not null)
                builder.Configure(configure);

            services.TryAddSingleton<IRedisPubSub, RedisPubSub>();

            return services;
        }
    }
}
