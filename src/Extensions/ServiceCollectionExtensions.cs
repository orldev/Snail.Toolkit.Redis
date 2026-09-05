using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.Cache;
using Snail.Toolkit.Redis.Health;
using Snail.Toolkit.Redis.PubSub;
using Snail.Toolkit.Redis.Streams;
using StackExchange.Redis;
using DistributedCacheOptions = Microsoft.Extensions.Caching.StackExchangeRedis.RedisCacheOptions;

namespace Snail.Toolkit.Redis.Extensions;

/// <summary>Registers the Redis connection, the cache, pub/sub, streams and the health check on top of one shared multiplexer.</summary>
public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        /// <summary>Binds <see cref="RedisOptions"/> from the "Redis" section, registers a singleton <see cref="IConnectionMultiplexer"/> and a start-up warm-up; the first call wins and later calls are ignored.</summary>
        /// <remarks>
        /// A rotated password applies to every later reconnect. Changed endpoints open a new connection that the
        /// library's own cache, pub/sub and health check follow; the <see cref="IConnectionMultiplexer"/> a consumer
        /// resolved itself stays on the old endpoints, since a singleton cannot be swapped underneath its holders.
        /// </remarks>
        public IServiceCollection AddRedis(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(configuration);

            if (services.Any(descriptor => descriptor.ServiceType == typeof(RedisConnection)))
                return services;

            Validated(services.AddOptions<RedisOptions>().Bind(configuration.GetSection(RedisOptions.SectionName)));
            services.AddSingleton<RedisConnection>();
            services.AddSingleton<IRedisConnection>(provider => provider.GetRequiredService<RedisConnection>());
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
                    provider => new RedisHealthCheck(provider.GetRequiredService<IRedisConnection>()),
                    failureStatus,
                    tags));
            });

            return services;
        }

        /// <summary>Registers <see cref="IRedisCache"/> over the shared Redis connection.</summary>
        public IServiceCollection AddRedisCache(IConfiguration configuration,
            Action<RedisCacheOptions>? configure = null)
        {
            services.AddRedis(configuration);

            var builder = services.AddOptions<RedisCacheOptions>();

            if (configure is not null)
                builder.Configure(configure);

            Validated(builder);
            services.TryAddSingleton<IRedisCache, RedisCache>();

            return services;
        }

        /// <summary>Registers Microsoft's <see cref="IDistributedCache"/> over Redis for consumers that need that abstraction, on a connection of its own.</summary>
        /// <remarks>
        /// Microsoft's cache closes and disposes the multiplexer it is given, so it never receives the shared one. Its
        /// own connection opens from the same live <see cref="ConfigurationOptions"/>, which keeps password rotation
        /// working for it, and the entry layout matches <see cref="IRedisCache"/>, so each reads the other's entries.
        /// </remarks>
        public IServiceCollection AddRedisDistributedCache(IConfiguration configuration)
        {
            services.AddRedis(configuration);

            if (services.Any(descriptor => descriptor.ServiceType == typeof(IDistributedCache) && descriptor.ImplementationType?.Namespace == typeof(DistributedCacheOptions).Namespace))
                return services;

            services.AddStackExchangeRedisCache(_ => { });
            services.AddOptions<DistributedCacheOptions>()
                .Configure<RedisConnection, IOptions<RedisOptions>>((cache, connection, redis) =>
                {
                    cache.ConnectionMultiplexerFactory = async () => await ConnectionMultiplexer.ConnectAsync(connection.Configuration).ConfigureAwait(false);
                    cache.InstanceName = redis.Value.InstanceName;
                });

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

            Validated(builder);
            services.TryAddSingleton<IRedisPubSub, RedisPubSub>();

            return services;
        }

        /// <summary>Registers <see cref="IRedisStream"/> over the shared Redis connection; appends are announced through pub/sub.</summary>
        public IServiceCollection AddRedisStreams(IConfiguration configuration,
            Action<RedisStreamOptions>? configure = null)
        {
            services.AddRedisPubSub(configuration);

            var builder = services.AddOptions<RedisStreamOptions>();

            if (configure is not null)
                builder.Configure(configure);

            Validated(builder);
            services.TryAddSingleton<IRedisStream, RedisStreams>();

            return services;
        }
    }

    /// <summary>The one place the connection settings are judged, at host start rather than at the first command.</summary>
    /// <remarks>
    /// The password and the abort flag are refused inside the connection string because the library applies them
    /// from their own properties: a second copy would silently win or lose depending on the order, and a rotated
    /// password would then be compared against the wrong value. A blank connection is reported once, by the first
    /// rule, so the others pass it through.
    /// </remarks>
    private static void Validated(OptionsBuilder<RedisOptions> builder) =>
        builder
            .Validate(
                settings => !string.IsNullOrWhiteSpace(settings.Connection),
                $"'{RedisOptions.SectionName}:{nameof(RedisOptions.Connection)}' is required.")
            .Validate(
                settings => Parses(settings.Connection),
                $"'{RedisOptions.SectionName}:{nameof(RedisOptions.Connection)}' is not a valid StackExchange.Redis configuration string.")
            .Validate(
                settings => !ParsedPassword(settings.Connection),
                $"'{RedisOptions.SectionName}:{nameof(RedisOptions.Connection)}' must not carry a password; set '{RedisOptions.SectionName}:{nameof(RedisOptions.Password)}' instead.")
            .Validate(
                settings => !Mentions(settings.Connection, "abortConnect"),
                $"'{RedisOptions.SectionName}:{nameof(RedisOptions.Connection)}' must not set abortConnect; set '{RedisOptions.SectionName}:{nameof(RedisOptions.AbortOnConnectFail)}' instead.")
            .ValidateOnStart();

    private static void Validated(OptionsBuilder<RedisCacheOptions> builder) =>
        builder
            .Validate(
                settings => settings.AbsoluteExpirationRelativeToNow is null or { Ticks: > 0 },
                $"{nameof(RedisCacheOptions)}.{nameof(RedisCacheOptions.AbsoluteExpirationRelativeToNow)} has to be positive.")
            .Validate(
                settings => settings.SlidingExpiration is null or { Ticks: > 0 },
                $"{nameof(RedisCacheOptions)}.{nameof(RedisCacheOptions.SlidingExpiration)} has to be positive.")
            .ValidateOnStart();

    private static void Validated(OptionsBuilder<RedisPubSubOptions> builder) =>
        builder
            .Validate(
                settings => settings.BufferCapacity > 0,
                $"{nameof(RedisPubSubOptions)}.{nameof(RedisPubSubOptions.BufferCapacity)} has to be positive.")
            .ValidateOnStart();

    private static void Validated(OptionsBuilder<RedisStreamOptions> builder) =>
        builder
            .Validate(
                settings => settings.PageSize > 0,
                $"{nameof(RedisStreamOptions)}.{nameof(RedisStreamOptions.PageSize)} has to be positive.")
            .Validate(
                settings => settings.PollInterval > TimeSpan.Zero,
                $"{nameof(RedisStreamOptions)}.{nameof(RedisStreamOptions.PollInterval)} has to be positive.")
            .ValidateOnStart();

    private static bool Parses(string? connection)
    {
        if (string.IsNullOrWhiteSpace(connection))
            return true;

        try
        {
            return ConfigurationOptions.Parse(connection).EndPoints.Count > 0;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool ParsedPassword(string? connection) =>
        !string.IsNullOrWhiteSpace(connection) && Parses(connection) && ConfigurationOptions.Parse(connection).Password is not null;

    /// <remarks>StackExchange.Redis trims the name on each side of the equals sign, so the check has to see "abortConnect = true" as well.</remarks>
    private static bool Mentions(string? connection, string keyword) =>
        connection is not null && connection
            .Split(',')
            .Select(token => token.Split('=', 2)[0].Trim())
            .Any(name => name.Equals(keyword, StringComparison.OrdinalIgnoreCase));
}
