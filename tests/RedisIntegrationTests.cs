using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Snail.Toolkit.Redis.Cache;
using Snail.Toolkit.Redis.Extensions;
using Snail.Toolkit.Redis.PubSub;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

public class RedisIntegrationTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    public sealed record Message(string Text, int Number);

    private async Task<ServiceProvider> BuildProviderAsync()
    {
        var configuration = TestConfiguration.Get(await fixture.GetConnectionStringAsync(), password: null, instanceName: "tests:");

        return new ServiceCollection()
            .AddLogging()
            .AddRedisCache(configuration)
            .AddRedisPubSub(configuration)
            .AddRedisHealthCheck(configuration)
            .BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task Cache_SetAndGet_RoundTrips()
    {
        await using var provider = await BuildProviderAsync();
        var cache = provider.GetRequiredService<IRedisCache>();
        var key = Guid.NewGuid().ToString();

        await cache.SetAsync(key, new Message("hello", 1));
        var value = await cache.GetAsync<Message>(key);

        Assert.Equal(new Message("hello", 1), value);

        await cache.RemoveAsync(key);
        Assert.Null(await cache.GetAsync<Message>(key));
    }

    [IntegrationFact]
    public async Task Cache_GetWithFactory_StoresValueUnderInstanceName()
    {
        await using var provider = await BuildProviderAsync();
        var cache = provider.GetRequiredService<IRedisCache>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var key = Guid.NewGuid().ToString();

        var value = await cache.GetAsync(key, _ => Task.FromResult<Message?>(new Message("factory", 2)));

        Assert.Equal(new Message("factory", 2), value);
        Assert.True(await connection.GetDatabase().KeyExistsAsync("tests:" + key));
        Assert.Equal(new Message("factory", 2), await cache.GetAsync<Message>(key));
    }

    [IntegrationFact]
    public async Task Cache_SlidingEntry_ExpiresWhenNotRead()
    {
        await using var provider = await BuildProviderAsync();
        var cache = provider.GetRequiredService<IRedisCache>();
        var key = Guid.NewGuid().ToString();

        await cache.SetAsync(key, new Message("short", 1), new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMilliseconds(300) });
        await Task.Delay(150);
        Assert.NotNull(await cache.GetAsync<Message>(key));
        await Task.Delay(500);

        Assert.Null(await cache.GetAsync<Message>(key));
    }

    [IntegrationFact]
    public async Task DistributedCache_EntriesAreReadableThroughRedisCacheAndItsDisposalKeepsTheSharedConnection()
    {
        var configuration = TestConfiguration.Get(await fixture.GetConnectionStringAsync(), password: null, instanceName: "tests:");
        await using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedisCache(configuration)
            .AddRedisDistributedCache(configuration)
            .BuildServiceProvider();
        var distributed = provider.GetRequiredService<IDistributedCache>();
        var cache = provider.GetRequiredService<IRedisCache>();
        var key = Guid.NewGuid().ToString();

        await distributed.SetAsync(key,
            JsonSerializer.SerializeToUtf8Bytes(new Message("shared", 5), JsonSerializerOptions.Web),
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(1) });
        await cache.SetAsync(key + ":back", new Message("back", 6));

        Assert.Equal(new Message("shared", 5), await cache.GetAsync<Message>(key));
        Assert.Equal(new Message("back", 6), JsonSerializer.Deserialize<Message>((await distributed.GetAsync(key + ":back"))!, JsonSerializerOptions.Web));

        (distributed as IDisposable)?.Dispose();

        Assert.True(provider.GetRequiredService<IConnectionMultiplexer>().IsConnected);
        Assert.Equal(new Message("shared", 5), await cache.GetAsync<Message>(key));
    }

    [IntegrationFact]
    public async Task PubSub_PublishedMessages_ArriveInOrder()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "task:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var received = new List<Message>();
        var consumer = Task.Run(async () =>
        {
            await foreach (var message in pubSub.StreamAsync<Message>(channel, cts.Token))
            {
                received.Add(message);
                if (received.Count == 3)
                    break;
            }
        }, cts.Token);

        await WaitForSubscribersAsync(connection, channel, expected: 1);
        for (var i = 1; i <= 3; i++)
            await pubSub.PublishAsync(channel, new Message("m", i));

        await consumer;

        Assert.Equal([1, 2, 3], received.Select(m => m.Number));
        await WaitForSubscribersAsync(connection, channel, expected: 0);
    }

    [IntegrationFact]
    public async Task PubSub_TwoReadersOfOneChannel_ShareOneRedisSubscription()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "task:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var first = await pubSub.SubscribeAsync<Message>(channel, cts.Token);
        var second = await pubSub.SubscribeAsync<Message>(channel, cts.Token);
        await WaitForSubscribersAsync(connection, channel, expected: 1);
        await pubSub.PublishAsync(channel, new Message("both", 1), cts.Token);

        Assert.Equal(1, (await FirstAsync(first.ReadAllAsync(cts.Token))).Message.Number);
        Assert.Equal(1, (await FirstAsync(second.ReadAllAsync(cts.Token))).Message.Number);

        await first.DisposeAsync();
        await WaitForSubscribersAsync(connection, channel, expected: 1);
        await second.DisposeAsync();
        await WaitForSubscribersAsync(connection, channel, expected: 0);
    }

    [IntegrationFact]
    public async Task PubSub_CancelledToken_UnsubscribesAndCompletesStream()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "task:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource();

        var consumer = Task.Run(async () =>
        {
            var count = 0;
            await foreach (var _ in pubSub.StreamAsync<Message>(channel, cts.Token))
                count++;
            return count;
        });

        await WaitForSubscribersAsync(connection, channel, expected: 1);
        cts.Cancel();

        var count = await consumer.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(0, count);
        await WaitForSubscribersAsync(connection, channel, expected: 0);
    }

    [IntegrationFact]
    public async Task PubSub_MalformedPayload_IsSkipped()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "task:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var consumer = Task.Run(async () =>
        {
            await foreach (var message in pubSub.StreamAsync<Message>(channel, cts.Token))
                return message;
            return null;
        }, cts.Token);

        await WaitForSubscribersAsync(connection, channel, expected: 1);
        await connection.GetSubscriber().PublishAsync(RedisChannel.Literal(channel), "not json");
        await pubSub.PublishAsync(channel, new Message("valid", 7));

        Assert.Equal(new Message("valid", 7), await consumer);
    }

    [IntegrationFact]
    public async Task PubSub_SubscribeAsync_IsActiveWhenItReturns()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var channel = "task:" + Guid.NewGuid() + ":events";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using var subscription = await pubSub.SubscribeAsync<Message>(channel, cts.Token);
        var receivers = await pubSub.PublishAsync(channel, new Message("first", 1), cts.Token);

        var received = await FirstAsync(subscription.ReadAllAsync(cts.Token));

        Assert.Equal(1, receivers);
        Assert.Equal(channel, subscription.Channel);
        Assert.Equal(channel, received.Channel);
        Assert.Equal(new Message("first", 1), received.Message);
    }

    [IntegrationFact]
    public async Task PubSub_PublishWithoutSubscribers_ReportsZeroReceivers()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();

        var receivers = await pubSub.PublishAsync("task:" + Guid.NewGuid(), new Message("nobody", 0));

        Assert.Equal(0, receivers);
    }

    [IntegrationFact]
    public async Task PubSub_PatternSubscription_YieldsChannelNames()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var prefix = "task-" + Guid.NewGuid() + ":";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using var subscription = await pubSub.SubscribePatternAsync<Message>(prefix + "*:events", cts.Token);
        await pubSub.PublishAsync(prefix + "a:events", new Message("a", 1), cts.Token);
        await pubSub.PublishAsync(prefix + "a:deltas", new Message("ignored", 0), cts.Token);
        await pubSub.PublishAsync(prefix + "b:events", new Message("b", 2), cts.Token);

        var received = new List<RedisMessage<Message>>();
        await foreach (var message in subscription.ReadAllAsync(cts.Token))
        {
            received.Add(message);
            if (received.Count == 2)
                break;
        }

        Assert.Equal([prefix + "a:events", prefix + "b:events"], received.Select(m => m.Channel));
        Assert.Equal([1, 2], received.Select(m => m.Message.Number));
    }

    [IntegrationFact]
    public async Task PubSub_DisposeSubscription_UnsubscribesAndCompletesReader()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "task:" + Guid.NewGuid() + ":events";

        var subscription = await pubSub.SubscribeAsync<Message>(channel);
        await WaitForSubscribersAsync(connection, channel, expected: 1);
        var reader = Task.Run(async () =>
        {
            var count = 0;
            await foreach (var _ in subscription.ReadAllAsync())
                count++;
            return count;
        });

        await subscription.DisposeAsync();

        Assert.Equal(0, await reader.WaitAsync(TimeSpan.FromSeconds(10)));
        await WaitForSubscribersAsync(connection, channel, expected: 0);
    }

    [IntegrationFact]
    public async Task PubSub_DisposeSubscriptionAfterConnectionClosed_CompletesReaderWithoutThrowing()
    {
        var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var subscription = await pubSub.SubscribeAsync<Message>("task:" + Guid.NewGuid());
        var reader = Task.Run(async () =>
        {
            var count = 0;
            await foreach (var _ in subscription.ReadAllAsync())
                count++;
            return count;
        });

        await provider.DisposeAsync();
        await subscription.DisposeAsync();

        Assert.Equal(0, await reader.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [IntegrationFact]
    public async Task Warmup_OpensConnectionAtHostStart()
    {
        await using var provider = await BuildProviderAsync();

        foreach (var service in provider.GetServices<IHostedService>())
            await service.StartAsync(CancellationToken.None);

        Assert.True(provider.GetRequiredService<IConnectionMultiplexer>().IsConnected);
    }

    [IntegrationFact]
    public async Task HealthCheck_ReportsHealthyWithLatency()
    {
        await using var provider = await BuildProviderAsync();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        Assert.Equal(HealthStatus.Healthy, report.Status);
        var entry = report.Entries["redis"];
        Assert.StartsWith("PING", entry.Description);
        Assert.True((double)entry.Data["latencyMs"] >= 0);
    }

    [IntegrationFact]
    public async Task HealthCheck_ReportsUnhealthyWhenRedisIsDown()
    {
        var configuration = TestConfiguration.Get("localhost:1,connectTimeout=500", password: null);
        await using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedisHealthCheck(configuration, failureStatus: HealthStatus.Degraded)
            .BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        Assert.Equal(HealthStatus.Degraded, report.Status);
    }

    private static async Task<T> FirstAsync<T>(IAsyncEnumerable<T> source)
    {
        await foreach (var item in source)
            return item;

        throw new InvalidOperationException("The stream completed without a message.");
    }

    private static async Task WaitForSubscribersAsync(IConnectionMultiplexer connection, string channel, int expected)
    {
        var server = connection.GetServer(connection.GetEndPoints()[0]);
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (DateTime.UtcNow < deadline)
        {
            var result = (RedisResult[])(await server.ExecuteAsync("PUBSUB", "NUMSUB", channel))!;
            if ((long)result[1] == expected)
                return;

            await Task.Delay(50);
        }

        throw new TimeoutException($"Channel '{channel}' did not reach {expected} subscriber(s) in time.");
    }
}
