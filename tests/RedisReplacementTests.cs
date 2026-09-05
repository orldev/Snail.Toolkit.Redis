using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Snail.Toolkit.Redis.Cache;
using Snail.Toolkit.Redis.Extensions;
using Snail.Toolkit.Redis.PubSub;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Uses two servers: the configuration moves from the first to the second while the host runs.</summary>
public class RedisReplacementTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    public sealed record Message(string Text, int Number);

    [IntegrationFact]
    public async Task EndpointChange_MovesCacheAndSubscriptionsToTheNewServer()
    {
        var first = await fixture.GetConnectionStringAsync();
        var second = await fixture.StartAnotherAsync();
        var (configuration, source) = MutableConfiguration.Create(new Dictionary<string, string?>
        {
            [$"{RedisOptions.SectionName}:Connection"] = first
        });
        await using var provider = new ServiceCollection()
            .AddLogging()
            .AddRedisCache(configuration)
            .AddRedisPubSub(configuration)
            .BuildServiceProvider();
        foreach (var hosted in provider.GetServices<IHostedService>())
            await hosted.StartAsync(CancellationToken.None);
        var cache = provider.GetRequiredService<IRedisCache>();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IRedisConnection>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var subscription = await pubSub.SubscribeAsync<Message>("moved", cts.Token);
        await cache.SetAsync("before", new Message("first", 1));

        source.Set($"{RedisOptions.SectionName}:Connection", second);
        await using var probe = await ConnectionMultiplexer.ConnectAsync(second);
        await WaitUntilAsync(() => connection.Multiplexer.GetEndPoints()[0].Equals(probe.GetEndPoints()[0]));
        await WaitUntilAsync(() => SubscriberCount(probe, "moved") == 1);
        await cache.SetAsync("after", new Message("second", 2));
        var receivers = await pubSub.PublishAsync("moved", new Message("moved", 3), cts.Token);

        Assert.Null(await cache.GetAsync<Message>("before"));
        Assert.True(await probe.GetDatabase().KeyExistsAsync("after"));
        Assert.Equal(1, receivers);
        await foreach (var received in subscription.ReadAllAsync(cts.Token))
        {
            Assert.Equal(3, received.Message.Number);
            break;
        }
    }

    private static long SubscriberCount(IConnectionMultiplexer connection, string channel)
    {
        var server = connection.GetServer(connection.GetEndPoints()[0]);
        var result = (RedisResult[])server.Execute("PUBSUB", "NUMSUB", channel)!;

        return (long)result[1];
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);

        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The condition was not met in time.");

            await Task.Delay(100);
        }
    }
}
