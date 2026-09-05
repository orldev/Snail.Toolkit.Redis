using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.Redis.Cache;
using Snail.Toolkit.Redis.Extensions;
using Snail.Toolkit.Redis.PubSub;
using Snail.Toolkit.Redis.Streams;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Poisoned keys, storms and hostile names against a live Redis.</summary>
public class RedisCrashIntegrationTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    public sealed record Message(string Text, int Number);

    private readonly List<LogLevel> _logs = [];

    private async Task<ServiceProvider> BuildProviderAsync()
    {
        var configuration = TestConfiguration.Get(await fixture.GetConnectionStringAsync(), password: null, instanceName: "crash:");

        return new ServiceCollection()
            .AddLogging(builder => builder.AddProvider(new ListLoggerProvider(_logs)))
            .AddRedisCache(configuration)
            .AddRedisPubSub(configuration)
            .AddRedisStreams(configuration, o => o.PollInterval = TimeSpan.FromSeconds(5))
            .BuildServiceProvider();
    }

    /// <summary>Somebody stored a plain string under a cache key; HGET answers WRONGTYPE and the cache must read that as an unreadable entry.</summary>
    [IntegrationFact]
    public async Task Cache_ForeignStringUnderTheKey_ReadsAsMiss()
    {
        await using var provider = await BuildProviderAsync();
        var cache = provider.GetRequiredService<IRedisCache>();
        var database = provider.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
        var key = Guid.NewGuid().ToString();
        await database.StringSetAsync("crash:" + key, "not a hash");

        var value = await cache.GetAsync<Message>(key);

        Assert.Null(value);
    }

    /// <summary>Five megabytes must round-trip unchanged.</summary>
    [IntegrationFact]
    public async Task Cache_FiveMegabytePayload_RoundTrips()
    {
        await using var provider = await BuildProviderAsync();
        var cache = provider.GetRequiredService<IRedisCache>();
        var key = Guid.NewGuid().ToString();
        var text = new string('x', 5 * 1024 * 1024);

        await cache.SetAsync(key, new Message(text, 1));

        Assert.Equal(text.Length, (await cache.GetAsync<Message>(key))?.Text.Length);
    }

    [IntegrationFact]
    public async Task Cache_EmojiKeyAndValue_RoundTrip()
    {
        await using var provider = await BuildProviderAsync();
        var cache = provider.GetRequiredService<IRedisCache>();

        await cache.SetAsync("🐌:ключ", new Message("🎉 значение", int.MaxValue));

        Assert.Equal(new Message("🎉 значение", int.MaxValue), await cache.GetAsync<Message>("🐌:ключ"));
    }

    /// <summary>Five hundred readers subscribe and dispose at once; afterwards Redis must hold no subscription and nothing may throw.</summary>
    [IntegrationFact]
    public async Task PubSub_FiveHundredParallelSubscribeAndDispose_LeavesNoSubscription()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "storm:" + Guid.NewGuid();

        await Task.WhenAll(Enumerable.Range(0, 500).Select(async _ =>
        {
            var subscription = await pubSub.SubscribeAsync<Message>(channel);
            await Task.Yield();
            await subscription.DisposeAsync();
        }));

        await WaitForSubscribersAsync(connection, channel, expected: 0);
    }

    /// <summary>Two hundred readers of one channel all receive a burst of messages in order.</summary>
    [IntegrationFact]
    public async Task PubSub_TwoHundredReadersOfOneChannel_AllReceiveTheBurstInOrder()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "burst:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var subscriptions = await Task.WhenAll(Enumerable.Range(0, 200).Select(_ => pubSub.SubscribeAsync<Message>(channel, cts.Token)));
        await WaitForSubscribersAsync(connection, channel, expected: 1);

        for (var i = 1; i <= 50; i++)
            await pubSub.PublishAsync(channel, new Message("burst", i), cts.Token);

        foreach (var subscription in subscriptions)
        {
            var received = new List<int>();
            await foreach (var message in subscription.ReadAllAsync(cts.Token))
            {
                received.Add(message.Message.Number);
                if (received.Count == 50)
                    break;
            }

            Assert.Equal(Enumerable.Range(1, 50), received);
            await subscription.DisposeAsync();
        }
    }

    /// <summary>A subscription typed as an interface cannot deserialize anything; the drop must be logged, not swallowed.</summary>
    [IntegrationFact]
    public async Task PubSub_UnsupportedType_LogsTheDrop()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "unsupported:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var subscription = await pubSub.SubscribeAsync<IDisposable>(channel, cts.Token);
        await WaitForSubscribersAsync(connection, channel, expected: 1);

        await connection.GetSubscriber().PublishAsync(RedisChannel.Literal(channel), "{}");
        await WaitUntilAsync(() => _logs.Contains(LogLevel.Warning));
    }

    /// <summary>Two consumers enumerating one subscription at the same time: the buffer is single-reader, so this must either work or fail loudly.</summary>
    [IntegrationFact]
    public async Task PubSub_TwoConcurrentReadersOfOneSubscription_DoNotCorruptTheBuffer()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var channel = "shared:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var subscription = await pubSub.SubscribeAsync<Message>(channel, cts.Token);
        await WaitForSubscribersAsync(connection, channel, expected: 1);

        var first = Task.Run(() => CountAsync(subscription, cts.Token), cts.Token);
        var second = Task.Run(() => CountAsync(subscription, cts.Token), cts.Token);
        for (var i = 0; i < 100; i++)
            await pubSub.PublishAsync(channel, new Message("shared", i), cts.Token);

        var counts = await Task.WhenAll(first, second);
        Assert.Equal(100, counts.Sum());
    }

    [IntegrationFact]
    public async Task PubSub_EmojiChannel_RoundTrips()
    {
        await using var provider = await BuildProviderAsync();
        var pubSub = provider.GetRequiredService<IRedisPubSub>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var subscription = await pubSub.SubscribeAsync<Message>("🐌:канал", cts.Token);

        await pubSub.PublishAsync("🐌:канал", new Message("🎉", 1), cts.Token);

        await foreach (var message in subscription.ReadAllAsync(cts.Token))
        {
            Assert.Equal("🐌:канал", message.Channel);
            break;
        }
    }

    /// <summary>An entry a value-type reader cannot parse must be skipped, not yielded as default(T).</summary>
    [IntegrationFact]
    public async Task Stream_UnreadableEntryForValueType_IsSkipped()
    {
        await using var provider = await BuildProviderAsync();
        var stream = provider.GetRequiredService<IRedisStream>();
        var database = provider.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
        var key = "stream:" + Guid.NewGuid();
        await database.StreamAddAsync(key, "data", "not json");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var received = new List<RedisStreamEntry<int>>();
        await foreach (var entry in stream.ReadAsync<int>(key, cancellationToken: cts.Token))
            received.Add(entry);

        Assert.Empty(received);
    }

    /// <summary>"$" is the Redis idiom for "only what comes next"; a follower started with it must see the next entry.</summary>
    [IntegrationFact]
    public async Task Stream_AfterDollar_FollowsNewEntries()
    {
        await using var provider = await BuildProviderAsync();
        var stream = provider.GetRequiredService<IRedisStream>();
        var connection = provider.GetRequiredService<IConnectionMultiplexer>();
        var key = "stream:" + Guid.NewGuid();
        await stream.AppendAsync(key, new Message("old", 1));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var reader = Task.Run(async () =>
        {
            await foreach (var entry in stream.ReadAsync<Message>(key, "$", cts.Token))
                return entry.Message.Number;
            return -1;
        }, cts.Token);
        await Task.Delay(500, cts.Token);
        await stream.AppendAsync(key, new Message("new", 2));

        Assert.Equal(2, await reader);
    }

    /// <summary>An id that is not an id must fail with an argument error, not with a server error deep inside the follow loop.</summary>
    [IntegrationFact]
    public async Task Stream_GarbageAfterId_ThrowsArgumentException()
    {
        await using var provider = await BuildProviderAsync();
        var stream = provider.GetRequiredService<IRedisStream>();
        var key = "stream:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in stream.ReadAsync<Message>(key, "🐌", cts.Token))
                break;
        });
    }

    private static async Task<int> CountAsync(IRedisSubscription<Message> subscription, CancellationToken cancellationToken)
    {
        var count = 0;

        await foreach (var _ in subscription.ReadAllAsync(cancellationToken))
            count++;

        return count;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);

        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The condition was not met in time.");

            await Task.Delay(50);
        }
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
