using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.Redis.Extensions;
using Snail.Toolkit.Redis.Streams;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

public class RedisStreamTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    public sealed record Message(string Text, int Number);

    private async Task<ServiceProvider> BuildProviderAsync()
    {
        var configuration = TestConfiguration.Get(await fixture.GetConnectionStringAsync(), password: null);

        return new ServiceCollection()
            .AddLogging()
            .AddRedisStreams(configuration, o => o.PollInterval = TimeSpan.FromSeconds(5))
            .BuildServiceProvider();
    }

    [IntegrationFact]
    public async Task ReadAsync_LateReader_SeesHistoryThenFollows()
    {
        await using var provider = await BuildProviderAsync();
        var stream = provider.GetRequiredService<IRedisStream>();
        var key = "stream:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await stream.AppendAsync(key, new Message("history", 1));
        await stream.AppendAsync(key, new Message("history", 2));

        var received = new List<RedisStreamEntry<Message>>();
        var reader = Task.Run(async () =>
        {
            await foreach (var entry in stream.ReadAsync<Message>(key, cancellationToken: cts.Token))
            {
                received.Add(entry);
                if (received.Count == 4)
                    break;
            }
        }, cts.Token);
        await Task.Delay(300, cts.Token);
        await stream.AppendAsync(key, new Message("live", 3));
        await stream.AppendAsync(key, new Message("live", 4));
        await reader;

        Assert.Equal([1, 2, 3, 4], received.Select(entry => entry.Message.Number));
        Assert.Equal(received.Select(entry => entry.Id).OrderBy(id => id, StringComparer.Ordinal), received.Select(entry => entry.Id));
    }

    [IntegrationFact]
    public async Task ReadAsync_AfterAnId_SkipsEarlierEntries()
    {
        await using var provider = await BuildProviderAsync();
        var stream = provider.GetRequiredService<IRedisStream>();
        var key = "stream:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await stream.AppendAsync(key, new Message("old", 1));
        var boundary = await stream.AppendAsync(key, new Message("boundary", 2));
        await stream.AppendAsync(key, new Message("new", 3));

        await foreach (var entry in stream.ReadAsync<Message>(key, boundary, cts.Token))
        {
            Assert.Equal(3, entry.Message.Number);
            break;
        }
    }

    [IntegrationFact]
    public async Task AppendAsync_WithMaxLength_TrimsTheStream()
    {
        await using var provider = await BuildProviderAsync();
        var stream = provider.GetRequiredService<IRedisStream>();
        var key = "stream:" + Guid.NewGuid();

        for (var i = 0; i < 400; i++)
            await stream.AppendAsync(key, new Message("trim", i), maxLength: 10);

        var length = await provider.GetRequiredService<IConnectionMultiplexer>().GetDatabase().StreamLengthAsync(key);
        Assert.InRange(length, 10, 200);
    }

    [IntegrationFact]
    public async Task ReadAsync_CancelledWhileFollowing_CompletesQuietly()
    {
        await using var provider = await BuildProviderAsync();
        var stream = provider.GetRequiredService<IRedisStream>();
        var key = "stream:" + Guid.NewGuid();
        using var cts = new CancellationTokenSource();

        var reader = Task.Run(async () =>
        {
            var count = 0;
            await foreach (var _ in stream.ReadAsync<Message>(key, cancellationToken: cts.Token))
                count++;
            return count;
        });
        await Task.Delay(300);
        cts.Cancel();

        Assert.Equal(0, await reader.WaitAsync(TimeSpan.FromSeconds(10)));
    }
}
