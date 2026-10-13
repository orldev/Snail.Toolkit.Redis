using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.PubSub;
using Snail.Toolkit.Redis.Streams;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Reading and appending with Redis replaced by mocks that answer as a server does: ids and field names as raw bytes.</summary>
public class RedisStreamsReadingTests
{
    public sealed class Payload
    {
        public int Number { get; set; }
    }

    private readonly Mock<IDatabase> _database = new();
    private readonly Mock<IRedisPubSub> _pubSub = new();
    private readonly RedisStreams _streams;

    public RedisStreamsReadingTests()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(_database.Object);
        var connection = new Mock<IRedisConnection>();
        connection.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).ReturnsAsync(multiplexer.Object);
        var subscription = new Mock<IRedisSubscription<string>>();
        subscription.Setup(s => s.ReadAllAsync(It.IsAny<CancellationToken>())).Returns(Quiet);
        _pubSub.Setup(p => p.SubscribeAsync<string>("stream:orders:appended", It.IsAny<CancellationToken>())).ReturnsAsync(subscription.Object);
        _pubSub.Setup(p => p.PublishAsync("stream:orders:appended", It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(1L);

        _streams = new RedisStreams(connection.Object, _pubSub.Object, Options.Create(new RedisStreamOptions { PageSize = 2 }), NullLogger<RedisStreams>.Instance);
    }

    /// <summary>The next page is asked for after the last id of the previous one, and every entry is handed out with its own id.</summary>
    [Fact]
    public async Task ReadAsync_PagesOfRawEntries_FollowTheLastIdAndHandOutEveryEntry()
    {
        _database.Setup(d => d.StreamReadAsync("orders", It.Is<RedisValue>(v => v == "0-0"), 2, It.IsAny<CommandFlags>()))
            .ReturnsAsync([Entry("1-0", "data", 1), Entry("1-1", "other", 2)]);
        _database.Setup(d => d.StreamReadAsync("orders", It.Is<RedisValue>(v => v == "1-1"), 2, It.IsAny<CommandFlags>()))
            .ReturnsAsync([Entry("2-0", "data", 3)]);
        var read = new List<string>();

        await foreach (var entry in _streams.ReadAsync<Payload>("orders"))
        {
            read.Add($"{entry.Id}:{entry.Message.Number}");

            if (read.Count == 2)
                break;
        }

        Assert.Equal(["1-0:1", "2-0:3"], read);
    }

    /// <summary>The id Redis assigned is both announced and returned.</summary>
    [Fact]
    public async Task AppendAsync_AddedEntry_AnnouncesAndReturnsItsId()
    {
        _database.Setup(d => d.StreamAddAsync("orders", "data", It.IsAny<RedisValue>(), null, null, true, It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisValue)Encoding.UTF8.GetBytes("5-0"));

        var id = await _streams.AppendAsync("orders", new Payload { Number = 5 });

        Assert.Equal("5-0", id);
        _pubSub.Verify(p => p.PublishAsync("stream:orders:appended", "5-0", It.IsAny<CancellationToken>()), Times.Once);
    }

    private static StreamEntry Entry(string id, string field, int number)
    {
        var payload = Encoding.UTF8.GetBytes($$"""{"number":{{number}}}""");

        return new StreamEntry(Encoding.UTF8.GetBytes(id), [new NameValueEntry(Encoding.UTF8.GetBytes(field), payload)]);
    }

    private static async IAsyncEnumerable<RedisMessage<string>> Quiet([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken);

        yield break;
    }
}
