using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.PubSub;
using Snail.Toolkit.Redis.Streams;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>The follow loop of a stream under the ordinary ways a consumer and a server misbehave, with Redis replaced by mocks.</summary>
public class RedisStreamsCrashTests
{
    public sealed class Payload
    {
        public int Number { get; set; }
    }

    private const string Announcements = "stream:orders:appended";

    private readonly Mock<IDatabase> _database = new();
    private readonly Mock<IRedisPubSub> _pubSub = new();
    private readonly Mock<IRedisSubscription<string>> _subscription = new();

    public RedisStreamsCrashTests()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(_database.Object);
        var connection = new Mock<IRedisConnection>();
        connection.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).ReturnsAsync(multiplexer.Object);
        _subscription.Setup(s => s.ReadAllAsync(It.IsAny<CancellationToken>())).Returns(Quiet);
        _pubSub.Setup(p => p.SubscribeAsync<string>(Announcements, It.IsAny<CancellationToken>())).ReturnsAsync(_subscription.Object);
        _pubSub.Setup(p => p.PublishAsync(Announcements, It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(1L);

        Connection = connection.Object;
    }

    private IRedisConnection Connection { get; }

    /// <summary>A consumer that leaves the loop with break while following must get control back at once, not wait for the next announcement.</summary>
    [Fact]
    public async Task ReadAsync_ConsumerBreaksWhileFollowing_ReturnsWithoutWaitingForAnAnnouncement()
    {
        Page("0-0", [Entry("1-0", 1)]);
        _database.SetupSequence(d => d.StreamReadAsync("orders", It.Is<RedisValue>(v => v == "1-0"), 2, It.IsAny<CommandFlags>()))
            .ReturnsAsync([])
            .ReturnsAsync([Entry("2-0", 2)]);
        Page("2-0", []);
        var streams = Streams(TimeSpan.FromMilliseconds(20));

        var consumer = Task.Run(async () =>
        {
            await foreach (var entry in streams.ReadAsync<Payload>("orders"))
            {
                if (entry.Id == "2-0")
                    break;
            }
        });

        await consumer.WaitAsync(TimeSpan.FromSeconds(3));
    }

    /// <summary>A follower runs until it is cancelled: a transient timeout while the multiplexer reconnects must cost it a poll, not the loop.</summary>
    [Fact]
    public async Task ReadAsync_TransientTimeoutWhileFollowing_KeepsFollowing()
    {
        Page("0-0", [Entry("1-0", 1)]);
        _database.SetupSequence(d => d.StreamReadAsync("orders", It.Is<RedisValue>(v => v == "1-0"), 2, It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisTimeoutException("Timeout awaiting response", CommandStatus.Sent))
            .ReturnsAsync([Entry("2-0", 2)]);
        Page("2-0", []);
        var streams = Streams(TimeSpan.FromMilliseconds(20));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var read = new List<string>();

        await foreach (var entry in streams.ReadAsync<Payload>("orders", cancellationToken: cts.Token))
        {
            read.Add(entry.Id);

            if (read.Count == 2)
                break;
        }

        Assert.Equal(["1-0", "2-0"], read);
    }

    /// <summary>A burst of appends is announced once per append; one read fetches them all, so a hundred announcements must not cost a hundred reads.</summary>
    [Fact]
    public async Task ReadAsync_HundredAnnouncementsAtOnce_ReadsTheStreamAFewTimesNotAHundred()
    {
        Page("0-0", [Entry("1-0", 1)]);
        Page("1-0", []);
        _subscription.Setup(s => s.ReadAllAsync(It.IsAny<CancellationToken>())).Returns((CancellationToken token) => Burst(100, token));
        var streams = Streams(TimeSpan.FromSeconds(10));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        await foreach (var _ in streams.ReadAsync<Payload>("orders", cancellationToken: cts.Token))
        {
        }

        var reads = _database.Invocations.Count(invocation => invocation.Method.Name == nameof(IDatabase.StreamReadAsync));
        Assert.True(reads <= 5, $"The stream was read {reads} times for 100 announcements.");
    }

    /// <summary>A follower cancelled mid-way completes quietly; one cancelled before it starts must answer the same way, as StreamAsync does.</summary>
    [Fact]
    public async Task ReadAsync_TokenAlreadyCancelled_CompletesQuietlyLikeAFollowerDoes()
    {
        var pubSub = new RedisPubSub(Connection, Options.Create(new RedisPubSubOptions()), NullLogger<RedisPubSub>.Instance);
        var streams = new RedisStreams(Connection, pubSub, Options.Create(new RedisStreamOptions()), NullLogger<RedisStreams>.Instance);
        var count = 0;

        await foreach (var _ in streams.ReadAsync<Payload>("orders", cancellationToken: new CancellationToken(canceled: true)))
            count++;

        Assert.Equal(0, count);
    }

    /// <summary>Once XADD answered, the entry is in the stream; a failed announcement must not throw, or a caller that retries appends it twice.</summary>
    [Fact]
    public async Task AppendAsync_AnnouncementFails_StillReturnsTheIdOfTheAppendedEntry()
    {
        _database.Setup(d => d.StreamAddAsync("orders", "data", It.IsAny<RedisValue>(), null, null, true, It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisValue)Encoding.UTF8.GetBytes("5-0"));
        _pubSub.Setup(p => p.PublishAsync(Announcements, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new RedisTimeoutException("Timeout awaiting response", CommandStatus.Sent));
        var streams = Streams(TimeSpan.FromSeconds(1));

        var id = await streams.AppendAsync("orders", new Payload { Number = 5 });

        Assert.Equal("5-0", id);
    }

    /// <summary>An id beyond 64 bits is no id Redis accepts and must fail as an argument up front, not as a server error inside the follow loop.</summary>
    [Fact]
    public async Task ReadAsync_IdBeyondSixtyFourBits_ThrowsArgumentException()
    {
        var streams = Streams(TimeSpan.FromSeconds(1));

        await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in streams.ReadAsync<Payload>("orders", "18446744073709551616-0"))
                break;
        });
    }

    private RedisStreams Streams(TimeSpan pollInterval)
    {
        var options = new RedisStreamOptions { PageSize = 2, PollInterval = pollInterval };

        return new RedisStreams(Connection, _pubSub.Object, Options.Create(options), NullLogger<RedisStreams>.Instance);
    }

    private void Page(string position, StreamEntry[] entries) =>
        _database.Setup(d => d.StreamReadAsync("orders", It.Is<RedisValue>(v => v == position), 2, It.IsAny<CommandFlags>()))
            .ReturnsAsync(entries);

    private static StreamEntry Entry(string id, int number)
    {
        var payload = Encoding.UTF8.GetBytes($$"""{"number":{{number}}}""");

        return new StreamEntry(Encoding.UTF8.GetBytes(id), [new NameValueEntry(Encoding.UTF8.GetBytes("data"), payload)]);
    }

    private static async IAsyncEnumerable<RedisMessage<string>> Quiet([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await WaitForeverAsync(cancellationToken);

        yield break;
    }

    private static async IAsyncEnumerable<RedisMessage<string>> Burst(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var index = 0; index < count; index++)
            yield return new RedisMessage<string>(Announcements, $"1-{index}");

        await WaitForeverAsync(cancellationToken);
    }

    private static async Task WaitForeverAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
    }
}
