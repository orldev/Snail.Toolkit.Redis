using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.PubSub;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Tests;

public class RedisPubSubTests
{
    private readonly RedisPubSub _pubSub = new(Mock.Of<IRedisConnection>(), Options.Create(new RedisPubSubOptions()), NullLogger<RedisPubSub>.Instance);

    [Fact]
    public async Task PublishAsync_NullMessage_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _pubSub.PublishAsync<object>("channel", null!));
    }

    [Fact]
    public async Task PublishAsync_BlankChannel_Throws()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _pubSub.PublishAsync(" ", new object()));
    }

    [Fact]
    public async Task SubscribeAsync_EmptyChannel_Throws()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _pubSub.SubscribeAsync<object>(""));
    }

    /// <summary>StackExchange.Redis builds a subscriber per GetSubscriber; publishing keeps one per multiplexer and follows a replacement.</summary>
    [Fact]
    public async Task PublishAsync_Repeated_AsksEachMultiplexerForItsSubscriberOnce()
    {
        var (first, firstSubscriber) = Multiplexer();
        var (second, secondSubscriber) = Multiplexer();
        var connection = new Mock<IRedisConnection>();
        connection.SetupSequence(c => c.Opened).Returns(first.Object).Returns(first.Object).Returns(second.Object);
        var pubSub = new RedisPubSub(connection.Object, Options.Create(new RedisPubSubOptions()), NullLogger<RedisPubSub>.Instance);

        await pubSub.PublishAsync("orders", "a");
        await pubSub.PublishAsync("orders", "b");
        await pubSub.PublishAsync("orders", "c");

        first.Verify(m => m.GetSubscriber(It.IsAny<object?>()), Times.Once);
        second.Verify(m => m.GetSubscriber(It.IsAny<object?>()), Times.Once);
        firstSubscriber.Verify(s => s.PublishAsync(RedisChannel.Literal("orders"), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()), Times.Exactly(2));
        secondSubscriber.Verify(s => s.PublishAsync(RedisChannel.Literal("orders"), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    private static (Mock<IConnectionMultiplexer> Multiplexer, Mock<ISubscriber> Subscriber) Multiplexer()
    {
        var subscriber = new Mock<ISubscriber>();
        subscriber.Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>())).ReturnsAsync(1L);
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(subscriber.Object);

        return (multiplexer, subscriber);
    }
}
