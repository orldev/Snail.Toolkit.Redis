using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.PubSub;

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
}
