using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.Redis.Cache;

namespace Snail.Toolkit.Redis.Tests;

public class CacheServiceTests
{
    public sealed class Payload
    {
        public string? Field1 { get; set; }
        public int Field2 { get; set; }
    }

    private readonly Mock<IDistributedCache> _cache = new();
    private readonly CacheServiceOptions _options = new();
    private readonly CacheService _service;

    public CacheServiceTests()
    {
        _service = new CacheService(_cache.Object, Options.Create(_options));
    }

    [Fact]
    public async Task GetAsync_MissingKey_ReturnsNull()
    {
        _cache.Setup(c => c.GetAsync("key", It.IsAny<CancellationToken>())).ReturnsAsync((byte[]?)null);

        var value = await _service.GetAsync<Payload>("key");

        Assert.Null(value);
    }

    [Fact]
    public async Task GetAsync_StoredJson_DeserializesCaseInsensitively()
    {
        _cache.Setup(c => c.GetAsync("key", It.IsAny<CancellationToken>()))
            .ReturnsAsync("{\"Field1\":\"a\",\"field2\":2}"u8.ToArray());

        var value = await _service.GetAsync<Payload>("key");

        Assert.NotNull(value);
        Assert.Equal("a", value.Field1);
        Assert.Equal(2, value.Field2);
    }

    [Fact]
    public async Task GetAsync_WithFactory_OnMissStoresFactoryValueAsCamelCase()
    {
        _cache.Setup(c => c.GetAsync("key", It.IsAny<CancellationToken>())).ReturnsAsync((byte[]?)null);
        byte[]? stored = null;
        _cache.Setup(c => c.SetAsync("key", It.IsAny<byte[]>(), It.IsAny<DistributedCacheEntryOptions>(), It.IsAny<CancellationToken>()))
            .Callback<string, byte[], DistributedCacheEntryOptions, CancellationToken>((_, bytes, _, _) => stored = bytes)
            .Returns(Task.CompletedTask);

        var value = await _service.GetAsync("key", _ => Task.FromResult(new Payload { Field1 = "b", Field2 = 3 }));

        Assert.Equal("b", value?.Field1);
        Assert.NotNull(stored);
        Assert.Equal("{\"field1\":\"b\",\"field2\":3}", Encoding.UTF8.GetString(stored));
    }

    [Fact]
    public async Task GetAsync_WithFactory_PassesCancellationTokenToFactory()
    {
        using var cts = new CancellationTokenSource();
        _cache.Setup(c => c.GetAsync("key", cts.Token)).ReturnsAsync((byte[]?)null);
        _cache.Setup(c => c.SetAsync("key", It.IsAny<byte[]>(), It.IsAny<DistributedCacheEntryOptions>(), cts.Token))
            .Returns(Task.CompletedTask);
        CancellationToken observed = default;

        await _service.GetAsync("key", token =>
        {
            observed = token;
            return Task.FromResult(new Payload());
        }, cancellationToken: cts.Token);

        Assert.Equal(cts.Token, observed);
    }

    [Fact]
    public async Task GetAsync_WithFactory_OnHitSkipsFactory()
    {
        _cache.Setup(c => c.GetAsync("key", It.IsAny<CancellationToken>()))
            .ReturnsAsync("{\"field1\":\"cached\"}"u8.ToArray());
        var factoryCalled = false;

        var value = await _service.GetAsync("key", _ =>
        {
            factoryCalled = true;
            return Task.FromResult(new Payload());
        });

        Assert.Equal("cached", value?.Field1);
        Assert.False(factoryCalled);
        _cache.Verify(c => c.SetAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<DistributedCacheEntryOptions>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task SetAsync_WithoutOptions_AppliesConfiguredDefaults()
    {
        _options.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(30);
        _options.SlidingExpiration = TimeSpan.FromMinutes(5);
        DistributedCacheEntryOptions? used = null;
        _cache.Setup(c => c.SetAsync("key", It.IsAny<byte[]>(), It.IsAny<DistributedCacheEntryOptions>(), It.IsAny<CancellationToken>()))
            .Callback<string, byte[], DistributedCacheEntryOptions, CancellationToken>((_, _, options, _) => used = options)
            .Returns(Task.CompletedTask);

        await _service.SetAsync("key", new Payload());

        Assert.NotNull(used);
        Assert.Equal(TimeSpan.FromMinutes(30), used.AbsoluteExpirationRelativeToNow);
        Assert.Equal(TimeSpan.FromMinutes(5), used.SlidingExpiration);
        Assert.Null(used.AbsoluteExpiration);
    }

    [Fact]
    public async Task SetAsync_WithOptions_UsesThemAsGiven()
    {
        var explicitOptions = new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(1) };
        DistributedCacheEntryOptions? used = null;
        _cache.Setup(c => c.SetAsync("key", It.IsAny<byte[]>(), It.IsAny<DistributedCacheEntryOptions>(), It.IsAny<CancellationToken>()))
            .Callback<string, byte[], DistributedCacheEntryOptions, CancellationToken>((_, _, options, _) => used = options)
            .Returns(Task.CompletedTask);

        await _service.SetAsync("key", new Payload(), explicitOptions);

        Assert.Same(explicitOptions, used);
    }

    [Fact]
    public async Task RemoveAsync_DelegatesToCache()
    {
        _cache.Setup(c => c.RemoveAsync("key", It.IsAny<CancellationToken>())).Returns(Task.CompletedTask).Verifiable();

        await _service.RemoveAsync("key");

        _cache.Verify();
    }
}
