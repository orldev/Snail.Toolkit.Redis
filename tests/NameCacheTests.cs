namespace Snail.Toolkit.Redis.Tests;

public class NameCacheTests
{
    [Fact]
    public void For_RepeatedName_MakesItOnce()
    {
        var made = 0;
        var cache = new NameCache<string>(name => $"{name}:{++made}", 2);

        var first = cache.For("orders");
        var second = cache.For("orders");

        Assert.Same(first, second);
        Assert.Equal(1, made);
    }

    /// <summary>A name beyond the limit is made on every call, and the names kept before it stay kept.</summary>
    [Fact]
    public void For_NamesBeyondTheLimit_AreMadeEveryTimeAndEvictNothing()
    {
        var made = new List<string>();
        var cache = new NameCache<string>(name =>
        {
            made.Add(name);
            return name.ToUpperInvariant();
        }, 2);

        foreach (var name in new[] { "a", "b", "c", "c", "a", "b" })
            Assert.Equal(name.ToUpperInvariant(), cache.For(name));

        Assert.Equal(["a", "b", "c", "c"], made);
    }
}
