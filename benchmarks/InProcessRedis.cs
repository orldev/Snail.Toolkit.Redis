using System.Reflection;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>A Redis that answers in process, so a benchmark times the library and not the network.</summary>
/// <remarks>
/// Every read of a hash answers with the one entry it was given, every expiry and publish succeeds at once. The
/// answers are completed tasks made once, so what a benchmark allocates beyond the proxy's argument arrays is the
/// library's own.
/// </remarks>
internal sealed class InProcessRedis : IRedisConnection
{
    private static readonly Task<long> Received = Task.FromResult(1L);
    private static readonly Task<bool> Done = Task.FromResult(true);

    private readonly Task<RedisValue[]> _hash;
    private readonly IDatabase _database;
    private readonly ISubscriber _subscriber;
    private readonly IBatch _batch;
    private readonly Task<IConnectionMultiplexer> _connected;

    public InProcessRedis(RedisValue[] hash)
    {
        _hash = Task.FromResult(hash);
        _database = Proxy<IDatabase>();
        _subscriber = Proxy<ISubscriber>();
        _batch = Proxy<IBatch>();
        Multiplexer = Proxy<IConnectionMultiplexer>();
        _connected = Task.FromResult(Multiplexer);
    }

    public IConnectionMultiplexer Multiplexer { get; }

    public IConnectionMultiplexer? Opened => Multiplexer;

    public ConfigurationOptions Configuration { get; } = new();

    public Task<IConnectionMultiplexer> ConnectAsync(CancellationToken cancellationToken) => _connected;

    public event Action<IConnectionMultiplexer>? Replaced
    {
        add { }
        remove { }
    }

    internal object? Answer(MethodInfo? method) =>
        method?.Name switch
        {
            "GetDatabase" => _database,
            "GetSubscriber" => _subscriber,
            "CreateBatch" => _batch,
            "get_IsConnected" => true,
            "HashGetAsync" => _hash,
            "HashSetAsync" => Task.CompletedTask,
            "KeyExpire" => true,
            "KeyExpireAsync" or "KeyPersistAsync" => Done,
            "Execute" => null,
            "PublishAsync" => Received,
            _ => throw new NotSupportedException($"The in-process Redis does not answer {method?.Name}.")
        };

    private T Proxy<T>() where T : class
    {
        var proxy = DispatchProxy.Create<T, RedisProxy>();
        ((RedisProxy)(object)proxy).Redis = this;

        return proxy;
    }
}

/// <summary>Hands every call of a proxied Redis interface to the in-process Redis that made it.</summary>
/// <remarks>Left open: <see cref="DispatchProxy"/> derives the proxy from it at run time.</remarks>
public class RedisProxy : DispatchProxy
{
    internal InProcessRedis Redis { get; set; } = null!;

    /// <inheritdoc />
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Redis.Answer(targetMethod);
}
