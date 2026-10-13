using System.Reflection;
using System.Text;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>A Redis that answers in process, so a benchmark times the library and not the network.</summary>
/// <remarks>
/// Every read of a hash answers with the one entry it was given, every read of a stream with the page it was given,
/// every expiry, append and publish succeeds at once. The answers are completed tasks made once, so what a benchmark
/// allocates beyond the proxy's argument arrays is the library's own. Stream ids and field names are raw bytes, as
/// StackExchange.Redis hands them over from a server. A recording Redis writes every call with its arguments to
/// <see cref="Recorded"/>: what reached Redis. Only a recording one hands the arguments on, as a proxy whose arguments
/// escape allocates more per call and would hide the library's own allocations.
/// </remarks>
internal sealed class InProcessRedis : IRedisConnection
{
    private static readonly Task<long> Received = Task.FromResult(1L);
    private static readonly Task<bool> Done = Task.FromResult(true);
    private static readonly Task<RedisValue> Appended = Task.FromResult((RedisValue)Encoding.UTF8.GetBytes("1760258400000-0"));

    private readonly Task<RedisValue[]> _hash;
    private readonly Task<StreamEntry[]> _page;
    private readonly IDatabase _database;
    private readonly ISubscriber _subscriber;
    private readonly IBatch _batch;
    private readonly Task<IConnectionMultiplexer> _connected;

    public InProcessRedis(RedisValue[] hash, StreamEntry[]? page = null, List<(string Method, object?[] Arguments)>? recorded = null)
    {
        Recorded = recorded;
        _hash = Task.FromResult(hash);
        _page = Task.FromResult(page ?? []);
        _database = Proxy<IDatabase>();
        _subscriber = Proxy<ISubscriber>();
        _batch = Proxy<IBatch>();
        Multiplexer = Proxy<IConnectionMultiplexer>();
        _connected = Task.FromResult(Multiplexer);
    }

    public IConnectionMultiplexer Multiplexer { get; }

    public IConnectionMultiplexer? Opened => Multiplexer;

    public ConfigurationOptions Configuration { get; } = new();

    /// <summary>For a recording Redis, every call made to the proxied interfaces, by name and arguments, in order.</summary>
    public List<(string Method, object?[] Arguments)>? Recorded { get; }

    public Task<IConnectionMultiplexer> ConnectAsync(CancellationToken cancellationToken) => _connected;

    public event Action<IConnectionMultiplexer>? Replaced
    {
        add { }
        remove { }
    }

    internal object? Answer(MethodInfo? method, object?[]? arguments)
    {
        if (arguments is not null)
            Recorded?.Add((method?.Name ?? "?", arguments));

        return method?.Name switch
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
            "StreamAddAsync" => Appended,
            "StreamReadAsync" => _page,
            _ => throw new NotSupportedException($"The in-process Redis does not answer {method?.Name}.")
        };
    }

    private T Proxy<T>() where T : class
    {
        var proxy = Recorded is null ? DispatchProxy.Create<T, RedisProxy>() : DispatchProxy.Create<T, RecordingRedisProxy>();
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
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Redis.Answer(targetMethod, null);
}

/// <summary>A <see cref="RedisProxy"/> that hands the arguments on as well, for the in-process Redis to record.</summary>
/// <remarks>Left open: <see cref="DispatchProxy"/> derives the proxy from it at run time.</remarks>
public class RecordingRedisProxy : RedisProxy
{
    /// <inheritdoc />
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Redis.Answer(targetMethod, args);
}
