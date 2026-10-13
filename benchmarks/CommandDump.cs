using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.Cache;
using Snail.Toolkit.Redis.PubSub;
using StackExchange.Redis;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>Every benchmarked operation run once against a recording in-process Redis: what reached Redis and what came back.</summary>
/// <remarks>
/// <para>
/// The proof that a speed-up kept the behaviour: the dump of the code before a change and after it must be equal.
/// Timestamps written into an entry are shown relative to now, rounded to the minute, so two runs compare.
/// </para>
/// <para>Run it with <c>dotnet run -c Release --project benchmarks -- dump</c>.</para>
/// </remarks>
internal static class CommandDump
{
    private const string Key = "order:6f1c2a43-9a51-4d8f-b0f1-3c5e7d2a9b10";

    /// <summary>Runs the operations and writes the dump to the console.</summary>
    public static async Task RunAsync()
    {
        var order = Order.Sample();
        var payload = JsonSerializer.SerializeToUtf8Bytes(order, new RedisCacheOptions().Json);
        RedisValue[] stored = [DateTimeOffset.UtcNow.AddHours(1).Ticks, TimeSpan.FromMinutes(2).Ticks, payload];
        RedisValue[] missing = [RedisValue.Null, RedisValue.Null, RedisValue.Null];

        foreach (var prefix in new[] { null, "shop:" })
        {
            await DumpAsync($"cache hit, prefix {prefix ?? "none"}", Recording(stored), async redis =>
                (await Cache(redis, prefix).GetAsync<Order>(Key))?.Quantity);
            await DumpAsync($"cache hit through the factory, prefix {prefix ?? "none"}", Recording(stored), async redis =>
                (await Cache(redis, prefix).GetAsync(Key, _ => Task.FromResult<Order?>(order with { Quantity = 99 })))?.Quantity);
            await DumpAsync($"cache miss through the factory, prefix {prefix ?? "none"}", Recording(missing), async redis =>
                (await Cache(redis, prefix).GetAsync(Key, _ => Task.FromResult<Order?>(order with { Quantity = 99 })))?.Quantity);
            await DumpAsync($"cache store, prefix {prefix ?? "none"}", Recording(stored), async redis =>
            {
                await Cache(redis, prefix).SetAsync(Key, order);
                return "stored";
            });
        }

        await DumpAsync("publish", Recording([]), async redis =>
        {
            var pubSub = new RedisPubSub(redis, Options.Create(new RedisPubSubOptions()), NullLogger<RedisPubSub>.Instance);
            var first = await pubSub.PublishAsync("orders:created", order);
            var second = await pubSub.PublishAsync("orders:shipped", order);
            var third = await pubSub.PublishAsync("orders:created", order);

            return $"{first} {second} {third}";
        });
        await DumpAsync("stream append", Recording([]), async redis =>
            await StreamBenchmarks.Create(redis).AppendAsync("orders", order));
        await DumpAsync("stream read, a page and the first entry of the next", Recording([], StreamBenchmarks.Page()), async redis =>
        {
            var read = new List<string>();

            await foreach (var entry in StreamBenchmarks.Create(redis).ReadAsync<Order>("orders", "1760258399999-7"))
            {
                read.Add($"{entry.Id}:{entry.Message.Quantity}");

                if (read.Count == StreamBenchmarks.PageSize + 1)
                    break;
            }

            return string.Join(' ', read);
        });
    }

    private static RedisCache Cache(InProcessRedis redis, string? prefix)
    {
        var settings = new RedisOptions { Connection = "localhost", InstanceName = prefix };

        return new RedisCache(redis, Options.Create(settings), Options.Create(new RedisCacheOptions()), NullLogger<RedisCache>.Instance);
    }

    private static InProcessRedis Recording(RedisValue[] hash, StreamEntry[]? page = null) => new(hash, page, []);

    private static async Task DumpAsync(string name, InProcessRedis redis, Func<InProcessRedis, Task<object?>> operation)
    {
        var answer = await operation(redis);

        Console.WriteLine($"== {name}");

        foreach (var (method, arguments) in redis.Recorded!)
            Console.WriteLine($"   {method}({string.Join(", ", arguments.Select(Format))})");

        Console.WriteLine($"   -> {answer}");
    }

    private static string Format(object? argument) =>
        argument switch
        {
            null => "null",
            RedisKey key => $"key '{key}'",
            RedisValue value => Value(value),
            RedisValue[] values => $"[{string.Join(", ", values.Select(Value))}]",
            HashEntry[] entries => $"{{{string.Join(", ", entries.Select(entry => $"{Value(entry.Name)}={Value(entry.Value)}"))}}}",
            RedisChannel channel => $"{(channel.IsPattern ? "pattern" : "channel")} '{channel}'",
            _ => Convert.ToString(argument, CultureInfo.InvariantCulture) ?? "?"
        };

    private static string Value(RedisValue value)
    {
        if (value.IsNull)
            return "nil";

        var now = DateTimeOffset.UtcNow.Ticks;

        if (value.TryParse(out long ticks) && ticks > now - TimeSpan.TicksPerDay && ticks < now + (365 * TimeSpan.TicksPerDay))
            return $"now+{Math.Round((ticks - now) / (double)TimeSpan.TicksPerMinute).ToString(CultureInfo.InvariantCulture)}m";

        return $"'{Encoding.UTF8.GetString((byte[])value!)}'";
    }
}
