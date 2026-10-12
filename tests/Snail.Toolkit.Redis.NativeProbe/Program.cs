using System.Text.Json.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis;
using Snail.Toolkit.Redis.Cache;
using Snail.Toolkit.Redis.Extensions;

var configuration = new ConfigurationBuilder()
    .AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Redis:Connection"] = "127.0.0.1:1,connectTimeout=200,syncTimeout=200,asyncTimeout=200",
        ["Redis:Password"] = "secret",
        ["Redis:InstanceName"] = "probe:"
    })
    .Build();

using var services = new ServiceCollection()
    .AddLogging()
    .AddRedisCache(configuration, cache => cache.Json.TypeInfoResolver = ProbeJson.Default)
    .BuildServiceProvider();

var settings = services.GetRequiredService<IOptions<RedisOptions>>().Value;
var connection = settings.ToConfigurationOptions();
var cache = services.GetRequiredService<IRedisCache>();

await cache.SetAsync("order", new Order("A-1", 3));

var isMissed = await cache.GetAsync<Order>("order") is null;
var isBound = settings.InstanceName == "probe:" && connection.Password == "secret" && connection.EndPoints.Count == 1;

Console.WriteLine($"settings bound: {isBound}, an unreachable Redis read as a miss: {isMissed}");

return isBound && isMissed ? 0 : 1;

/// <summary>A value the cache stores, serialized by a source-generated context as a native app must.</summary>
internal sealed record Order(string Id, int Quantity);

/// <summary>The JSON contract of the probe's values.</summary>
[JsonSerializable(typeof(Order))]
internal sealed partial class ProbeJson : JsonSerializerContext;
