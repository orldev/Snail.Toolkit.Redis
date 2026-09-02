## Snail.Toolkit.Redis

Redis extensions over `StackExchange.Redis` for .NET 10, with Microsoft-only dependencies:

- one shared `IConnectionMultiplexer` registered as a singleton;
- `ICacheService` — distributed cache with a fallback factory (over `Microsoft.Extensions.Caching.StackExchangeRedis`);
- `IRedisPubSub` — typed publish and `IAsyncEnumerable<T>` subscription over Redis channels.

Both cache values and pub/sub messages are serialized with `System.Text.Json` (web defaults).

| Namespace | Contents |
|---|---|
| `Snail.Toolkit.Redis` | `RedisOptions` |
| `Snail.Toolkit.Redis.Extensions` | `AddRedis`, `AddRedisCache`, `AddRedisPubSub`, `AddRedisHealthCheck` |
| `Snail.Toolkit.Redis.Cache` | `ICacheService`, `CacheServiceOptions` |
| `Snail.Toolkit.Redis.PubSub` | `IRedisPubSub`, `IRedisSubscription<T>`, `RedisMessage<T>`, `RedisPubSubOptions` |

### Configuration

```json
{
  "Redis": {
    "Connection": "localhost:6379",
    "Password": "secret",
    "InstanceName": "app:",
    "AbortOnConnectFail": false
  }
}
```

`Connection` is a StackExchange.Redis configuration string (several hosts, `ssl=true`, etc. are allowed).
`InstanceName` prefixes cache keys and is empty by default. With `AbortOnConnectFail = false` the host starts while Redis is down and reconnects later.

### Registration

Each `Add*` registers its own subsystem and reuses the shared connection, so they can be combined freely:

```c#
using Snail.Toolkit.Redis.Extensions;

builder.Services
    .AddRedisCache(builder.Configuration)        // IDistributedCache + ICacheService
    .AddRedisPubSub(builder.Configuration)       // IRedisPubSub
    .AddRedisHealthCheck(builder.Configuration); // health check "redis"
```

`AddRedis(configuration)` alone registers `RedisOptions`, `IConnectionMultiplexer` and a hosted warm-up that opens the connection at host start, so the first request never pays for the connect.
Pass the same `IConfiguration` instance to every call: each one binds `RedisOptions` from it.
The host must have logging registered (`AddLogging()` or a generic host).

### Cache

```c#
var config = await cacheService.GetAsync(
    "settings",
    token => LoadSettingsAsync(token),     // called on a cache miss with the same token, result is stored
    new DistributedCacheEntryOptions
    {
        AbsoluteExpiration = DateTimeOffset.UtcNow.AddMinutes(30),
        SlidingExpiration = TimeSpan.FromMinutes(10)
    },
    cancellationToken);
```

Without options an entry lives 10 minutes absolute with a 2 minute sliding window; both defaults and the JSON options are configurable:

```c#
builder.Services.AddRedisCache(builder.Configuration, options =>
{
    options.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1);
    options.SlidingExpiration = null;
    options.Json.PropertyNamingPolicy = null;
});
```

### Pub/Sub

Best-effort delivery: messages are not persisted and late subscribers miss earlier messages.

```c#
// publisher (fire-and-forget, JSON via System.Text.Json web defaults)
await pubSub.PublishAsync($"task:{taskId}:events", envelope, cancellationToken);
```

Three ways to consume, all backed by the same subscription type:

```c#
// 1. stream one channel: completes when the token is cancelled, which also unsubscribes
await foreach (var evt in pubSub.StreamAsync<TaskEventEnvelope>($"task:{taskId}:events", cancellationToken))
    await hub.Clients.Group(taskId).SendAsync("event", evt);

// 2. explicit subscription: active as soon as SubscribeAsync returns, so subscribe first, then load history
await using var subscription = await pubSub.SubscribeAsync<TaskEventEnvelope>($"task:{taskId}:events", cancellationToken);
var history = await journal.ReadAsync(taskId, cancellationToken);
await foreach (var message in subscription.ReadAllAsync(cancellationToken))
    ... // deduplicate against history by sequence

// 3. glob pattern across channels: every message carries the channel it came from
await using var all = await pubSub.SubscribePatternAsync<TaskEventEnvelope>("task:*:events", cancellationToken);
await foreach (var message in all.ReadAllAsync(cancellationToken))
    Console.WriteLine($"{message.Channel}: {message.Message}");
```

Publish the declared base type when a hierarchy uses `JsonPolymorphic`: `PublishAsync<TaskEvent>(...)` or an envelope whose property is typed as the base. Publishing a derived type directly omits the discriminator and the subscriber drops the message.

Each subscription buffers up to `BufferCapacity` messages (1024 by default) ahead of a slow consumer and drops the oldest when full; the first drop and every thousandth are logged at `Warning`.
Payloads that fail to deserialize are logged at `Warning` and skipped. Publishing `null` throws.

```c#
builder.Services.AddRedisPubSub(builder.Configuration, options =>
{
    options.BufferCapacity = 4096;
    options.Json.PropertyNamingPolicy = null;
});
```

### Health check

`AddRedisHealthCheck(configuration, name: "redis", failureStatus: HealthStatus.Unhealthy, tags: null)` registers a check that PINGs the shared connection and reports the latency in `Data["latencyMs"]`. The name is registered once even when the call is repeated. Expose it as usual:

```c#
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") });
```

### Password rotation

`RedisOptions` is bound with change tracking. When the configuration reloads with a new `Password` (for example from a Vault provider), the running connection keeps its authenticated sockets and every later reconnect uses the new password, without restarting the host. A changed `Connection` (endpoints) is only logged: applying it needs a restart.

### Reusing the connection

Anything that accepts a `ConnectionMultiplexer` factory can share the registered one, so a second connection is never needed:

```c#
// SignalR backplane for several UI instances
builder.Services.AddSignalR().AddStackExchangeRedis(o =>
    o.ConnectionFactory = _ => Task.FromResult(
        (IConnectionMultiplexer)builder.Services.BuildServiceProvider().GetRequiredService<IConnectionMultiplexer>()));

// data protection keys shared between instances
builder.Services.AddDataProtection().PersistKeysToStackExchangeRedis(
    () => provider.GetRequiredService<IConnectionMultiplexer>().GetDatabase(), "dataprotection-keys");

// raw commands (counters, flags, locks) through IDatabase
var database = provider.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
await database.StringIncrementAsync("provider:openai:requests");
```

### Patterns for live pages

- **Late opener of a streaming step.** Deltas already published are gone. Let the producer store the accumulated text every N deltas with `ICacheService.SetAsync(key, text, options)` under a short TTL; a page that opens mid-step reads it once and then follows the delta channel.
- **Global stop flag.** A broadcast on a message bus reaches only the workers alive at that moment. Keep the flag in Redis as well (`SetAsync("stop-all", ...)`) and check it before each step, so a worker that starts later sees it too.

### Tests

Unit tests run everywhere. Integration tests start a `redis:7-alpine` container through Testcontainers and are skipped when no Docker daemon is reachable.

## License

Snail.Toolkit.Redis is a free and open source project, released under the permissible [MIT license](LICENSE).
