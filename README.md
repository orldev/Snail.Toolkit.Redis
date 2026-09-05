## Snail.Toolkit.Redis

Redis extensions over `StackExchange.Redis` for .NET 10, with Microsoft-only dependencies:

- one shared `IConnectionMultiplexer`, opened at host start, that follows a rotated password and a changed endpoint without a restart;
- `IRedisCache` — JSON cache with a fallback factory, single-flight misses and a fail-open policy;
- `IRedisPubSub` — typed publish with a receiver count and `IAsyncEnumerable<T>` subscriptions that share one Redis subscription per channel;
- `IRedisStream` — append-only history a late reader catches up on before it follows;
- a health check, counters under the meter `Snail.Toolkit.Redis`, and AOT-compatible serialization.

Values are serialized with `System.Text.Json` (web defaults); a source-generated context can replace reflection through the `Json` option of each feature.

| Namespace | Contents |
|---|---|
| `Snail.Toolkit.Redis` | `RedisOptions`, `RedisMetrics` |
| `Snail.Toolkit.Redis.Extensions` | `AddRedis`, `AddRedisCache`, `AddRedisDistributedCache`, `AddRedisPubSub`, `AddRedisStreams`, `AddRedisHealthCheck` |
| `Snail.Toolkit.Redis.Cache` | `IRedisCache`, `RedisCacheOptions`, `CacheFailure` |
| `Snail.Toolkit.Redis.PubSub` | `IRedisPubSub`, `IRedisSubscription<T>`, `RedisMessage<T>`, `RedisPubSubOptions` |
| `Snail.Toolkit.Redis.Streams` | `IRedisStream`, `RedisStreamEntry<T>`, `RedisStreamOptions` |

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

`Connection` is a StackExchange.Redis configuration string (several hosts, `ssl=true`, etc. are allowed). The password and `abortConnect` are refused inside it: they come from `Password` and `AbortOnConnectFail`, so there is one source of truth for rotation.
`InstanceName` prefixes cache keys and is empty by default. With `AbortOnConnectFail = false` the host starts while Redis is down and reconnects later.

The options are validated at host start: a blank or unparsable connection string, a zero buffer or a negative expiry stop the host instead of the first command.

### Registration

Each `Add*` registers its own subsystem and reuses the shared connection, so they can be combined freely:

```c#
using Snail.Toolkit.Redis.Extensions;

builder.Services
    .AddRedisCache(builder.Configuration)        // IRedisCache
    .AddRedisPubSub(builder.Configuration)       // IRedisPubSub
    .AddRedisStreams(builder.Configuration)      // IRedisStream
    .AddRedisHealthCheck(builder.Configuration); // health check "redis"
```

`AddRedis(configuration)` alone registers `RedisOptions`, `IConnectionMultiplexer` and a hosted warm-up that opens the connection asynchronously at host start. The first call binds the options; later calls are ignored.
The host must have logging registered (`AddLogging()` or a generic host).

### Cache

```c#
var config = await cache.GetAsync(
    "settings",
    token => LoadSettingsAsync(token),     // called on a miss with the same token, a non-null result is stored
    new DistributedCacheEntryOptions
    {
        AbsoluteExpiration = DateTimeOffset.UtcNow.AddMinutes(30),
        SlidingExpiration = TimeSpan.FromMinutes(10)
    },
    cancellationToken);
```

Without options an entry lives 10 minutes absolute with a 2 minute sliding window. Concurrent misses of one key inside the process share a single factory call. An entry that no longer deserializes is evicted and reported as a miss. Storing `null` throws: null is the miss marker.

When Redis is unreachable a read reports a miss and a write is skipped, with one warning per minute, so the host keeps serving from the factory; `RedisCacheOptions.OnFailure = CacheFailure.Throw` propagates the error instead. A failed `RemoveAsync` always throws, because stale data left behind is something the caller has to know.

```c#
builder.Services.AddRedisCache(builder.Configuration, options =>
{
    options.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1);
    options.SlidingExpiration = null;
    options.OnFailure = CacheFailure.Throw;
    options.Json.PropertyNamingPolicy = null;
});
```

Entries use the hash layout of Microsoft's distributed cache. A consumer that needs `IDistributedCache` itself (session, output caching) adds it with `AddRedisDistributedCache(configuration)`: it opens a connection of its own, because Microsoft's implementation disposes the multiplexer it is given, and it reads and writes the same entries as `IRedisCache`.

### Pub/Sub

Best-effort delivery: messages are not persisted and late subscribers miss earlier messages. Use a stream when that matters.

```c#
// publisher: waits for the broker and returns how many subscribers received the message
var receivers = await pubSub.PublishAsync($"task:{taskId}:events", envelope, cancellationToken);
```

Three ways to consume; readers of one channel share a single Redis subscription and a single deserialization:

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

Each reader buffers up to `BufferCapacity` messages (1024 by default) ahead of a slow consumer and drops the oldest when full; the first drop and every thousandth are logged at `Warning`.
Payloads that fail to deserialize are logged at `Warning` and skipped. Publishing `null` throws.

```c#
builder.Services.AddRedisPubSub(builder.Configuration, options =>
{
    options.BufferCapacity = 4096;
    options.Json.PropertyNamingPolicy = null;
});
```

### Streams

A stream keeps its entries, so a page that opens mid-task reads what it missed and then follows:

```c#
// producer: returns the entry id; keep roughly the last 10 000 entries
var id = await stream.AppendAsync($"task:{taskId}", delta, maxLength: 10_000, cancellationToken);

// consumer: everything after the last id it saw (all entries when null), then live entries until cancelled
await foreach (var entry in stream.ReadAsync<Delta>($"task:{taskId}", after: lastSeenId, cancellationToken))
    lastSeenId = entry.Id;
```

Every append is announced over pub/sub, so a follower wakes at once; `RedisStreamOptions.PollInterval` (one second) only covers a lost announcement, and `PageSize` (100) bounds a catch-up round trip.

### Health check

`AddRedisHealthCheck(configuration, name: "redis", failureStatus: HealthStatus.Unhealthy, tags: null)` registers a check that PINGs the shared connection and reports the latency in `Data["latencyMs"]`. The name is registered once even when the call is repeated. Expose it as usual:

```c#
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") });
```

### Password rotation and endpoint changes

`RedisOptions` is bound with change tracking. When the configuration reloads with a new `Password` (for example from a Vault provider), the running connection keeps its authenticated sockets and every later reconnect uses the new password. When it reloads with a new `Connection`, a connection to the new endpoints is opened; the cache, pub/sub subscriptions, streams and the health check move to it, and the previous connection is disposed a few seconds later. A reload the library cannot validate is logged at `Error` and ignored.

The `IConnectionMultiplexer` a consumer resolved itself is a singleton and stays on the endpoints it was opened with.

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

### Observability

Connection failures, restores and server errors are logged. Counters are published under the meter `Snail.Toolkit.Redis` (`RedisMetrics.MeterName`): cache hits, misses and degraded calls, messages published, received and dropped, connection failures, restores and replacements.

```c#
builder.Services.AddOpenTelemetry().WithMetrics(metrics => metrics.AddMeter(RedisMetrics.MeterName));
```

### Ahead-of-time compilation

The library is marked AOT-compatible. Serialization goes through the resolver of the JSON options, so a source-generated context makes it work without reflection:

```c#
builder.Services.AddRedisCache(builder.Configuration, options => options.Json.TypeInfoResolver = AppJsonContext.Default);
```

### Tests

Unit tests run everywhere. Integration tests start `redis:7-alpine` containers through Testcontainers and are skipped when no Docker daemon is reachable.

## License

Snail.Toolkit.Redis is a free and open source project, released under the permissible [MIT license](LICENSE).
