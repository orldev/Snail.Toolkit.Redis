using System.Text.Json;

namespace Snail.Toolkit.Redis.Streams;

/// <summary>Serialization and reading settings for <see cref="IRedisStream"/>.</summary>
public sealed class RedisStreamOptions
{
    /// <summary>JSON options for entries; web defaults (camelCase, case-insensitive) unless replaced.</summary>
    public JsonSerializerOptions Json { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>Entries fetched per round trip while catching up; 100 by default.</summary>
    public int PageSize { get; set; } = 100;

    /// <summary>How long a follower waits for an announcement before it checks the stream anyway; one second by default.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
}
