using System.Text.Json;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary>Serialization and buffering settings for <see cref="IRedisPubSub"/>.</summary>
public sealed class RedisPubSubOptions
{
    /// <summary>JSON options for messages; web defaults (camelCase, case-insensitive) unless replaced.</summary>
    public JsonSerializerOptions Json { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>Messages buffered per subscription ahead of a slow consumer; the oldest are dropped when the buffer is full.</summary>
    public int BufferCapacity { get; set; } = 1024;
}
