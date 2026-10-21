using System.Text.Json;

namespace Snail.Toolkit.Redis.PubSub;

/// <summary>Serialization and buffering settings for <see cref="IRedisPubSub"/>.</summary>
public sealed class RedisPubSubOptions
{
    /// <summary>JSON options for messages; web defaults (camelCase, case-insensitive) unless replaced.</summary>
    /// <remarks>
    /// The instance becomes the library's once the feature is resolved: without a resolver it gets the reflection one
    /// where reflection is enabled, and its first use makes it read-only, as System.Text.Json does with any options it
    /// serializes with. Configure it in the registration and share it only with code that is done changing it.
    /// </remarks>
    public JsonSerializerOptions Json { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>Messages buffered per subscription ahead of a slow consumer; the oldest are dropped when the buffer is full.</summary>
    public int BufferCapacity { get; set; } = 1024;
}
