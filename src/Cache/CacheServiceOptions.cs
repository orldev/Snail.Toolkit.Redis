using System.Text.Json;

namespace Snail.Toolkit.Redis.Cache;

/// <summary>Serialization and default expiration settings for <see cref="ICacheService"/>.</summary>
public sealed class CacheServiceOptions
{
    /// <summary>JSON options for cached values; web defaults (camelCase, case-insensitive) unless replaced.</summary>
    public JsonSerializerOptions Json { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>Absolute lifetime applied when an entry is stored without explicit options; 10 minutes by default.</summary>
    public TimeSpan? AbsoluteExpirationRelativeToNow { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Sliding lifetime applied when an entry is stored without explicit options; 2 minutes by default.</summary>
    public TimeSpan? SlidingExpiration { get; set; } = TimeSpan.FromMinutes(2);
}
