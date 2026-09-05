using System.Text.Json;

namespace Snail.Toolkit.Redis.Cache;

/// <summary>Serialization, default expiration and failure settings for <see cref="IRedisCache"/>.</summary>
public sealed class RedisCacheOptions
{
    /// <summary>JSON options for cached values; web defaults (camelCase, case-insensitive) unless replaced.</summary>
    public JsonSerializerOptions Json { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>Absolute lifetime applied when an entry is stored without explicit options; 10 minutes by default.</summary>
    public TimeSpan? AbsoluteExpirationRelativeToNow { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Sliding lifetime applied when an entry is stored without explicit options; 2 minutes by default.</summary>
    public TimeSpan? SlidingExpiration { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>What a read or write does when Redis is unreachable; a miss by default so the host keeps serving from the factory.</summary>
    public CacheFailure OnFailure { get; set; } = CacheFailure.Miss;
}
