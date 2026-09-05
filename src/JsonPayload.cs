using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Snail.Toolkit.Redis;

/// <summary>Serializes through the resolver of the options, so a source-generated context makes the library AOT-safe.</summary>
/// <remarks>
/// The generic overloads that take options are annotated as requiring dynamic code; going through
/// <see cref="JsonSerializerOptions.GetTypeInfo"/> is not. Options without a resolver get the reflection one where
/// reflection is enabled, which the feature switch turns off under trimming and ahead-of-time compilation.
/// </remarks>
internal static class JsonPayload
{
    public static byte[] Write<T>(T value, JsonSerializerOptions options) =>
        JsonSerializer.SerializeToUtf8Bytes(value, TypeInfo<T>(options));

    public static T? Read<T>(ReadOnlySpan<byte> payload, JsonSerializerOptions options) =>
        JsonSerializer.Deserialize(payload, TypeInfo<T>(options));

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Guarded by JsonSerializer.IsReflectionEnabledByDefault, which is false when trimming.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Guarded by JsonSerializer.IsReflectionEnabledByDefault, which is false ahead of time.")]
    public static JsonSerializerOptions Prepared(JsonSerializerOptions options)
    {
        if (options.TypeInfoResolver is null && !options.IsReadOnly && JsonSerializer.IsReflectionEnabledByDefault)
            options.TypeInfoResolver = new DefaultJsonTypeInfoResolver();

        return options;
    }

    private static JsonTypeInfo<T> TypeInfo<T>(JsonSerializerOptions options) => (JsonTypeInfo<T>)options.GetTypeInfo(typeof(T));
}
