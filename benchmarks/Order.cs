namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>A cached or published value of an everyday size: a few scalars, a date and a short list.</summary>
public sealed record Order(Guid Id, string Customer, decimal Amount, DateTime CreatedAt, string[] Tags, int Quantity)
{
    /// <summary>The same order every time, so every run serializes the same bytes.</summary>
    public static Order Sample() =>
        new(
            Guid.Parse("6f1c2a43-9a51-4d8f-b0f1-3c5e7d2a9b10"),
            "ACME Corporation, Berlin office",
            1234.56m,
            new DateTime(2026, 10, 12, 8, 30, 0, DateTimeKind.Utc),
            ["priority", "export", "b2b"],
            7);
}
