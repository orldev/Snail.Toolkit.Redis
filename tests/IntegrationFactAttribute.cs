using System.Diagnostics;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Marks a test that requires Docker; skipped only when no Docker daemon is reachable.</summary>
public sealed class IntegrationFactAttribute : FactAttribute
{
    private static readonly Lazy<bool> DockerAvailable = new(DetectDocker);

    public IntegrationFactAttribute()
    {
        if (!DockerAvailable.Value)
            Skip = "Docker is not available; Redis integration tests were skipped";
    }

    private static bool DetectDocker()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "info",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            });

            if (process is null)
                return false;

            if (!process.WaitForExit(10_000))
            {
                process.Kill(entireProcessTree: true);
                return false;
            }

            return process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
