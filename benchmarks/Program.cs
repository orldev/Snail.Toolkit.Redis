using System.Globalization;
using BenchmarkDotNet.Running;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>Runs the benchmarks named on the command line, or offers all of them; <c>probe silent|refused N</c> runs the connect probe instead.</summary>
public static class Program
{
    /// <summary>Hands the command line to BenchmarkDotNet, or to <see cref="ConnectProbe"/> when it asks for the probe.</summary>
    public static void Main(string[] args)
    {
        if (args is ["probe", var target, var callers])
        {
            ConnectProbe.Run(target, int.Parse(callers, CultureInfo.InvariantCulture));
            return;
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
