using System.Globalization;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Metrology;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>
/// Runs the benchmarks named on the command line, or offers all of them; <c>probe silent|refused N</c> runs the connect
/// probe instead, <c>dump</c> the command dump. Allocations are reported in bytes, whatever their size, so that a
/// change of a few bytes shows.
/// </summary>
public static class Program
{
    /// <summary>Hands the command line to BenchmarkDotNet, or to <see cref="ConnectProbe"/> or <see cref="CommandDump"/> when it asks for them.</summary>
    public static async Task Main(string[] args)
    {
        if (args is ["probe", var target, var callers])
        {
            ConnectProbe.Run(target, int.Parse(callers, CultureInfo.InvariantCulture));
            return;
        }

        if (args is ["dump"])
        {
            await CommandDump.RunAsync().ConfigureAwait(false);
            return;
        }

        var config = DefaultConfig.Instance.WithSummaryStyle(SummaryStyle.Default.WithSizeUnit(SizeUnit.B));

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
