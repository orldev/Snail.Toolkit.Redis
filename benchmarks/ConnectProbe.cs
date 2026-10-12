using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Snail.Toolkit.Redis.Cache;

namespace Snail.Toolkit.Redis.Benchmarks;

/// <summary>What callers of the cache do to the thread pool while Redis cannot be reached: the wait every other piece of work pays.</summary>
/// <remarks>
/// <para>
/// Not a BenchmarkDotNet run: the cost is not in the calls themselves but in the threads they hold. The callers arrive at
/// once, as requests do, each reading the cache; Redis is a port on 127.0.0.1 in this process, either silent (it accepts
/// and never answers, so every connect runs into the connect timeout) or refused (nothing listens). A separate thread
/// drops a trivial work item into the pool every 50 ms and records how long it waited for a thread.
/// </para>
/// <para>Run it with <c>dotnet run -c Release --project benchmarks -- probe silent 64</c> (or <c>refused</c>).</para>
/// </remarks>
internal static class ConnectProbe
{
    private const string Key = "order:6f1c2a43-9a51-4d8f-b0f1-3c5e7d2a9b10";
    private static readonly TimeSpan ProbeInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>Runs the callers against the given kind of unreachable Redis and prints what the pool went through.</summary>
    public static void Run(string target, int callers)
    {
        using var redis = target == "refused" ? Unreachable.Refused() : Unreachable.Silent();
        var settings = new RedisOptions
        {
            Connection = $"127.0.0.1:{redis.Port.ToString(CultureInfo.InvariantCulture)},connectTimeout=1000,connectRetry=1",
            AbortOnConnectFail = true
        };
        using var connection = new RedisConnection(Options.Create(settings), new OptionsFactory<RedisOptions>([], []), [], NullLogger<RedisConnection>.Instance);
        var cache = new RedisCache(connection, Options.Create(settings), Options.Create(new RedisCacheOptions()), NullLogger<RedisCache>.Instance);
        var pool = new PoolProbe();

        pool.Start();

        var clock = Stopwatch.StartNew();
        var reads = new Task[callers];

        for (var caller = 0; caller < callers; caller++)
            reads[caller] = Task.Run(() => cache.GetAsync<Order>(Key));

        while (!Array.TrueForAll(reads, read => read.IsCompleted))
            Thread.Sleep(5);

        var elapsed = clock.Elapsed;
        var (median, longest, peak) = pool.Stop();
        var line = string.Create(
            CultureInfo.InvariantCulture,
            $"{target,-8} callers={callers} all done in {elapsed.TotalMilliseconds:F0} ms; pool threads peak {peak}; probe wait p50 {median:F1} ms, max {longest:F0} ms");

        Console.WriteLine(line);
    }

    /// <summary>Queues a trivial work item at a fixed interval from its own thread and keeps how long each waited.</summary>
    private sealed class PoolProbe
    {
        private readonly List<double> _waits = [];
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private readonly Thread _thread;
        private volatile bool _stopped;
        private int _peak = ThreadPool.ThreadCount;

        public PoolProbe() => _thread = new Thread(Probe) { IsBackground = true };

        public void Start() => _thread.Start();

        /// <summary>Gives the last queued items time to run, then stops and answers the median and longest wait and the peak thread count.</summary>
        public (double Median, double Longest, int Peak) Stop()
        {
            Thread.Sleep(300);
            _stopped = true;
            _thread.Join();

            double[] waits;

            lock (_waits)
                waits = [.. _waits];

            Array.Sort(waits);

            return (waits[waits.Length / 2], waits[^1], _peak);
        }

        private void Probe()
        {
            while (!_stopped)
            {
                var queuedAt = _clock.Elapsed.TotalMilliseconds;

                ThreadPool.UnsafeQueueUserWorkItem(_ => Record(_clock.Elapsed.TotalMilliseconds - queuedAt), null);
                _peak = Math.Max(_peak, ThreadPool.ThreadCount);
                Thread.Sleep(ProbeInterval);
            }
        }

        private void Record(double wait)
        {
            lock (_waits)
                _waits.Add(wait);
        }
    }

    /// <summary>A port on 127.0.0.1 where no Redis answers.</summary>
    private sealed class Unreachable : IDisposable
    {
        private readonly TcpListener? _listener;
        private readonly List<Socket> _held = [];

        private Unreachable(int port, TcpListener? listener)
        {
            Port = port;
            _listener = listener;
        }

        public int Port { get; }

        /// <summary>A listener that accepts every connection and never says a word.</summary>
        public static Unreachable Silent()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);

            listener.Start();

            var silent = new Unreachable(((IPEndPoint)listener.LocalEndpoint).Port, listener);

            new Thread(silent.Accept) { IsBackground = true }.Start();

            return silent;
        }

        /// <summary>A port that was free a moment ago and has nothing listening on it, so every connect is refused.</summary>
        public static Unreachable Refused()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);

            listener.Start();

            var port = ((IPEndPoint)listener.LocalEndpoint).Port;

            listener.Stop();

            return new Unreachable(port, null);
        }

        public void Dispose()
        {
            _listener?.Stop();

            lock (_held)
            {
                foreach (var socket in _held)
                    socket.Dispose();
            }
        }

        private void Accept()
        {
            try
            {
                while (true)
                {
                    var socket = _listener!.AcceptSocket();

                    lock (_held)
                        _held.Add(socket);
                }
            }
            catch (SocketException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
        }
    }
}
