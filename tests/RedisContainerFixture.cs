using Testcontainers.Redis;

namespace Snail.Toolkit.Redis.Tests;

/// <summary>Starts a single Redis container lazily and shares it across the tests of a class.</summary>
public sealed class RedisContainerFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private RedisContainer? _container;

    public async Task<string> GetConnectionStringAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_container is null)
            {
                _container = new RedisBuilder("redis:7-alpine").Build();
                await _container.StartAsync();
            }

            return _container.GetConnectionString();
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }
}
