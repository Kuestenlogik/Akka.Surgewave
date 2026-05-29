namespace Kuestenlogik.Akka.Surgewave.Persistence.Tests;

using Kuestenlogik.Surgewave.Runtime;
using Xunit;

/// <summary>
/// Starts an in-process Surgewave broker (in-memory storage, auto-created
/// topics) once for the whole TCK collection, so the Akka.Persistence
/// Journal- and SnapshotStore-TCK specs run against a real broker without
/// needing an external one. The auto-assigned port avoids collisions; specs
/// read <see cref="BootstrapServers"/> and feed it into their HOCON.
/// </summary>
public sealed class SurgewaveBrokerFixture : IAsyncLifetime
{
    private SurgewaveRuntime? _runtime;

    public string BootstrapServers { get; private set; } = "localhost:9092";

    public async Task InitializeAsync()
    {
        _runtime = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0) // auto-assign — no fixed-port collision in CI/local
            .WithStorageEngine("memory")
            .WithAutoCreateTopics(true)
            .WithPartitions(4)
            .Build()
            .StartAsync();

        BootstrapServers = _runtime.BootstrapServers;
    }

    public async Task DisposeAsync()
    {
        if (_runtime is not null)
            await _runtime.DisposeAsync();
    }
}

/// <summary>
/// Collection that shares one <see cref="SurgewaveBrokerFixture"/> across the
/// TCK specs and runs them sequentially (the specs share the broker; serial
/// execution keeps topic/offset state predictable).
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SurgewaveBrokerCollection : ICollectionFixture<SurgewaveBrokerFixture>
{
    public const string Name = "SurgewaveBroker";
}
