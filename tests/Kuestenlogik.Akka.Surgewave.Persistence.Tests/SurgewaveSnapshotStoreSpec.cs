namespace Kuestenlogik.Akka.Surgewave.Persistence.Tests;

using global::Akka.Persistence.TCK.Snapshot;
using Xunit;

/// <summary>
/// Runs the full Akka.NET SnapshotStore TCK against a real Surgewave broker,
/// started in-process by <see cref="SurgewaveBrokerFixture"/>.
/// </summary>
[Collection(SurgewaveBrokerCollection.Name)]
public class SurgewaveSnapshotStoreSpec : SnapshotStoreSpec
{
    public SurgewaveSnapshotStoreSpec(SurgewaveBrokerFixture broker, ITestOutputHelper output)
        : base(SurgewaveSnapshotSpecConfig.Create(broker.BootstrapServers), nameof(SurgewaveSnapshotStoreSpec), output)
    {
        Initialize();
    }
}
