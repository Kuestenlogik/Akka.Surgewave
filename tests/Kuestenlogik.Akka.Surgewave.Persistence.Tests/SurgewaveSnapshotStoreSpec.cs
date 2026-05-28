namespace Kuestenlogik.Akka.Surgewave.Persistence.Tests;

using global::Akka.Persistence.TCK.Snapshot;

/// <summary>
/// Runs the full Akka.NET SnapshotStore TCK against Surgewave.
/// Requires a running Surgewave broker at localhost:9092.
/// </summary>
public class SurgewaveSnapshotStoreSpec : SnapshotStoreSpec
{
    public SurgewaveSnapshotStoreSpec()
        : base(SurgewaveSnapshotSpecConfig.Create(), nameof(SurgewaveSnapshotStoreSpec))
    {
        Initialize();
    }
}
