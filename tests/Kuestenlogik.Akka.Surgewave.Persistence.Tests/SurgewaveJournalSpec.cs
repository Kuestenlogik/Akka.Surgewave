namespace Kuestenlogik.Akka.Surgewave.Persistence.Tests;

using global::Akka.Persistence.TCK.Journal;

/// <summary>
/// Runs the full Akka.NET Journal TCK against Surgewave.
/// Requires a running Surgewave broker at localhost:9092.
/// </summary>
public class SurgewaveJournalSpec : JournalSpec
{
    public SurgewaveJournalSpec()
        : base(SurgewaveJournalSpecConfig.Create(), nameof(SurgewaveJournalSpec))
    {
        Initialize();
    }

    protected override bool SupportsRejectingNonSerializableObjects => true;
}
