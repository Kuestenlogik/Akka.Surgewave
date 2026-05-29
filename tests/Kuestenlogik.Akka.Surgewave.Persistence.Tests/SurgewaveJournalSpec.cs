namespace Kuestenlogik.Akka.Surgewave.Persistence.Tests;

using global::Akka.Persistence.TCK.Journal;
using Xunit;

/// <summary>
/// Runs the full Akka.NET Journal TCK against a real Surgewave broker,
/// started in-process by <see cref="SurgewaveBrokerFixture"/>.
/// </summary>
[Collection(SurgewaveBrokerCollection.Name)]
public class SurgewaveJournalSpec : JournalSpec
{
    public SurgewaveJournalSpec(SurgewaveBrokerFixture broker)
        : base(SurgewaveJournalSpecConfig.Create(broker.BootstrapServers), nameof(SurgewaveJournalSpec))
    {
        Initialize();
    }

    protected override bool SupportsRejectingNonSerializableObjects => true;
}
