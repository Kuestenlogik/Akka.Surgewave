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
    // ITestOutputHelper passes through to the TCK's `Output` property; the
    // "reject non-serializable events" test calls Output.WriteLine and NREs
    // if the base wasn't given a writer.
    public SurgewaveJournalSpec(SurgewaveBrokerFixture broker, ITestOutputHelper output)
        : base(SurgewaveJournalSpecConfig.Create(broker.BootstrapServers), nameof(SurgewaveJournalSpec), output)
    {
        Initialize();
    }

    protected override bool SupportsRejectingNonSerializableObjects => true;
}
