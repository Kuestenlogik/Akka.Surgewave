namespace Kuestenlogik.Surgewave.AkkaPersistence.Tests;

using Kuestenlogik.Surgewave.AkkaPersistence.Query;
using Xunit;

/// <summary>
/// Tests for SurgewaveReadJournal persistence query implementation.
/// Requires a running Surgewave broker at localhost:9092.
/// </summary>
public class SurgewaveReadJournalSpec
{
    [Fact]
    public void SurgewaveReadJournal_should_have_correct_identifier()
    {
        Assert.Equal("akka.persistence.query.surgewave-read-journal", SurgewaveReadJournal.Identifier);
    }
}
