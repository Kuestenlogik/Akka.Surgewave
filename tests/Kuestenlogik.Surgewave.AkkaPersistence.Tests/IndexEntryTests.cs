namespace Kuestenlogik.Surgewave.AkkaPersistence.Tests;

using Kuestenlogik.Surgewave.AkkaPersistence.Journal;
using Xunit;

public class IndexEntryTests
{
    [Fact]
    public void Should_have_sensible_defaults()
    {
        var entry = new IndexEntry();

        Assert.Equal(0L, entry.HighestSequenceNr);
        Assert.Equal(0, entry.Partition);
        Assert.Equal(0L, entry.FirstOffset);
        Assert.Equal(0L, entry.LastOffset);
        Assert.Equal(0L, entry.DeletedToSequenceNr);
        Assert.Empty(entry.Tags);
        Assert.Equal(default, entry.LastUpdated);
    }

    [Fact]
    public void Should_support_with_expression()
    {
        var entry = new IndexEntry
        {
            HighestSequenceNr = 100,
            Partition = 3,
            FirstOffset = 500,
            LastOffset = 600,
            Tags = ["position", "status"]
        };

        var updated = entry with { HighestSequenceNr = 200, LastOffset = 700 };

        Assert.Equal(200, updated.HighestSequenceNr);
        Assert.Equal(700, updated.LastOffset);
        Assert.Equal(3, updated.Partition);
        Assert.Equal(500, updated.FirstOffset);
        Assert.Equal(["position", "status"], updated.Tags);
    }
}
