namespace Kuestenlogik.Surgewave.AkkaPersistence.Journal;

/// <summary>
/// Compact summary stored in the compacted index topic per PersistenceId.
/// Enables fast replay without full topic scan.
/// </summary>
public sealed record IndexEntry
{
    public long HighestSequenceNr { get; init; }
    public int Partition { get; init; }
    public long FirstOffset { get; init; }
    public long LastOffset { get; init; }
    public long DeletedToSequenceNr { get; init; }
    public string[] Tags { get; init; } = [];
    public DateTimeOffset LastUpdated { get; init; }
}
