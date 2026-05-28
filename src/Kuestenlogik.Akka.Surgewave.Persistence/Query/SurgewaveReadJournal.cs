namespace Kuestenlogik.Akka.Surgewave.Persistence.Query;

using global::Akka.Actor;
using global::Akka.Configuration;
using global::Akka.Persistence.Query;
using global::Akka.Streams.Dsl;
using Kuestenlogik.Akka.Surgewave.Persistence.Journal;
using Kuestenlogik.Akka.Surgewave.Persistence.Serialization;
using Kuestenlogik.Surgewave.Client;

/// <summary>
/// Persistence Query implementation backed by Surgewave.
/// Supports EventsByPersistenceId, EventsByTag, AllEvents in both
/// live-tail and current (finite) modes.
/// </summary>
public sealed class SurgewaveReadJournal :
    IEventsByPersistenceIdQuery,
    IEventsByTagQuery,
    IAllEventsQuery,
    ICurrentEventsByPersistenceIdQuery,
    ICurrentEventsByTagQuery,
    ICurrentAllEventsQuery
{
    public static string Identifier => "akka.persistence.query.surgewave-read-journal";

    private readonly ExtendedActorSystem _system;
    private readonly SurgewaveReadJournalSettings _settings;

    public SurgewaveReadJournal(ExtendedActorSystem system, Config config)
    {
        _system = system;
        _settings = new SurgewaveReadJournalSettings(config);
    }

    /// <summary>
    /// Live-tailing query: emits events as they are written.
    /// Completes only when the stream is cancelled.
    /// </summary>
    public Source<EventEnvelope, NotUsed> EventsByPersistenceId(
        string persistenceId, long fromSequenceNr, long toSequenceNr)
    {
        return Source.UnfoldResourceAsync<EventEnvelope, SurgewaveEventReader>(
            () => CreateReaderAsync(persistenceId, fromSequenceNr, liveTail: true),
            reader => reader.ReadNextAsync(toSequenceNr),
            reader => reader.CloseAsync());
    }

    /// <summary>
    /// Finite query: returns all current events and completes.
    /// </summary>
    public Source<EventEnvelope, NotUsed> CurrentEventsByPersistenceId(
        string persistenceId, long fromSequenceNr, long toSequenceNr)
    {
        return Source.UnfoldResourceAsync<EventEnvelope, SurgewaveEventReader>(
            () => CreateReaderAsync(persistenceId, fromSequenceNr, liveTail: false),
            reader => reader.ReadNextAsync(toSequenceNr),
            reader => reader.CloseAsync());
    }

    /// <summary>
    /// Events filtered by tag. Reads all partitions and filters
    /// by the akka-tags header.
    /// </summary>
    public Source<EventEnvelope, NotUsed> EventsByTag(string tag, Offset offset)
    {
        return Source.UnfoldResourceAsync<EventEnvelope, SurgewaveTagReader>(
            () => CreateTagReaderAsync(tag, offset, liveTail: true),
            reader => reader.ReadNextAsync(),
            reader => reader.CloseAsync());
    }

    public Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, Offset offset)
    {
        return Source.UnfoldResourceAsync<EventEnvelope, SurgewaveTagReader>(
            () => CreateTagReaderAsync(tag, offset, liveTail: false),
            reader => reader.ReadNextAsync(),
            reader => reader.CloseAsync());
    }

    /// <summary>
    /// All events across all PersistenceIds.
    /// </summary>
    public Source<EventEnvelope, NotUsed> AllEvents(Offset offset)
    {
        return Source.UnfoldResourceAsync<EventEnvelope, SurgewaveAllEventsReader>(
            () => CreateAllEventsReaderAsync(offset, liveTail: true),
            reader => reader.ReadNextAsync(),
            reader => reader.CloseAsync());
    }

    public Source<EventEnvelope, NotUsed> CurrentAllEvents(Offset offset)
    {
        return Source.UnfoldResourceAsync<EventEnvelope, SurgewaveAllEventsReader>(
            () => CreateAllEventsReaderAsync(offset, liveTail: false),
            reader => reader.ReadNextAsync(),
            reader => reader.CloseAsync());
    }

    private async Task<SurgewaveEventReader> CreateReaderAsync(
        string persistenceId, long fromSequenceNr, bool liveTail)
    {
        var reader = new SurgewaveEventReader(_settings, _system, persistenceId, fromSequenceNr, liveTail);
        await reader.InitializeAsync();
        return reader;
    }

    private async Task<SurgewaveTagReader> CreateTagReaderAsync(
        string tag, Offset offset, bool liveTail)
    {
        var reader = new SurgewaveTagReader(_settings, _system, tag, offset, liveTail);
        await reader.InitializeAsync();
        return reader;
    }

    private async Task<SurgewaveAllEventsReader> CreateAllEventsReaderAsync(
        Offset offset, bool liveTail)
    {
        var reader = new SurgewaveAllEventsReader(_settings, _system, offset, liveTail);
        await reader.InitializeAsync();
        return reader;
    }
}
