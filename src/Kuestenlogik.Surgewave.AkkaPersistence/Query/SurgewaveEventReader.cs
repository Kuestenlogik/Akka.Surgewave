namespace Kuestenlogik.Surgewave.AkkaPersistence.Query;

using Akka;
using Akka.Actor;
using Akka.Persistence.Query;
using Kuestenlogik.Surgewave.AkkaPersistence.Serialization;
using Akka.Util;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Reads events for a specific PersistenceId from Surgewave.
/// Used by EventsByPersistenceId and CurrentEventsByPersistenceId queries.
/// </summary>
internal sealed class SurgewaveEventReader
{
    private readonly SurgewaveReadJournalSettings _settings;
    private readonly ExtendedActorSystem _system;
    private readonly string _persistenceId;
    private readonly long _fromSequenceNr;
    private readonly bool _liveTail;
    private IConsumer<string, byte[]>? _consumer;
    private ISurgewaveEventSerializer? _serializer;
    private long _currentOffset;

    public SurgewaveEventReader(
        SurgewaveReadJournalSettings settings,
        ExtendedActorSystem system,
        string persistenceId,
        long fromSequenceNr,
        bool liveTail)
    {
        _settings = settings;
        _system = system;
        _persistenceId = persistenceId;
        _fromSequenceNr = fromSequenceNr;
        _liveTail = liveTail;
    }

    public async Task InitializeAsync()
    {
        _serializer = new OpaqueEventSerializer(_system);

        _consumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
            opts.GroupId = $"akka-query-{_persistenceId}-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        var topic = _settings.ResolveTopicName(_settings.JournalTopic);
        await _consumer.SubscribeAsync(CancellationToken.None, topic);
    }

    public async Task<Option<EventEnvelope>> ReadNextAsync(long toSequenceNr)
    {
        while (true)
        {
            var timeout = _liveTail ? _settings.RefreshInterval : TimeSpan.FromSeconds(2);
            var result = await _consumer!.ConsumeAsync(timeout, CancellationToken.None);

            if (result is null)
                return _liveTail ? default : Option<EventEnvelope>.None;

            if (result.Key != _persistenceId)
                continue;

            if (result.Headers is null ||
                !result.Headers.TryGetValue(EventEnvelopeCodec.SequenceNrHeader, out var seqNrBytes))
                continue;

            var seqNr = EventEnvelopeCodec.DecodeLong(seqNrBytes);

            if (seqNr < _fromSequenceNr)
                continue;
            if (seqNr > toSequenceNr)
                return Option<EventEnvelope>.None;

            var persistent = _serializer!.Deserialize(
                _persistenceId, seqNr, result.Value, result.Headers);

            var offset = Offset.Sequence(result.Offset);
            _currentOffset = result.Offset;

            return Option<EventEnvelope>.Create(
                new EventEnvelope(offset, _persistenceId, seqNr, persistent.Payload));
        }
    }

    public async Task<Done> CloseAsync()
    {
        if (_consumer is IAsyncDisposable disposable)
            await disposable.DisposeAsync();
        return Done.Instance;
    }
}
