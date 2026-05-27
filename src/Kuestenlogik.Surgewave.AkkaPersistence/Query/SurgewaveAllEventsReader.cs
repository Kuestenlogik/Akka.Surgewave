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
/// Reads all events across all PersistenceIds from Surgewave.
/// Uses a consumer group to read all partitions.
/// </summary>
internal sealed class SurgewaveAllEventsReader
{
    private readonly SurgewaveReadJournalSettings _settings;
    private readonly ExtendedActorSystem _system;
    private readonly Offset _startOffset;
    private readonly bool _liveTail;
    private IConsumer<string, byte[]>? _consumer;
    private ISurgewaveEventSerializer? _serializer;

    public SurgewaveAllEventsReader(
        SurgewaveReadJournalSettings settings,
        ExtendedActorSystem system,
        Offset startOffset,
        bool liveTail)
    {
        _settings = settings;
        _system = system;
        _startOffset = startOffset;
        _liveTail = liveTail;
    }

    public async Task InitializeAsync()
    {
        _serializer = new OpaqueEventSerializer(_system);

        _consumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
            opts.GroupId = _liveTail
                ? $"akka-query-all-{Guid.NewGuid():N}"
                : _settings.ConsumerGroup;
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        var topic = _settings.ResolveTopicName(_settings.JournalTopic);
        await _consumer.SubscribeAsync(CancellationToken.None, topic);
    }

    public async Task<Option<EventEnvelope>> ReadNextAsync()
    {
        var timeout = _liveTail ? _settings.RefreshInterval : TimeSpan.FromSeconds(2);
        var result = await _consumer!.ConsumeAsync(timeout, CancellationToken.None);

        if (result is null)
            return _liveTail ? default : Option<EventEnvelope>.None;

        if (result.Headers is null ||
            !result.Headers.TryGetValue(EventEnvelopeCodec.SequenceNrHeader, out var seqNrBytes))
        {
            // Skip non-Akka messages or index entries
            return _liveTail ? default : Option<EventEnvelope>.None;
        }

        var seqNr = EventEnvelopeCodec.DecodeLong(seqNrBytes);
        var persistenceId = result.Key ?? "";

        var persistent = _serializer!.Deserialize(
            persistenceId, seqNr, result.Value, result.Headers);

        var offset = Offset.Sequence(result.Offset);

        return Option<EventEnvelope>.Create(
            new EventEnvelope(offset, persistenceId, seqNr, persistent.Payload));
    }

    public async Task<Done> CloseAsync()
    {
        if (_consumer is IAsyncDisposable disposable)
            await disposable.DisposeAsync();
        return Done.Instance;
    }
}
