namespace Kuestenlogik.Surgewave.AkkaPersistence.Query;

using Akka.Actor;
using Akka.Persistence.Query;
using Kuestenlogik.Surgewave.AkkaPersistence.Serialization;
using Akka.Util;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Reads events filtered by tag from Surgewave.
/// Uses header-based filtering (Option A from the concept).
/// </summary>
internal sealed class SurgewaveTagReader
{
    private readonly SurgewaveReadJournalSettings _settings;
    private readonly ExtendedActorSystem _system;
    private readonly string _tag;
    private readonly Offset _startOffset;
    private readonly bool _liveTail;
    private IConsumer<string, byte[]>? _consumer;
    private ISurgewaveEventSerializer? _serializer;

    public SurgewaveTagReader(
        SurgewaveReadJournalSettings settings,
        ExtendedActorSystem system,
        string tag,
        Offset startOffset,
        bool liveTail)
    {
        _settings = settings;
        _system = system;
        _tag = tag;
        _startOffset = startOffset;
        _liveTail = liveTail;
    }

    public async Task InitializeAsync()
    {
        _serializer = new OpaqueEventSerializer(_system);

        _consumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
            opts.GroupId = $"akka-query-tag-{_tag}-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        var topic = _settings.ResolveTopicName(_settings.JournalTopic);
        await _consumer.SubscribeAsync(CancellationToken.None, topic);
    }

    public async Task<Option<EventEnvelope>> ReadNextAsync()
    {
        while (true)
        {
            var timeout = _liveTail ? _settings.RefreshInterval : TimeSpan.FromSeconds(2);
            var result = await _consumer!.ConsumeAsync(timeout, CancellationToken.None);

            if (result is null)
                return _liveTail ? default : Option<EventEnvelope>.None;

            if (result.Headers is null)
                continue;

            // Header-based tag filtering
            if (!result.Headers.TryGetValue(EventEnvelopeCodec.TagsHeader, out var tagsBytes))
                continue;

            var tags = EventEnvelopeCodec.DecodeTags(tagsBytes);
            if (!tags.Contains(_tag))
                continue;

            if (!result.Headers.TryGetValue(EventEnvelopeCodec.SequenceNrHeader, out var seqNrBytes))
                continue;

            var seqNr = EventEnvelopeCodec.DecodeLong(seqNrBytes);
            var persistenceId = result.Key ?? "";

            var persistent = _serializer!.Deserialize(
                persistenceId, seqNr, result.Value, result.Headers);

            var offset = Offset.Sequence(result.Offset);

            return Option<EventEnvelope>.Create(
                new EventEnvelope(offset, persistenceId, seqNr, persistent.Payload));
        }
    }

    public async Task<Done> CloseAsync()
    {
        if (_consumer is IAsyncDisposable disposable)
            await disposable.DisposeAsync();
        return Done.Instance;
    }
}
