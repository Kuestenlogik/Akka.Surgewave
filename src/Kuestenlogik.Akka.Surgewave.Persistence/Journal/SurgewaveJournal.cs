namespace Kuestenlogik.Akka.Surgewave.Persistence.Journal;

using System.Collections.Immutable;
using global::Akka.Actor;
using global::Akka.Configuration;
using global::Akka.Persistence.Journal;
using Kuestenlogik.Akka.Surgewave.Persistence.Serialization;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Microsoft.Extensions.Logging;

/// <summary>
/// AsyncWriteJournal implementation backed by Surgewave.
/// Events are written to an append-only topic keyed by PersistenceId.
/// A compacted index topic enables fast replay without full topic scan.
/// </summary>
public sealed class SurgewaveJournal : AsyncWriteJournal
{
    private readonly SurgewaveJournalSettings _settings;
    private readonly ActorSystem _system;
    private ISurgewaveEventSerializer? _serializer;
    private IProducer<string, byte[]>? _producer;
    private IConsumer<string, byte[]>? _replayConsumer;
    private JournalIndexManager? _indexManager;
    private SurgewaveNativeClient? _nativeClient;
#pragma warning disable CS0649
    private readonly ILogger? _logger;
#pragma warning restore CS0649

    public SurgewaveJournal(Config journalConfig)
    {
        _settings = new SurgewaveJournalSettings(journalConfig);
        _system = Context.System; // capture while ActorContext is active
    }

    private bool _initialized;

    private async Task EnsureInitializedAsync()
    {
        if (_initialized) return;

        // Create NativeClient for admin operations and schema registry
        var parts = _settings.BootstrapServers.Split(',')[0].Split(':');
        var host = parts[0];
        var port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : 9092;
        _nativeClient = new SurgewaveNativeClient(host, port);
        await _nativeClient.ConnectAsync();

        // Wire up serializer (using _system captured in constructor)
        if (_settings.SerializationMode is SerializationMode.Json or SerializationMode.Proto)
        {
            _settings.SchemaRegistryOperations = _nativeClient.Schema;
            _serializer = new SchemaRegistryEventSerializer(_settings);
        }
        else
        {
            _serializer = new OpaqueEventSerializer(_system);
        }

        // Ensure topics exist
        await using var topicManager = new TopicManager(_settings.BootstrapServers, _logger);
        await topicManager.EnsureTopicsAsync(_settings);

        _producer = new SurgewaveProducer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
            opts.BatchSize = _settings.ProduceBatchSize;
            opts.LingerMs = _settings.ProduceLingerMs;
        });

        var indexProducer = new SurgewaveProducer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
        });

        var indexConsumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
            opts.GroupId = $"akka-journal-index-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        _indexManager = new JournalIndexManager(_settings, indexProducer, indexConsumer, _logger);
        await _indexManager.WarmupAsync();

        _replayConsumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
            opts.GroupId = $"akka-journal-replay-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        _initialized = true;
    }

    protected override async Task<IImmutableList<Exception?>> WriteMessagesAsync(
        IEnumerable<AtomicWrite> messages,
        CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync();
        var results = new List<Exception?>();

        foreach (var atomicWrite in messages)
        {
            try
            {
                var persistentMessages = (IReadOnlyList<IPersistentRepresentation>)atomicWrite.Payload;
                var journalTopic = _settings.ResolveTopicName(_settings.JournalTopic);
                ProduceResult? lastResult = null;
                IEnumerable<string>? allTags = null;

                // TODO: When EnableEos is true, wrap in Surgewave transaction:
                //   var txn = await nativeClient.Transactions.BeginTransaction(transactionalId).InitAsync();
                //   ... produce within transaction ...
                //   await txn.CommitAsync();

                foreach (var persistent in persistentMessages)
                {
                    var payload = _serializer!.Serialize(persistent, out var headers);

                    headers[EventEnvelopeCodec.SequenceNrHeader] = EventEnvelopeCodec.EncodeLong(persistent.SequenceNr);
                    headers[EventEnvelopeCodec.WriterUuidHeader] = EventEnvelopeCodec.EncodeString(persistent.WriterGuid);
                    headers[EventEnvelopeCodec.TimestampHeader] = EventEnvelopeCodec.EncodeLong(DateTimeOffset.UtcNow.Ticks);

                    if (persistent.Payload is Tagged tagged && tagged.Tags.Count > 0)
                    {
                        headers[EventEnvelopeCodec.TagsHeader] = EventEnvelopeCodec.EncodeTags(tagged.Tags);
                        allTags = tagged.Tags;
                    }

                    lastResult = await _producer!.ProduceAsync(
                        journalTopic,
                        persistent.PersistenceId,
                        payload,
                        headers);
                }

                if (lastResult is not null)
                {
                    var last = persistentMessages[^1];
                    await _indexManager!.UpdateAsync(
                        last.PersistenceId,
                        last.SequenceNr,
                        lastResult.Partition,
                        lastResult.Offset,
                        allTags);
                }

                results.Add(null);
            }
            catch (Exception ex)
            {
                results.Add(ex);
            }
        }

        return results.ToImmutableList();
    }

    public override async Task ReplayMessagesAsync(
        IActorContext context,
        string persistenceId,
        long fromSequenceNr,
        long toSequenceNr,
        long max,
        Action<IPersistentRepresentation> recoveryCallback)
    {
        await EnsureInitializedAsync();
        var index = await _indexManager!.GetIndexAsync(persistenceId);
        if (index is null)
            return;

        if (index.DeletedToSequenceNr >= toSequenceNr)
            return;

        var journalTopic = _settings.ResolveTopicName(_settings.JournalTopic);
        _replayConsumer!.Assign(journalTopic, index.Partition, index.FirstOffset);

        long count = 0;
        var effectiveFrom = Math.Max(fromSequenceNr, index.DeletedToSequenceNr + 1);

        while (count < max)
        {
            var result = await _replayConsumer.ConsumeAsync(
                _settings.ReplayTimeout, CancellationToken.None);

            if (result is null)
                break;
            if (result.Offset > index.LastOffset)
                break;
            if (result.Key != persistenceId)
                continue;
            if (result.Headers is null ||
                !result.Headers.TryGetValue(EventEnvelopeCodec.SequenceNrHeader, out var seqNrBytes))
                continue;

            var seqNr = EventEnvelopeCodec.DecodeLong(seqNrBytes);

            if (seqNr < effectiveFrom) continue;
            if (seqNr > toSequenceNr) break;

            var persistent = _serializer!.Deserialize(
                persistenceId, seqNr, result.Value, result.Headers);
            recoveryCallback(persistent);
            count++;
        }
    }

    public override async Task<long> ReadHighestSequenceNrAsync(
        string persistenceId, long fromSequenceNr,
        CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync();
        var index = await _indexManager!.GetIndexAsync(persistenceId);
        return index?.HighestSequenceNr ?? 0L;
    }

    protected override async Task DeleteMessagesToAsync(
        string persistenceId, long toSequenceNr,
        CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync();
        await _indexManager!.MarkDeletedToAsync(persistenceId, toSequenceNr);
    }

    protected override void PostStop()
    {
        _indexManager?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        (_producer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        (_replayConsumer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _nativeClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        base.PostStop();
    }
}
