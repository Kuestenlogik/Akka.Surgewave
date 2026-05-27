namespace Kuestenlogik.Surgewave.AkkaPersistence.Snapshot;

using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence;
using Akka.Persistence.Snapshot;
using Kuestenlogik.Surgewave.AkkaPersistence.Journal;
using Kuestenlogik.Surgewave.AkkaPersistence.Serialization;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// SnapshotStore implementation using a Surgewave compacted topic.
/// Surgewave's log compaction automatically retains only the latest
/// snapshot per PersistenceId — no manual cleanup needed.
/// </summary>
public sealed class SurgewaveSnapshotStore : SnapshotStore
{
    private readonly SurgewaveSnapshotSettings _settings;
    private readonly ActorSystem _system;
    private ISurgewaveEventSerializer? _serializer;
    private IProducer<string, byte[]>? _producer;
    private IConsumer<string, byte[]>? _consumer;
    private SurgewaveNativeClient? _nativeClient;
    private bool _initialized;

    public SurgewaveSnapshotStore(Config snapshotConfig)
    {
        _settings = new SurgewaveSnapshotSettings(snapshotConfig);
        _system = Context.System;
    }

    private async Task EnsureInitializedAsync()
    {
        if (_initialized) return;

        // Create NativeClient for topic management and schema registry
        var parts = _settings.BootstrapServers.Split(',')[0].Split(':');
        var host = parts[0];
        var port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : 9092;
        _nativeClient = new SurgewaveNativeClient(host, port);
        await _nativeClient.ConnectAsync();

        // Wire up serializer based on mode
        if (_settings.SerializationMode is SerializationMode.Json or SerializationMode.Proto)
        {
            // Create journal settings to share with SchemaRegistryEventSerializer
            var modeStr = _settings.SerializationMode == SerializationMode.Proto ? "proto" : "json";
            var autoReg = _settings.SchemaRegistryAutoRegister.ToString().ToLowerInvariant();
            var hocon =
                $"bootstrap-servers = \"{_settings.BootstrapServers}\"\n" +
                $"journal-topic = \"{_settings.SnapshotTopic}\"\n" +
                $"serialization-mode = \"{modeStr}\"\n" +
                $"schema-registry {{ url = \"{_settings.SchemaRegistryUrl}\", auto-register = {autoReg} }}";
            var journalSettings = new SurgewaveJournalSettings(
                Akka.Configuration.ConfigurationFactory.ParseString(hocon))
            {
                SchemaRegistryOperations = _nativeClient.Schema
            };
            _serializer = new SchemaRegistryEventSerializer(journalSettings);
        }
        else
        {
            _serializer = new OpaqueEventSerializer(_system);
        }

        // Ensure topic exists
        await using var topicManager = new TopicManager(_settings.BootstrapServers);
        await topicManager.EnsureSnapshotTopicAsync(_settings);

        _producer = new SurgewaveProducer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
        });

        _consumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
            opts.GroupId = $"akka-snapshot-reader-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        _initialized = true;
    }

    protected override async Task<SelectedSnapshot?> LoadAsync(
        string persistenceId,
        SnapshotSelectionCriteria criteria,
        CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync();
        var snapshotTopic = _settings.ResolveTopicName(_settings.SnapshotTopic);
        await _consumer!.SubscribeAsync(cancellationToken, snapshotTopic);

        SelectedSnapshot? result = null;

        while (!cancellationToken.IsCancellationRequested)
        {
            var record = await _consumer.ConsumeAsync(TimeSpan.FromSeconds(5), cancellationToken);
            if (record is null)
                break;

            if (record.Key != persistenceId)
                continue;
            if (record.Value is null or { Length: 0 })
                continue;
            if (record.Headers is null)
                continue;
            if (!record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotSeqNrHeader, out var seqNrBytes))
                continue;

            var seqNr = EventEnvelopeCodec.DecodeLong(seqNrBytes);
            var timestamp = record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotTimestampHeader, out var tsBytes)
                ? new DateTime(EventEnvelopeCodec.DecodeLong(tsBytes), DateTimeKind.Utc)
                : DateTime.UtcNow;

            if (seqNr > criteria.MaxSequenceNr || seqNr < criteria.MinSequenceNr)
                continue;
            if (timestamp > criteria.MaxTimeStamp || timestamp < criteria.MinTimestamp)
                continue;

            var snapshot = _serializer!.DeserializeSnapshot(record.Value, record.Headers);
            result = new SelectedSnapshot(
                new SnapshotMetadata(persistenceId, seqNr, timestamp), snapshot);
        }

        return result;
    }

    protected override async Task SaveAsync(
        SnapshotMetadata metadata, object snapshot,
        CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync();
        var snapshotTopic = _settings.ResolveTopicName(_settings.SnapshotTopic);
        var payload = _serializer!.SerializeSnapshot(snapshot, out var headers);

        headers[EventEnvelopeCodec.SnapshotSeqNrHeader] = EventEnvelopeCodec.EncodeLong(metadata.SequenceNr);
        headers[EventEnvelopeCodec.SnapshotTimestampHeader] = EventEnvelopeCodec.EncodeLong(metadata.Timestamp.Ticks);

        await _producer!.ProduceAsync(
            snapshotTopic, metadata.PersistenceId, payload, headers);
    }

    protected override async Task DeleteAsync(
        SnapshotMetadata metadata, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync();
        var snapshotTopic = _settings.ResolveTopicName(_settings.SnapshotTopic);
        await _producer!.ProduceAsync(snapshotTopic, metadata.PersistenceId, null!);
    }

    protected override async Task DeleteAsync(
        string persistenceId, SnapshotSelectionCriteria criteria,
        CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync();
        var snapshotTopic = _settings.ResolveTopicName(_settings.SnapshotTopic);
        await _producer!.ProduceAsync(snapshotTopic, persistenceId, null!);
    }

    protected override void PostStop()
    {
        (_producer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        (_consumer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _nativeClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        base.PostStop();
    }
}
