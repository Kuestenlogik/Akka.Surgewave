namespace Kuestenlogik.Akka.Surgewave.Persistence.Snapshot;

using global::Akka.Actor;
using global::Akka.Configuration;
using global::Akka.Persistence.Snapshot;
using Kuestenlogik.Akka.Surgewave.Persistence.Journal;
using Kuestenlogik.Akka.Surgewave.Persistence.Serialization;
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
                global::Akka.Configuration.ConfigurationFactory.ParseString(hocon))
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

        // Fresh consumer per LoadAsync — the snapshot topic is a compacted
        // event log we have to replay from the beginning every time. A
        // long-lived shared consumer would carry forward an offset, so
        // subsequent calls would see only deltas. Cheap to spin up because
        // the snapshot topic is small (one record per save + tombstone).
        await using var loadConsumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _settings.BootstrapServers;
            opts.GroupId = $"akka-snapshot-reader-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });
        await loadConsumer.SubscribeAsync(cancellationToken, snapshotTopic);

        // Collect all live snapshots for this persistenceId in a dictionary
        // keyed by seqNr; tombstone records (specific seqNr OR criteria
        // range) remove matching entries. After the scan, return the
        // highest-seqNr live snapshot that also satisfies the load criteria.
        // Keyed-by-seqNr is the natural Akka.Persistence identity for a
        // snapshot — Save+Delete pair up by seqNr.
        var live = new Dictionary<long, (DateTime Timestamp, object Snapshot)>();

        while (!cancellationToken.IsCancellationRequested)
        {
            // Short per-call timeout so the end-of-topic detection (null) is
            // cheap: long-polling 5s per partition would exceed Akka's 10s
            // ExpectMsg budget on its own. 2s leaves enough slack for the
            // first record after a fresh save while still falling well below
            // the TCK deadline.
            var record = await loadConsumer.ConsumeAsync(TimeSpan.FromSeconds(2), cancellationToken);
            if (record is null)
                break;

            if (record.Key != persistenceId)
                continue;
            if (record.Headers is null)
                continue;

            // Tombstone: specific seqNr — DeleteAsync(metadata) path. When the
            // tombstone carries a non-zero timestamp it must match the stored
            // snapshot exactly; otherwise it is a no-op (the TCK uses this to
            // express "don't delete if my metadata is stale").
            if (record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotTombstoneSeqNrHeader, out var delSeqBytes))
            {
                var delSeq = EventEnvelopeCodec.DecodeLong(delSeqBytes);
                var delTsTicks = record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotTombstoneTimestampHeader, out var delTsBytes)
                    ? EventEnvelopeCodec.DecodeLong(delTsBytes)
                    : 0L;
                if (live.TryGetValue(delSeq, out var liveEntry))
                {
                    if (delTsTicks == 0L || liveEntry.Timestamp.Ticks == delTsTicks)
                        live.Remove(delSeq);
                }
                continue;
            }

            // Tombstone: criteria range — DeleteAsync(criteria) path.
            if (record.Headers.ContainsKey(EventEnvelopeCodec.SnapshotTombstoneMaxSeqNrHeader)
                || record.Headers.ContainsKey(EventEnvelopeCodec.SnapshotTombstoneMinSeqNrHeader)
                || record.Headers.ContainsKey(EventEnvelopeCodec.SnapshotTombstoneMaxTimestampHeader)
                || record.Headers.ContainsKey(EventEnvelopeCodec.SnapshotTombstoneMinTimestampHeader))
            {
                var tMaxSeq = record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotTombstoneMaxSeqNrHeader, out var msb)
                    ? EventEnvelopeCodec.DecodeLong(msb) : long.MaxValue;
                var tMinSeq = record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotTombstoneMinSeqNrHeader, out var mnb)
                    ? EventEnvelopeCodec.DecodeLong(mnb) : 0L;
                var tMaxTs = record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotTombstoneMaxTimestampHeader, out var mtb)
                    ? new DateTime(EventEnvelopeCodec.DecodeLong(mtb), DateTimeKind.Utc) : DateTime.MaxValue;
                var tMinTs = record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotTombstoneMinTimestampHeader, out var ntb)
                    ? new DateTime(EventEnvelopeCodec.DecodeLong(ntb), DateTimeKind.Utc) : DateTime.MinValue;
                var doomed = live.Where(kv =>
                        kv.Key >= tMinSeq && kv.Key <= tMaxSeq
                        && kv.Value.Timestamp >= tMinTs && kv.Value.Timestamp <= tMaxTs)
                    .Select(kv => kv.Key)
                    .ToList();
                foreach (var k in doomed) live.Remove(k);
                continue;
            }

            // Regular snapshot record — needs a non-empty body and the seqNr header.
            if (record.Value is null or { Length: 0 })
                continue;
            if (!record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotSeqNrHeader, out var seqNrBytes))
                continue;

            var seqNr = EventEnvelopeCodec.DecodeLong(seqNrBytes);
            var timestamp = record.Headers.TryGetValue(EventEnvelopeCodec.SnapshotTimestampHeader, out var tsBytes)
                ? new DateTime(EventEnvelopeCodec.DecodeLong(tsBytes), DateTimeKind.Utc)
                : DateTime.UtcNow;

            var snapshot = _serializer!.DeserializeSnapshot(record.Value, record.Headers);
            live[seqNr] = (timestamp, snapshot);
        }

        // Apply the load criteria to the live set and return the highest seqNr.
        var matching = live
            .Where(kv =>
                kv.Key <= criteria.MaxSequenceNr && kv.Key >= criteria.MinSequenceNr
                && kv.Value.Timestamp <= criteria.MaxTimeStamp && kv.Value.Timestamp >= (criteria.MinTimestamp ?? DateTime.MinValue))
            .OrderByDescending(kv => kv.Key)
            .ToList();
        if (matching.Count == 0) return null;
        var best = matching[0];
        return new SelectedSnapshot(
            new SnapshotMetadata(persistenceId, best.Key, best.Value.Timestamp), best.Value.Snapshot);
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
        // Tombstone for one specific seqNr — empty body + marker header.
        // The producer rejects null values; a Kafka-style null compaction
        // tombstone would also drop every prior seqNr for the same key,
        // which is wrong for Akka's per-snapshot Delete semantics.
        //
        // Akka allows the caller to scope the delete by both seqNr and
        // timestamp. We always serialise the timestamp ticks alongside the
        // seqNr; LoadAsync only honours the tombstone when the timestamp
        // either matches the stored snapshot exactly or is zero
        // (DateTime.MinValue = "any timestamp").
        var headers = new Dictionary<string, byte[]>
        {
            [EventEnvelopeCodec.SnapshotTombstoneSeqNrHeader] = EventEnvelopeCodec.EncodeLong(metadata.SequenceNr),
            [EventEnvelopeCodec.SnapshotTombstoneTimestampHeader] = EventEnvelopeCodec.EncodeLong(metadata.Timestamp.Ticks),
        };
        await _producer!.ProduceAsync(
            snapshotTopic, metadata.PersistenceId, Array.Empty<byte>(), headers);
    }

    protected override async Task DeleteAsync(
        string persistenceId, SnapshotSelectionCriteria criteria,
        CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync();
        var snapshotTopic = _settings.ResolveTopicName(_settings.SnapshotTopic);
        // Range-tombstone: carry the four criteria bounds as headers so
        // LoadAsync can drop every live snapshot that matches.
        var headers = new Dictionary<string, byte[]>
        {
            [EventEnvelopeCodec.SnapshotTombstoneMaxSeqNrHeader] = EventEnvelopeCodec.EncodeLong(criteria.MaxSequenceNr),
            [EventEnvelopeCodec.SnapshotTombstoneMinSeqNrHeader] = EventEnvelopeCodec.EncodeLong(criteria.MinSequenceNr),
            [EventEnvelopeCodec.SnapshotTombstoneMaxTimestampHeader] = EventEnvelopeCodec.EncodeLong(criteria.MaxTimeStamp.Ticks),
            [EventEnvelopeCodec.SnapshotTombstoneMinTimestampHeader] = EventEnvelopeCodec.EncodeLong((criteria.MinTimestamp ?? DateTime.MinValue).Ticks),
        };
        await _producer!.ProduceAsync(
            snapshotTopic, persistenceId, Array.Empty<byte>(), headers);
    }

    protected override void PostStop()
    {
        (_producer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        (_consumer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _nativeClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        base.PostStop();
    }
}
