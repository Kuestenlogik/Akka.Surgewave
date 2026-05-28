namespace Kuestenlogik.Akka.Surgewave.Persistence.Journal;

using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Topics;
using Microsoft.Extensions.Logging;

/// <summary>
/// Ensures that required Surgewave topics exist with correct configuration.
/// Creates topics automatically on first startup if they don't exist.
/// </summary>
public sealed class TopicManager : IAsyncDisposable
{
    private readonly SurgewaveNativeClient _client;
    private readonly ILogger? _logger;
    private bool _initialized;

    public TopicManager(string bootstrapServers, ILogger? logger = null)
    {
        // Parse host:port from bootstrap servers (use first broker)
        var parts = bootstrapServers.Split(',')[0].Split(':');
        var host = parts[0];
        var port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : 9092;

        _client = new SurgewaveNativeClient(host, port);
        _logger = logger;
    }

    /// <summary>
    /// Ensures all required topics exist. Safe to call multiple times.
    /// </summary>
    public async Task EnsureTopicsAsync(SurgewaveJournalSettings settings, CancellationToken cancellationToken = default)
    {
        if (_initialized) return;

        await _client.ConnectAsync(cancellationToken);

        var existingTopics = await _client.Topics.ListAsync(cancellationToken);
        var existingNames = new HashSet<string>(existingTopics.Select(t => t.Name));

        var journalTopic = settings.ResolveTopicName(settings.JournalTopic);
        var indexTopic = settings.ResolveTopicName(settings.IndexTopic);

        // Journal topic: append-only, configurable retention
        if (!existingNames.Contains(journalTopic))
        {
            _logger?.LogInformation("Creating journal topic '{Topic}' ({Partitions} partitions)...",
                journalTopic, settings.JournalTopicPartitions);

            await _client.Topics.Create(journalTopic)
                .WithPartitions(settings.JournalTopicPartitions)
                .WithReplicationFactor((short)settings.JournalTopicReplicationFactor)
                .WithConfig("cleanup.policy", "delete")
                .WithConfig("retention.ms", "-1") // Unbegrenzt
                .ExecuteAsync(cancellationToken);
        }

        // Index topic: compacted, single partition
        if (!existingNames.Contains(indexTopic))
        {
            _logger?.LogInformation("Creating index topic '{Topic}'...", indexTopic);

            await _client.Topics.Create(indexTopic)
                .WithPartitions(1)
                .WithReplicationFactor((short)Math.Min(settings.JournalTopicReplicationFactor, 3))
                .WithCompaction()
                .ExecuteAsync(cancellationToken);
        }

        _initialized = true;
        _logger?.LogInformation("Topic setup complete.");
    }

    /// <summary>
    /// Ensures the snapshot topic exists with compaction enabled.
    /// </summary>
    public async Task EnsureSnapshotTopicAsync(
        Snapshot.SurgewaveSnapshotSettings settings,
        CancellationToken cancellationToken = default)
    {
        await _client.ConnectAsync(cancellationToken);

        var existingTopics = await _client.Topics.ListAsync(cancellationToken);
        var existingNames = new HashSet<string>(existingTopics.Select(t => t.Name));

        var snapshotTopic = settings.ResolveTopicName(settings.SnapshotTopic);

        if (!existingNames.Contains(snapshotTopic))
        {
            _logger?.LogInformation("Creating snapshot topic '{Topic}' ({Partitions} partitions, compacted)...",
                snapshotTopic, settings.SnapshotTopicPartitions);

            await _client.Topics.Create(snapshotTopic)
                .WithPartitions(settings.SnapshotTopicPartitions)
                .WithReplicationFactor((short)settings.SnapshotTopicReplicationFactor)
                .WithCompaction()
                .ExecuteAsync(cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
    }
}
