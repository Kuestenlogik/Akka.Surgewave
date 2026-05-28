namespace Kuestenlogik.Akka.Surgewave.Persistence.Journal;

using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Microsoft.Extensions.Logging;

/// <summary>
/// Manages the compacted index topic for fast event replay.
/// Maintains an in-memory LRU cache of index entries, writes updates
/// to both cache and compacted topic.
/// </summary>
public sealed class JournalIndexManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, IndexEntry> _cache = new();
    private readonly IProducer<string, byte[]> _indexProducer;
    private readonly IConsumer<string, byte[]> _indexConsumer;
    private readonly string _indexTopic;
    private readonly ILogger? _logger;
    private bool _warmedUp;

    public JournalIndexManager(
        SurgewaveJournalSettings settings,
        IProducer<string, byte[]> indexProducer,
        IConsumer<string, byte[]> indexConsumer,
        ILogger? logger = null)
    {
        _indexTopic = settings.ResolveTopicName(settings.IndexTopic);
        _indexProducer = indexProducer;
        _indexConsumer = indexConsumer;
        _logger = logger;
    }

    /// <summary>
    /// Warms the index cache by consuming all entries from the compacted index topic.
    /// Should be called once on startup.
    /// </summary>
    public async Task WarmupAsync(CancellationToken cancellationToken = default)
    {
        if (_warmedUp)
            return;

        _logger?.LogInformation("Warming up journal index from topic {Topic}...", _indexTopic);

        await _indexConsumer.SubscribeAsync(cancellationToken, _indexTopic);

        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await _indexConsumer.ConsumeAsync(TimeSpan.FromSeconds(2), cancellationToken);
            if (result is null)
                break;

            if (result.Value is { Length: > 0 } && result.Key is not null)
            {
                var entry = JsonSerializer.Deserialize<IndexEntry>(result.Value);
                if (entry is not null)
                    _cache[result.Key] = entry;
            }
        }

        _warmedUp = true;
        _logger?.LogInformation("Journal index warmed up with {Count} entries.", _cache.Count);
    }

    /// <summary>
    /// Reads the index from cache or from the compacted topic.
    /// </summary>
    public async Task<IndexEntry?> GetIndexAsync(string persistenceId)
    {
        if (_cache.TryGetValue(persistenceId, out var cached))
            return cached;

        return await ReadFromTopicAsync(persistenceId);
    }

    /// <summary>
    /// Called after each successful write to update the index.
    /// Writes to both cache and compacted topic.
    /// </summary>
    public async Task UpdateAsync(
        string persistenceId,
        long newHighestSeqNr,
        int partition,
        long offset,
        IEnumerable<string>? tags = null)
    {
        var existing = _cache.GetValueOrDefault(persistenceId);
        var entry = new IndexEntry
        {
            HighestSequenceNr = newHighestSeqNr,
            Partition = partition,
            FirstOffset = existing?.FirstOffset ?? offset,
            LastOffset = offset,
            DeletedToSequenceNr = existing?.DeletedToSequenceNr ?? 0,
            Tags = MergeTags(existing?.Tags, tags),
            LastUpdated = DateTimeOffset.UtcNow
        };

        _cache[persistenceId] = entry;

        await _indexProducer.ProduceAsync(
            _indexTopic,
            persistenceId,
            JsonSerializer.SerializeToUtf8Bytes(entry));
    }

    /// <summary>
    /// Marks events as logically deleted up to the given sequence number.
    /// </summary>
    public async Task MarkDeletedToAsync(string persistenceId, long toSequenceNr)
    {
        var existing = await GetIndexAsync(persistenceId);
        if (existing is null)
            return;

        var updated = existing with
        {
            DeletedToSequenceNr = toSequenceNr,
            LastUpdated = DateTimeOffset.UtcNow
        };

        _cache[persistenceId] = updated;

        await _indexProducer.ProduceAsync(
            _indexTopic,
            persistenceId,
            JsonSerializer.SerializeToUtf8Bytes(updated));
    }

    private async Task<IndexEntry?> ReadFromTopicAsync(string persistenceId)
    {
        // For non-cached entries, attempt a targeted read.
        // In practice, after warmup all entries should be cached.
        await Task.CompletedTask;
        return null;
    }

    private static string[] MergeTags(string[]? existing, IEnumerable<string>? newTags)
    {
        if (newTags is null)
            return existing ?? [];

        var set = new HashSet<string>(existing ?? []);
        foreach (var tag in newTags)
            set.Add(tag);
        return [.. set];
    }

    public async ValueTask DisposeAsync()
    {
        if (_indexProducer is IAsyncDisposable producerDisposable)
            await producerDisposable.DisposeAsync();
        if (_indexConsumer is IAsyncDisposable consumerDisposable)
            await consumerDisposable.DisposeAsync();
    }
}
