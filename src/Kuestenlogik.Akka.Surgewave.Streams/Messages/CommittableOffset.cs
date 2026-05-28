namespace Kuestenlogik.Akka.Surgewave.Streams.Messages;

using Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Represents the offset of a consumed message, waiting to be committed.
/// </summary>
public sealed class CommittableOffset
{
    private readonly Func<TopicPartitionOffset, CancellationToken, Task> _commitFunc;

    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public string GroupId { get; }

    internal CommittableOffset(
        Func<TopicPartitionOffset, CancellationToken, Task> commitFunc,
        string topic,
        int partition,
        long offset,
        string groupId)
    {
        _commitFunc = commitFunc;
        Topic = topic;
        Partition = partition;
        Offset = offset;
        GroupId = groupId;
    }

    /// <summary>
    /// Commits this offset to Surgewave.
    /// </summary>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        await _commitFunc(
            new TopicPartitionOffset(Topic, Partition, Offset + 1),
            cancellationToken);
    }

    public TopicPartitionOffset ToTopicPartitionOffset() =>
        new(Topic, Partition, Offset + 1);
}
