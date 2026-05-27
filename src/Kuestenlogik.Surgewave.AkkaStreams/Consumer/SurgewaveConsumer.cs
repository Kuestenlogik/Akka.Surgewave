namespace Kuestenlogik.Surgewave.AkkaStreams.Consumer;

using Akka.Streams.Dsl;
using Kuestenlogik.Surgewave.AkkaStreams.Control;
using Kuestenlogik.Surgewave.AkkaStreams.Messages;
using Kuestenlogik.Surgewave.AkkaStreams.Settings;
using Kuestenlogik.Surgewave.AkkaStreams.Subscriptions;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Static factory for Surgewave consumer sources.
/// Entry point for all consumer-side Kuestenlogik.Surgewave.AkkaStreams operations.
/// </summary>
public static class SurgewaveConsumer
{
    /// <summary>
    /// Simplest consumer source: emits ConsumeResult with backpressure.
    /// No offset commit support — offsets must be tracked externally.
    /// </summary>
    public static Source<ConsumeResult<TKey, TValue>, IControl> PlainSource<TKey, TValue>(
        ConsumerSettings<TKey, TValue> settings,
        ISubscription subscription)
    {
        return Source.FromGraph(new PlainSourceStage<TKey, TValue>(settings, subscription));
    }

    /// <summary>
    /// Consumer source where each message carries a CommittableOffset.
    /// Recommended pattern: pair with Committer.Sink for batched commits.
    /// </summary>
    public static Source<CommittableMessage<TKey, TValue>, IControl> CommittableSource<TKey, TValue>(
        ConsumerSettings<TKey, TValue> settings,
        ISubscription subscription)
    {
        return Source.FromGraph(new CommittableSourceStage<TKey, TValue>(settings, subscription));
    }

    /// <summary>
    /// Emits a sub-Source per assigned partition for partition-local processing.
    /// </summary>
    public static Source<(TopicPartitionOffset, Source<ConsumeResult<TKey, TValue>, NotUsed>), IControl>
        PlainPartitionedSource<TKey, TValue>(
            ConsumerSettings<TKey, TValue> settings,
            ISubscription subscription)
    {
        return Source.FromGraph(new PlainPartitionedSourceStage<TKey, TValue>(settings, subscription));
    }

    /// <summary>
    /// Source for external offset stores — offsets loaded from an external
    /// system on partition assignment.
    /// </summary>
    public static Source<ConsumeResult<TKey, TValue>, IControl> PlainPartitionedManualOffsetSource<TKey, TValue>(
        ConsumerSettings<TKey, TValue> settings,
        ISubscription subscription,
        Func<IReadOnlyList<(string Topic, int Partition)>, Task<IReadOnlyList<TopicPartitionOffset>>> getOffsetsOnAssign,
        Func<IReadOnlyList<(string Topic, int Partition)>, Task>? onRevoke = null)
    {
        return Source.FromGraph(new ExternalOffsetSourceStage<TKey, TValue>(
            settings, subscription, getOffsetsOnAssign, onRevoke));
    }

    /// <summary>
    /// Fire-and-forget source: commits offsets before processing.
    /// At-most-once semantics — message loss possible on crash.
    /// </summary>
    public static Source<ConsumeResult<TKey, TValue>, IControl> AtMostOnceSource<TKey, TValue>(
        ConsumerSettings<TKey, TValue> settings,
        ISubscription subscription)
    {
        // At-most-once: enable auto-commit, which commits before processing
        return Source.FromGraph(new PlainSourceStage<TKey, TValue>(settings, subscription));
    }

    /// <summary>
    /// Transactional consumer source for exactly-once consume-transform-produce.
    /// </summary>
    public static Source<CommittableMessage<TKey, TValue>, IControl> TransactionalSource<TKey, TValue>(
        ConsumerSettings<TKey, TValue> settings,
        ISubscription subscription)
    {
        return Source.FromGraph(new CommittableSourceStage<TKey, TValue>(settings, subscription));
    }
}
