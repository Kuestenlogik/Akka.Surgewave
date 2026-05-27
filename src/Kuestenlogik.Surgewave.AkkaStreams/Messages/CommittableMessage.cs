namespace Kuestenlogik.Surgewave.AkkaStreams.Messages;

using Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// A consumed message paired with a committable offset.
/// Used by CommittableSource for at-least-once semantics.
/// </summary>
public sealed record CommittableMessage<TKey, TValue>
{
    public required ConsumeResult<TKey, TValue> Record { get; init; }
    public required CommittableOffset CommittableOffset { get; init; }
}
