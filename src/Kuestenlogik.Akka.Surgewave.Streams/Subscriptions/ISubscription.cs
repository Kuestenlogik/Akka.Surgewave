namespace Kuestenlogik.Akka.Surgewave.Streams.Subscriptions;

/// <summary>
/// Describes how to subscribe to Surgewave topics.
/// </summary>
public interface ISubscription;

/// <summary>
/// Subscribe to one or more specific topics by name.
/// </summary>
public sealed record TopicSubscription(IReadOnlyList<string> Topics) : ISubscription;

/// <summary>
/// Subscribe to topics matching a regex pattern.
/// </summary>
public sealed record TopicPatternSubscription(string Pattern) : ISubscription;

/// <summary>
/// Manually assign specific topic-partition pairs.
/// </summary>
public sealed record AssignmentSubscription(
    IReadOnlyList<(string Topic, int Partition)> Assignments) : ISubscription;

/// <summary>
/// Manually assign specific topic-partition-offset triples.
/// </summary>
public sealed record AssignmentWithOffsetSubscription(
    IReadOnlyList<(string Topic, int Partition, long Offset)> Assignments) : ISubscription;
