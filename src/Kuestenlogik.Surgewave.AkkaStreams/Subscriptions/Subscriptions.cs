namespace Kuestenlogik.Surgewave.AkkaStreams.Subscriptions;

/// <summary>
/// Factory methods for creating subscriptions.
/// </summary>
public static class Subscriptions
{
    /// <summary>
    /// Subscribe to one or more topics by name.
    /// </summary>
    public static ISubscription Topics(params string[] topics) =>
        new TopicSubscription(topics);

    /// <summary>
    /// Subscribe to topics matching a regex pattern.
    /// </summary>
    public static ISubscription TopicPattern(string pattern) =>
        new TopicPatternSubscription(pattern);

    /// <summary>
    /// Manually assign specific topic-partition pairs.
    /// </summary>
    public static ISubscription Assignment(params (string Topic, int Partition)[] assignments) =>
        new AssignmentSubscription(assignments);

    /// <summary>
    /// Manually assign specific topic-partition-offset triples.
    /// </summary>
    public static ISubscription AssignmentWithOffset(params (string Topic, int Partition, long Offset)[] assignments) =>
        new AssignmentWithOffsetSubscription(assignments);
}
