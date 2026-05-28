namespace Kuestenlogik.Akka.Surgewave.Streams.Messages;

using Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Result of a produce operation with passthrough data.
/// </summary>
public interface IResults<out TKey, out TValue, out TPassThrough>
{
    TPassThrough PassThrough { get; }
}

/// <summary>
/// Single produce result.
/// </summary>
public sealed record ProducerResult<TKey, TValue, TPassThrough>(
    ProduceResult Result,
    TPassThrough PassThrough) : IResults<TKey, TValue, TPassThrough>;

/// <summary>
/// Multi-record produce result.
/// </summary>
public sealed record MultiResult<TKey, TValue, TPassThrough>(
    IReadOnlyList<ProduceResult> Results,
    TPassThrough PassThrough) : IResults<TKey, TValue, TPassThrough>;

/// <summary>
/// Passthrough-only result (no actual produce happened).
/// </summary>
public sealed record PassThroughResult<TKey, TValue, TPassThrough>(
    TPassThrough PassThrough) : IResults<TKey, TValue, TPassThrough>;
