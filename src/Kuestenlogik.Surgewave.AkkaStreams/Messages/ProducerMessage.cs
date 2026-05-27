namespace Kuestenlogik.Surgewave.AkkaStreams.Messages;

using Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Factory for creating producer messages with passthrough support.
/// </summary>
public static class ProducerMessage
{
    /// <summary>
    /// Creates a single-record producer message.
    /// </summary>
    public static SingleMessage<TKey, TValue, TPassThrough> Single<TKey, TValue, TPassThrough>(
        ProducerRecord<TKey, TValue> record,
        TPassThrough passThrough)
    {
        return new SingleMessage<TKey, TValue, TPassThrough>(record, passThrough);
    }

    /// <summary>
    /// Creates a multi-record producer message.
    /// </summary>
    public static MultiMessage<TKey, TValue, TPassThrough> Multi<TKey, TValue, TPassThrough>(
        IReadOnlyList<ProducerRecord<TKey, TValue>> records,
        TPassThrough passThrough)
    {
        return new MultiMessage<TKey, TValue, TPassThrough>(records, passThrough);
    }

    /// <summary>
    /// Creates a passthrough-only message (no record to produce).
    /// </summary>
    public static PassThroughMessage<TKey, TValue, TPassThrough> PassThroughOnly<TKey, TValue, TPassThrough>(
        TPassThrough passThrough)
    {
        return new PassThroughMessage<TKey, TValue, TPassThrough>(passThrough);
    }
}

/// <summary>
/// A single record to be produced with associated passthrough data.
/// </summary>
public sealed record SingleMessage<TKey, TValue, TPassThrough>(
    ProducerRecord<TKey, TValue> Record,
    TPassThrough PassThrough) : IEnvelope<TKey, TValue, TPassThrough>;

/// <summary>
/// Multiple records to be produced with associated passthrough data.
/// </summary>
public sealed record MultiMessage<TKey, TValue, TPassThrough>(
    IReadOnlyList<ProducerRecord<TKey, TValue>> Records,
    TPassThrough PassThrough) : IEnvelope<TKey, TValue, TPassThrough>;

/// <summary>
/// No record to produce, only passthrough data.
/// </summary>
public sealed record PassThroughMessage<TKey, TValue, TPassThrough>(
    TPassThrough PassThrough) : IEnvelope<TKey, TValue, TPassThrough>;
