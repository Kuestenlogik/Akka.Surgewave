namespace Kuestenlogik.Akka.Surgewave.Streams.Producer;

using global::Akka.Streams.Dsl;
using Kuestenlogik.Akka.Surgewave.Streams.Messages;
using Kuestenlogik.Akka.Surgewave.Streams.Settings;
using Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Static factory for Surgewave producer sinks and flows.
/// Entry point for all producer-side Kuestenlogik.Akka.Surgewave.Streams operations.
/// </summary>
public static class SurgewaveProducer
{
    /// <summary>
    /// Simplest producer sink: consumes ProducerRecord and publishes to Surgewave.
    /// </summary>
    public static Sink<ProducerRecord<TKey, TValue>, Task<Done>> PlainSink<TKey, TValue>(
        ProducerSettings<TKey, TValue> settings)
    {
        return Sink.FromGraph(new PlainSinkStage<TKey, TValue>(settings));
    }

    /// <summary>
    /// Producer flow with feedback and passthrough support.
    /// Publishes records and returns ProducerResult with the original
    /// passthrough data (e.g., CommittableOffset for consume-produce pipelines).
    /// </summary>
    public static Flow<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>, NotUsed>
        FlexiFlow<TKey, TValue, TPassThrough>(ProducerSettings<TKey, TValue> settings)
    {
        return Flow.FromGraph(new FlexiFlowStage<TKey, TValue, TPassThrough>(settings));
    }

    /// <summary>
    /// Transactional flow for exactly-once consume-transform-produce.
    /// </summary>
    public static Flow<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>, NotUsed>
        TransactionalFlow<TKey, TValue, TPassThrough>(
            ProducerSettings<TKey, TValue> settings,
            string transactionalId)
    {
        return Flow.FromGraph(new TransactionalFlowStage<TKey, TValue, TPassThrough>(settings, transactionalId));
    }
}
