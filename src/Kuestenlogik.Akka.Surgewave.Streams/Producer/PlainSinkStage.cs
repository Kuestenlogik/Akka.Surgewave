namespace Kuestenlogik.Akka.Surgewave.Streams.Producer;

using global::Akka.Streams.Stage;
using Kuestenlogik.Akka.Surgewave.Streams.Settings;
using Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// GraphStage that consumes ProducerRecord and publishes to Surgewave.
/// Simplest producer sink — fire and forget.
/// </summary>
internal sealed class PlainSinkStage<TKey, TValue>
    : GraphStageWithMaterializedValue<SinkShape<ProducerRecord<TKey, TValue>>, Task<Done>>
{
    private readonly ProducerSettings<TKey, TValue> _settings;

    public PlainSinkStage(ProducerSettings<TKey, TValue> settings)
    {
        _settings = settings;
        Shape = new SinkShape<ProducerRecord<TKey, TValue>>(In);
    }

    public Inlet<ProducerRecord<TKey, TValue>> In { get; } = new("SurgewavePlainSink.In");

    public override SinkShape<ProducerRecord<TKey, TValue>> Shape { get; }

    public override ILogicAndMaterializedValue<Task<Done>> CreateLogicAndMaterializedValue(
        Attributes inheritedAttributes)
    {
        var completion = new TaskCompletionSource<Done>();
        var logic = new PlainSinkLogic(this, completion);
        return new LogicAndMaterializedValue<Task<Done>>(logic, completion.Task);
    }

    private sealed class PlainSinkLogic : GraphStageLogic
    {
        private readonly PlainSinkStage<TKey, TValue> _stage;
        private readonly TaskCompletionSource<Done> _completion;
        private IProducer<TKey, TValue>? _producer;

        public PlainSinkLogic(PlainSinkStage<TKey, TValue> stage, TaskCompletionSource<Done> completion)
            : base(stage.Shape)
        {
            _stage = stage;
            _completion = completion;

            SetHandler(stage.In, onPush: OnPush, onUpstreamFinish: OnFinish, onUpstreamFailure: OnFailure);
        }

        public override void PreStart()
        {
            _producer = _stage._settings.CreateProducer();
            Pull(_stage.In);
        }

        private async void OnPush()
        {
            try
            {
                var record = Grab(_stage.In);
                await _producer!.ProduceAsync(record.Topic, record.Key!, record.Value);
                Pull(_stage.In);
            }
            catch (Exception ex)
            {
                _completion.TrySetException(ex);
                FailStage(ex);
            }
        }

        private void OnFinish()
        {
            _producer?.FlushAsync(CancellationToken.None).GetAwaiter().GetResult();
            _completion.TrySetResult(Done.Instance);
        }

        private void OnFailure(Exception ex)
        {
            _completion.TrySetException(ex);
        }

        public override void PostStop()
        {
            (_producer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
