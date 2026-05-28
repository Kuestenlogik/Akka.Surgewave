namespace Kuestenlogik.Surgewave.AkkaStreams.Producer;

using Akka.Streams.Stage;
using Kuestenlogik.Surgewave.AkkaStreams.Messages;
using Kuestenlogik.Surgewave.AkkaStreams.Settings;
using Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Flow that publishes records to Surgewave and passes through the result
/// with the original passthrough data. Supports Single, Multi, and
/// PassThroughOnly message types.
/// </summary>
internal sealed class FlexiFlowStage<TKey, TValue, TPassThrough>
    : GraphStage<FlowShape<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>>>
{
    private readonly ProducerSettings<TKey, TValue> _settings;

    public FlexiFlowStage(ProducerSettings<TKey, TValue> settings)
    {
        _settings = settings;
        Shape = new FlowShape<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>>(In, Out);
    }

    public Inlet<IEnvelope<TKey, TValue, TPassThrough>> In { get; } = new("SurgewaveFlexiFlow.In");
    public Outlet<IResults<TKey, TValue, TPassThrough>> Out { get; } = new("SurgewaveFlexiFlow.Out");

    public override FlowShape<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>> Shape { get; }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new FlexiFlowLogic(this);
    }

    private sealed class FlexiFlowLogic : GraphStageLogic
    {
        private readonly FlexiFlowStage<TKey, TValue, TPassThrough> _stage;
        private IProducer<TKey, TValue>? _producer;

        public FlexiFlowLogic(FlexiFlowStage<TKey, TValue, TPassThrough> stage)
            : base(stage.Shape)
        {
            _stage = stage;

            SetHandler(stage.In, onPush: OnPush);
            SetHandler(stage.Out, onPull: () => Pull(stage.In));
        }

        public override void PreStart()
        {
            _producer = _stage._settings.CreateProducer();
        }

        private async void OnPush()
        {
            try
            {
                var envelope = Grab(_stage.In);

                IResults<TKey, TValue, TPassThrough> result = envelope switch
                {
                    SingleMessage<TKey, TValue, TPassThrough> single =>
                        new ProducerResult<TKey, TValue, TPassThrough>(
                            await _producer!.ProduceAsync(
                                single.Record.Topic, single.Record.Key!, single.Record.Value),
                            single.PassThrough),

                    MultiMessage<TKey, TValue, TPassThrough> multi =>
                        new MultiResult<TKey, TValue, TPassThrough>(
                            await ProduceMultiAsync(multi.Records),
                            multi.PassThrough),

                    PassThroughMessage<TKey, TValue, TPassThrough> pt =>
                        new PassThroughResult<TKey, TValue, TPassThrough>(pt.PassThrough),

                    _ => throw new InvalidOperationException($"Unknown envelope type: {envelope.GetType()}")
                };

                Push(_stage.Out, result);
            }
            catch (Exception ex)
            {
                FailStage(ex);
            }
        }

        private async Task<IReadOnlyList<ProduceResult>> ProduceMultiAsync(
            IReadOnlyList<ProducerRecord<TKey, TValue>> records)
        {
            var results = new List<ProduceResult>(records.Count);
            foreach (var record in records)
            {
                var result = await _producer!.ProduceAsync(record.Topic, record.Key!, record.Value);
                results.Add(result);
            }
            return results;
        }

        public override void PostStop()
        {
            _producer?.FlushAsync(CancellationToken.None).GetAwaiter().GetResult();
            (_producer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
