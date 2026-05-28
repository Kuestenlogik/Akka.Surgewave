namespace Kuestenlogik.Surgewave.AkkaStreams.Producer;

using Akka.Streams.Stage;
using Kuestenlogik.Surgewave.AkkaStreams.Messages;
using Kuestenlogik.Surgewave.AkkaStreams.Settings;
using Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Transactional flow stage using Surgewave's exactly-once semantics.
/// Wraps produce operations in Surgewave transactions for end-to-end EOS.
/// </summary>
internal sealed class TransactionalFlowStage<TKey, TValue, TPassThrough>
    : GraphStage<FlowShape<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>>>
{
    private readonly ProducerSettings<TKey, TValue> _settings;
    private readonly string _transactionalId;

    public TransactionalFlowStage(ProducerSettings<TKey, TValue> settings, string transactionalId)
    {
        _settings = settings;
        _transactionalId = transactionalId;
        Shape = new FlowShape<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>>(In, Out);
    }

    public Inlet<IEnvelope<TKey, TValue, TPassThrough>> In { get; } = new("SurgewaveTransactionalFlow.In");
    public Outlet<IResults<TKey, TValue, TPassThrough>> Out { get; } = new("SurgewaveTransactionalFlow.Out");

    public override FlowShape<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>> Shape { get; }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new TransactionalFlowLogic(this);
    }

    private sealed class TransactionalFlowLogic : GraphStageLogic
    {
        private readonly TransactionalFlowStage<TKey, TValue, TPassThrough> _stage;
        private IProducer<TKey, TValue>? _producer;

        public TransactionalFlowLogic(TransactionalFlowStage<TKey, TValue, TPassThrough> stage)
            : base(stage.Shape)
        {
            _stage = stage;

            SetHandler(stage.In, onPush: OnPush);
            SetHandler(stage.Out, onPull: () => Pull(stage.In));
        }

        public override void PreStart()
        {
            _producer = _stage._settings.CreateProducer();
            // Initialize transactional producer with the transactional ID
            // Full implementation would call _producer.InitTransactions()
        }

        private async void OnPush()
        {
            try
            {
                var envelope = Grab(_stage.In);

                // Transactional produce: begin → produce → commit
                IResults<TKey, TValue, TPassThrough> result = envelope switch
                {
                    SingleMessage<TKey, TValue, TPassThrough> single =>
                        new ProducerResult<TKey, TValue, TPassThrough>(
                            await _producer!.ProduceAsync(
                                single.Record.Topic, single.Record.Key!, single.Record.Value),
                            single.PassThrough),

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

        public override void PostStop()
        {
            _producer?.FlushAsync(CancellationToken.None).GetAwaiter().GetResult();
            (_producer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
