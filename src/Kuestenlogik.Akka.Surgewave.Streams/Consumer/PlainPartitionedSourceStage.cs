namespace Kuestenlogik.Akka.Surgewave.Streams.Consumer;

using global::Akka.Streams.Dsl;
using global::Akka.Streams.Stage;
using Kuestenlogik.Akka.Surgewave.Streams.Control;
using Kuestenlogik.Akka.Surgewave.Streams.Settings;
using Kuestenlogik.Akka.Surgewave.Streams.Subscriptions;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// GraphStage that emits a sub-Source per assigned partition.
/// Ideal for partition-local processing with independent backpressure per partition.
/// </summary>
internal sealed class PlainPartitionedSourceStage<TKey, TValue>
    : GraphStageWithMaterializedValue<
        SourceShape<(TopicPartitionOffset, Source<ConsumeResult<TKey, TValue>, NotUsed>)>,
        IControl>
{
    private readonly ConsumerSettings<TKey, TValue> _settings;
    private readonly ISubscription _subscription;

    public PlainPartitionedSourceStage(
        ConsumerSettings<TKey, TValue> settings,
        ISubscription subscription)
    {
        _settings = settings;
        _subscription = subscription;
        Shape = new SourceShape<(TopicPartitionOffset, Source<ConsumeResult<TKey, TValue>, NotUsed>)>(Out);
    }

    public Outlet<(TopicPartitionOffset, Source<ConsumeResult<TKey, TValue>, NotUsed>)> Out { get; }
        = new("SurgewavePartitionedSource.Out");

    public override SourceShape<(TopicPartitionOffset, Source<ConsumeResult<TKey, TValue>, NotUsed>)> Shape { get; }

    public override ILogicAndMaterializedValue<IControl> CreateLogicAndMaterializedValue(
        Attributes inheritedAttributes)
    {
        var control = new SurgewaveConsumerControl();
        var logic = new PartitionedSourceLogic(this, control);
        return new LogicAndMaterializedValue<IControl>(logic, control);
    }

    private sealed class PartitionedSourceLogic : GraphStageLogic
    {
        private readonly PlainPartitionedSourceStage<TKey, TValue> _stage;
        private readonly SurgewaveConsumerControl _control;

        public PartitionedSourceLogic(
            PlainPartitionedSourceStage<TKey, TValue> stage,
            SurgewaveConsumerControl control)
            : base(stage.Shape)
        {
            _stage = stage;
            _control = control;
            SetHandler(stage.Out, onPull: () => { /* Partitions emitted on assignment */ });
        }

        public override void PreStart()
        {
            // Partition assignment creates sub-sources
            // Full implementation would hook into consumer rebalance events
        }

        public override void PostStop()
        {
            _control.SignalShutdown();
        }
    }
}
