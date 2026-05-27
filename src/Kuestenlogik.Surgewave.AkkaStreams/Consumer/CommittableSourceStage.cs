namespace Kuestenlogik.Surgewave.AkkaStreams.Consumer;

using Akka.Streams;
using Akka.Streams.Stage;
using Kuestenlogik.Surgewave.AkkaStreams.Control;
using Kuestenlogik.Surgewave.AkkaStreams.Messages;
using Kuestenlogik.Surgewave.AkkaStreams.Settings;
using Kuestenlogik.Surgewave.AkkaStreams.Subscriptions;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// GraphStage that creates a Source emitting CommittableMessage — each message
/// carries a CommittableOffset for at-least-once processing.
/// </summary>
internal sealed class CommittableSourceStage<TKey, TValue>
    : GraphStageWithMaterializedValue<SourceShape<CommittableMessage<TKey, TValue>>, IControl>
{
    private readonly ConsumerSettings<TKey, TValue> _settings;
    private readonly ISubscription _subscription;

    public CommittableSourceStage(ConsumerSettings<TKey, TValue> settings, ISubscription subscription)
    {
        _settings = settings;
        _subscription = subscription;
        Shape = new SourceShape<CommittableMessage<TKey, TValue>>(Out);
    }

    public Outlet<CommittableMessage<TKey, TValue>> Out { get; } = new("SurgewaveCommittableSource.Out");

    public override SourceShape<CommittableMessage<TKey, TValue>> Shape { get; }

    public override ILogicAndMaterializedValue<IControl> CreateLogicAndMaterializedValue(
        Attributes inheritedAttributes)
    {
        var control = new SurgewaveConsumerControl();
        var logic = new CommittableSourceLogic(this, control);
        return new LogicAndMaterializedValue<IControl>(logic, control);
    }

    private sealed class CommittableSourceLogic : GraphStageLogic
    {
        private readonly CommittableSourceStage<TKey, TValue> _stage;
        private readonly SurgewaveConsumerControl _control;
        private IConsumer<TKey, TValue>? _consumer;

        public CommittableSourceLogic(
            CommittableSourceStage<TKey, TValue> stage,
            SurgewaveConsumerControl control)
            : base(stage.Shape)
        {
            _stage = stage;
            _control = control;

            SetHandler(stage.Out, onPull: OnPull);
        }

        public override void PreStart()
        {
            var consumer = _stage._settings.CreateConsumer();
            _consumer = consumer;

            switch (_stage._subscription)
            {
                case TopicSubscription ts:
                    _consumer.Subscribe([.. ts.Topics]);
                    break;
                case AssignmentSubscription a:
                    foreach (var (topic, partition) in a.Assignments)
                        _consumer.Assign(topic, partition);
                    break;
                case AssignmentWithOffsetSubscription ao:
                    foreach (var (topic, partition, offset) in ao.Assignments)
                        _consumer.Assign(topic, partition, offset);
                    break;
            }
        }

        private void OnPull()
        {
            PollAndEmit();
        }

        private async void PollAndEmit()
        {
            try
            {
                var result = await _consumer!.ConsumeAsync(
                    _stage._settings.PollTimeout,
                    _control.ShutdownToken);

                if (result is not null)
                {
                    var consumer = _consumer!;
                    var offset = new CommittableOffset(
                        (tpo, ct) => consumer.CommitAsync(tpo, ct),
                        result.Topic,
                        result.Partition,
                        result.Offset,
                        _stage._settings.GroupId ?? "");

                    var message = new CommittableMessage<TKey, TValue>
                    {
                        Record = result,
                        CommittableOffset = offset
                    };

                    Push(_stage.Out, message);
                }
                else if (!_control.ShutdownToken.IsCancellationRequested)
                {
                    PollAndEmit();
                }
                else
                {
                    CompleteStage();
                }
            }
            catch (OperationCanceledException)
            {
                CompleteStage();
            }
            catch (Exception ex)
            {
                _control.SignalError(ex);
                FailStage(ex);
            }
        }

        public override void PostStop()
        {
            (_consumer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _control.SignalShutdown();
        }
    }
}
