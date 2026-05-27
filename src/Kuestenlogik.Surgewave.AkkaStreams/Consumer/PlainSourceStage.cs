namespace Kuestenlogik.Surgewave.AkkaStreams.Consumer;

using Akka.Streams;
using Akka.Streams.Stage;
using Kuestenlogik.Surgewave.AkkaStreams.Control;
using Kuestenlogik.Surgewave.AkkaStreams.Settings;
using Kuestenlogik.Surgewave.AkkaStreams.Subscriptions;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// GraphStage that creates a Source emitting ConsumeResult from Surgewave.
/// No offset commit support — offsets must be tracked externally.
/// </summary>
internal sealed class PlainSourceStage<TKey, TValue>
    : GraphStageWithMaterializedValue<SourceShape<ConsumeResult<TKey, TValue>>, IControl>
{
    private readonly ConsumerSettings<TKey, TValue> _settings;
    private readonly ISubscription _subscription;

    public PlainSourceStage(ConsumerSettings<TKey, TValue> settings, ISubscription subscription)
    {
        _settings = settings;
        _subscription = subscription;
        Shape = new SourceShape<ConsumeResult<TKey, TValue>>(Out);
    }

    public Outlet<ConsumeResult<TKey, TValue>> Out { get; } = new("SurgewavePlainSource.Out");

    public override SourceShape<ConsumeResult<TKey, TValue>> Shape { get; }

    public override ILogicAndMaterializedValue<IControl> CreateLogicAndMaterializedValue(
        Attributes inheritedAttributes)
    {
        var control = new SurgewaveConsumerControl();
        var logic = new PlainSourceLogic(this, control);
        return new LogicAndMaterializedValue<IControl>(logic, control);
    }

    private sealed class PlainSourceLogic : GraphStageLogic
    {
        private readonly PlainSourceStage<TKey, TValue> _stage;
        private readonly SurgewaveConsumerControl _control;
        private IConsumer<TKey, TValue>? _consumer;

        public PlainSourceLogic(PlainSourceStage<TKey, TValue> stage, SurgewaveConsumerControl control)
            : base(stage.Shape)
        {
            _stage = stage;
            _control = control;

            SetHandler(stage.Out, onPull: OnPull);
        }

        public override void PreStart()
        {
            _consumer = _stage._settings.CreateConsumer();
            SubscribeConsumer();
        }

        private void SubscribeConsumer()
        {
            switch (_stage._subscription)
            {
                case TopicSubscription ts:
                    _consumer!.Subscribe([.. ts.Topics]);
                    break;
                case AssignmentSubscription a:
                    foreach (var (topic, partition) in a.Assignments)
                        _consumer!.Assign(topic, partition);
                    break;
                case AssignmentWithOffsetSubscription ao:
                    foreach (var (topic, partition, offset) in ao.Assignments)
                        _consumer!.Assign(topic, partition, offset);
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
                    Push(_stage.Out, result);
                else if (!_control.ShutdownToken.IsCancellationRequested)
                    PollAndEmit(); // Keep polling
                else
                    CompleteStage();
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
