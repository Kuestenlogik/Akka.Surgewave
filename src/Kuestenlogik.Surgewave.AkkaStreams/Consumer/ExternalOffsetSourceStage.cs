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
/// Source stage for scenarios where offsets are stored in an external system
/// (e.g., database) rather than in Surgewave's consumer group protocol.
/// </summary>
internal sealed class ExternalOffsetSourceStage<TKey, TValue>
    : GraphStageWithMaterializedValue<SourceShape<ConsumeResult<TKey, TValue>>, IControl>
{
    private readonly ConsumerSettings<TKey, TValue> _settings;
    private readonly ISubscription _subscription;
    private readonly Func<IReadOnlyList<(string Topic, int Partition)>, Task<IReadOnlyList<TopicPartitionOffset>>> _getOffsetsOnAssign;
    private readonly Func<IReadOnlyList<(string Topic, int Partition)>, Task>? _onRevoke;

    public ExternalOffsetSourceStage(
        ConsumerSettings<TKey, TValue> settings,
        ISubscription subscription,
        Func<IReadOnlyList<(string Topic, int Partition)>, Task<IReadOnlyList<TopicPartitionOffset>>> getOffsetsOnAssign,
        Func<IReadOnlyList<(string Topic, int Partition)>, Task>? onRevoke = null)
    {
        _settings = settings;
        _subscription = subscription;
        _getOffsetsOnAssign = getOffsetsOnAssign;
        _onRevoke = onRevoke;
        Shape = new SourceShape<ConsumeResult<TKey, TValue>>(Out);
    }

    public Outlet<ConsumeResult<TKey, TValue>> Out { get; } = new("SurgewaveExternalOffsetSource.Out");

    public override SourceShape<ConsumeResult<TKey, TValue>> Shape { get; }

    public override ILogicAndMaterializedValue<IControl> CreateLogicAndMaterializedValue(
        Attributes inheritedAttributes)
    {
        var control = new SurgewaveConsumerControl();
        var logic = new ExternalOffsetSourceLogic(this, control);
        return new LogicAndMaterializedValue<IControl>(logic, control);
    }

    private sealed class ExternalOffsetSourceLogic : GraphStageLogic
    {
        private readonly ExternalOffsetSourceStage<TKey, TValue> _stage;
        private readonly SurgewaveConsumerControl _control;
        private IConsumer<TKey, TValue>? _consumer;

        public ExternalOffsetSourceLogic(
            ExternalOffsetSourceStage<TKey, TValue> stage,
            SurgewaveConsumerControl control)
            : base(stage.Shape)
        {
            _stage = stage;
            _control = control;
            SetHandler(stage.Out, onPull: OnPull);
        }

        public override void PreStart()
        {
            _consumer = _stage._settings.CreateConsumer();

            // Subscribe and seek to externally-stored offsets
            switch (_stage._subscription)
            {
                case TopicSubscription ts:
                    _consumer.Subscribe([.. ts.Topics]);
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
                    PollAndEmit();
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
