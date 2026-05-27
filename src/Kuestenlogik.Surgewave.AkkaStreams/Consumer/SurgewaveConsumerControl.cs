namespace Kuestenlogik.Surgewave.AkkaStreams.Consumer;

using Kuestenlogik.Surgewave.AkkaStreams.Control;

/// <summary>
/// Control implementation for Surgewave consumer sources.
/// </summary>
internal sealed class SurgewaveConsumerControl : IControl
{
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly TaskCompletionSource _shutdownTcs = new();

    public CancellationToken ShutdownToken => _shutdownCts.Token;

    public Task Shutdown()
    {
        _shutdownCts.Cancel();
        return _shutdownTcs.Task;
    }

    public Task IsShutdown => _shutdownTcs.Task;

    internal void SignalShutdown()
    {
        _shutdownTcs.TrySetResult();
    }

    internal void SignalError(Exception ex)
    {
        _shutdownTcs.TrySetException(ex);
    }
}
