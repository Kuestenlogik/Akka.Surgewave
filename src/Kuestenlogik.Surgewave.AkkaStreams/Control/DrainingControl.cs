namespace Kuestenlogik.Surgewave.AkkaStreams.Control;


/// <summary>
/// Materialized value that supports orderly shutdown with drain.
/// Waits until all in-flight messages are committed before completing.
/// </summary>
public sealed class DrainingControl<T> : IControl
{
    private readonly IControl _control;
    private readonly Task<T> _streamCompletion;

    private DrainingControl(IControl control, Task<T> streamCompletion)
    {
        _control = control;
        _streamCompletion = streamCompletion;
    }

    /// <summary>
    /// Creates a DrainingControl from a (IControl, Task&lt;T&gt;) materialized value pair.
    /// </summary>
    public static DrainingControl<T> Create((IControl Control, Task<T> StreamCompletion) tuple) =>
        new(tuple.Control, tuple.StreamCompletion);

    /// <summary>
    /// Initiates a graceful shutdown and waits for all in-flight work to complete.
    /// </summary>
    public async Task<T> DrainAndShutdown()
    {
        await _control.Shutdown();
        return await _streamCompletion;
    }

    public Task Shutdown() => _control.Shutdown();

    public Task IsShutdown => _control.IsShutdown;

    /// <summary>
    /// The stream completion task.
    /// </summary>
    public Task<T> StreamCompletion => _streamCompletion;
}
