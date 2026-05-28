namespace Kuestenlogik.Akka.Surgewave.Streams.Control;

/// <summary>
/// Control interface for managing the lifecycle of a running Surgewave stream.
/// </summary>
public interface IControl
{
    /// <summary>
    /// Initiates a graceful shutdown of the source/sink.
    /// </summary>
    Task Shutdown();

    /// <summary>
    /// Returns a task that completes when the source/sink has shut down.
    /// </summary>
    Task IsShutdown { get; }
}
