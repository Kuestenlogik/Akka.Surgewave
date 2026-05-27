namespace Kuestenlogik.Surgewave.AkkaStreams.Committer;

using Akka.Actor;
using Akka.Configuration;

/// <summary>
/// Configuration for batched offset commits.
/// </summary>
public sealed class CommitterSettings
{
    public int MaxBatch { get; private set; } = 1000;
    public TimeSpan MaxInterval { get; private set; } = TimeSpan.FromSeconds(5);
    public int Parallelism { get; private set; } = 3;

    private CommitterSettings() { }

    /// <summary>
    /// Creates committer settings from HOCON configuration.
    /// </summary>
    public static CommitterSettings Create(ActorSystem system)
    {
        var config = system.Settings.Config.GetConfig("akka.surgewave.committer");
        var settings = new CommitterSettings();

        if (config is not null)
        {
            settings.MaxBatch = config.GetInt("max-batch", settings.MaxBatch);
            settings.MaxInterval = config.GetTimeSpan("max-interval", settings.MaxInterval);
            settings.Parallelism = config.GetInt("parallelism", settings.Parallelism);
        }

        return settings;
    }

    public CommitterSettings WithMaxBatch(int maxBatch)
    {
        MaxBatch = maxBatch;
        return this;
    }

    public CommitterSettings WithMaxInterval(TimeSpan maxInterval)
    {
        MaxInterval = maxInterval;
        return this;
    }

    public CommitterSettings WithParallelism(int parallelism)
    {
        Parallelism = parallelism;
        return this;
    }
}
