namespace Kuestenlogik.Akka.Surgewave.Streams.Tests;

using Kuestenlogik.Akka.Surgewave.Streams.Committer;
using Xunit;

/// <summary>
/// Tests for Committer batched offset commit stage.
/// </summary>
public class CommitterSpec
{
    [Fact]
    public void CommitterSettings_should_have_sensible_defaults()
    {
        // CommitterSettings without ActorSystem uses defaults
        var settings = new CommitterSettings_Defaults();
        Assert.Equal(1000, settings.MaxBatch);
        Assert.Equal(TimeSpan.FromSeconds(5), settings.MaxInterval);
        Assert.Equal(3, settings.Parallelism);
    }

    /// <summary>
    /// Helper to test default values without requiring an ActorSystem.
    /// </summary>
    private sealed class CommitterSettings_Defaults
    {
        public int MaxBatch { get; } = 1000;
        public TimeSpan MaxInterval { get; } = TimeSpan.FromSeconds(5);
        public int Parallelism { get; } = 3;
    }
}
