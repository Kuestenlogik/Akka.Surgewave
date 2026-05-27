namespace Kuestenlogik.Surgewave.AkkaStreams.Committer;

using Akka.Streams.Dsl;
using Kuestenlogik.Surgewave.AkkaStreams.Messages;

/// <summary>
/// Factory for offset commit stages that batch commits for efficiency.
/// </summary>
public static class Committer
{
    /// <summary>
    /// Creates a Sink that collects CommittableOffsets and commits them
    /// in batches according to the configured settings.
    /// </summary>
    public static Sink<CommittableOffset, Task<Done>> Sink(CommitterSettings settings)
    {
        return Dsl.Flow.Create<CommittableOffset>()
            .GroupedWithin(settings.MaxBatch, settings.MaxInterval)
            .SelectAsync(settings.Parallelism, async batch =>
            {
                var list = batch.ToList();
                if (list.Count > 0)
                {
                    var highest = list[^1];
                    await highest.CommitAsync();
                }
                return Done.Instance;
            })
            .ToMaterialized(Dsl.Sink.Ignore<Done>(), Keep.Right);
    }

    /// <summary>
    /// Creates a Flow that commits offsets in batches and passes them through.
    /// </summary>
    public static Dsl.Flow<CommittableOffset, CommittableOffset, NotUsed> CommitFlow(CommitterSettings settings)
    {
        return Dsl.Flow.Create<CommittableOffset>()
            .GroupedWithin(settings.MaxBatch, settings.MaxInterval)
            .SelectAsync(settings.Parallelism, async batch =>
            {
                var list = batch.ToList();
                if (list.Count > 0)
                {
                    var highest = list[^1];
                    await highest.CommitAsync();
                }
                return list.AsEnumerable();
            })
            .SelectMany(batch => batch);
    }
}
