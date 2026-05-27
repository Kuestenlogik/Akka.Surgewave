namespace Kuestenlogik.Surgewave.AkkaPersistence.Journal;

/// <summary>
/// Routes tagged events to optional secondary tag topics for high-volume
/// EventsByTag queries. Default implementation uses header-based filtering
/// (no secondary topics needed).
/// </summary>
public sealed class TagTopicRouter
{
    private readonly SurgewaveJournalSettings _settings;

    public TagTopicRouter(SurgewaveJournalSettings settings)
    {
        _settings = settings;
    }

    /// <summary>
    /// Returns the topic name for a tag-specific secondary topic.
    /// Used only when tag topics are enabled (future Option B/C).
    /// </summary>
    public string GetTagTopic(string tag)
    {
        return _settings.ResolveTopicName($"akka-journal-tag-{tag}");
    }
}
