namespace Kuestenlogik.Akka.Surgewave.Persistence.Query;

using global::Akka.Configuration;

/// <summary>
/// Settings for SurgewaveReadJournal, parsed from HOCON configuration.
/// </summary>
public sealed class SurgewaveReadJournalSettings
{
    public SurgewaveReadJournalSettings(Config config)
    {
        BootstrapServers = config.GetString("bootstrap-servers", "localhost:9092");
        JournalTopic = config.GetString("journal-topic", "akka-journal");
        RefreshInterval = config.GetTimeSpan("refresh-interval", TimeSpan.FromMilliseconds(250));
        ConsumerGroup = config.GetString("consumer-group", "akka-persistence-query");
        TopicPrefix = config.GetString("topic-prefix", "");
    }

    public string BootstrapServers { get; init; }
    public string JournalTopic { get; init; }
    public TimeSpan RefreshInterval { get; init; }
    public string ConsumerGroup { get; init; }
    public string TopicPrefix { get; init; }

    public string ResolveTopicName(string baseTopic) =>
        string.IsNullOrEmpty(TopicPrefix) ? baseTopic : $"{TopicPrefix}{baseTopic}";
}
