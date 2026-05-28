namespace Kuestenlogik.Akka.Surgewave.Persistence.Journal;

using global::Akka.Configuration;
using Kuestenlogik.Akka.Surgewave.Persistence.Serialization;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Client.SchemaRegistry;

/// <summary>
/// Settings for SurgewaveJournal, parsed from HOCON configuration.
/// </summary>
public sealed class SurgewaveJournalSettings
{
    public SurgewaveJournalSettings(Config config)
    {
        BootstrapServers = config.GetString("bootstrap-servers", "localhost:9092");
        Protocol = config.GetString("protocol", "auto");
        JournalTopic = config.GetString("journal-topic", "akka-journal");
        IndexTopic = config.GetString("index-topic", "akka-journal-index");
        JournalTopicPartitions = config.GetInt("journal-topic-partitions", 16);
        JournalTopicReplicationFactor = config.GetInt("journal-topic-replication-factor", 3);
        TopicPrefix = config.GetString("topic-prefix", "");

        SerializationMode = config.GetString("serialization-mode", "opaque").ToLowerInvariant() switch
        {
            "json" => SerializationMode.Json,
            "proto" or "protobuf" => SerializationMode.Proto,
            "schema-registry" => SerializationMode.Json, // backward compat
            _ => SerializationMode.Hyperion
        };

        if (config.HasPath("schema-registry"))
        {
            SchemaRegistryUrl = config.GetString("schema-registry.url", "http://localhost:8081");
            SchemaRegistryAutoRegister = config.GetBoolean("schema-registry.auto-register", true);
            SchemaRegistrySubjectStrategy = config.GetString("schema-registry.subject-strategy", "topic-record-name") switch
            {
                "topic-name" => SubjectNameStrategyType.TopicName,
                "record-name" => SubjectNameStrategyType.RecordName,
                _ => SubjectNameStrategyType.TopicRecordName
            };
            CompatibilityLevel = config.GetString("schema-registry.compatibility-level", "backward");
        }

        ProduceBatchSize = config.GetInt("produce-batch-size", 100);
        ProduceLingerMs = config.GetInt("produce-linger-ms", 5);
        ReplayTimeout = config.GetTimeSpan("replay-timeout", TimeSpan.FromSeconds(30));
        ReplayReadBatchSize = config.GetInt("replay-read-batch-size", 500);
        EnableEos = config.GetBoolean("enable-eos", false);
    }

    public string BootstrapServers { get; init; }
    public string Protocol { get; init; }
    public string JournalTopic { get; init; }
    public string IndexTopic { get; init; }
    public int JournalTopicPartitions { get; init; }
    public int JournalTopicReplicationFactor { get; init; }
    public string TopicPrefix { get; init; }

    public SerializationMode SerializationMode { get; init; }
    public string? SchemaRegistryUrl { get; init; }
    public bool SchemaRegistryAutoRegister { get; init; } = true;
    public SubjectNameStrategyType SchemaRegistrySubjectStrategy { get; init; } = SubjectNameStrategyType.TopicRecordName;
    public string CompatibilityLevel { get; init; } = "backward";

    public int ProduceBatchSize { get; init; }
    public int ProduceLingerMs { get; init; }
    public TimeSpan ReplayTimeout { get; init; }
    public int ReplayReadBatchSize { get; init; }
    public bool EnableEos { get; init; }

    /// <summary>
    /// Resolved schema registry operations instance.
    /// Set by the hosting integration or created lazily.
    /// </summary>
    public ISchemaRegistryOperations? SchemaRegistryOperations { get; set; }

    public string ResolveTopicName(string baseTopic) =>
        string.IsNullOrEmpty(TopicPrefix) ? baseTopic : $"{TopicPrefix}{baseTopic}";
}
