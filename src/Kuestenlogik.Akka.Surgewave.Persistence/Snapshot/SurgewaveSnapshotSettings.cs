namespace Kuestenlogik.Akka.Surgewave.Persistence.Snapshot;

using global::Akka.Configuration;
using Kuestenlogik.Akka.Surgewave.Persistence.Serialization;
using Kuestenlogik.Surgewave.Schema.Registry.Client;

/// <summary>
/// Settings for SurgewaveSnapshotStore, parsed from HOCON configuration.
/// </summary>
public sealed class SurgewaveSnapshotSettings
{
    public SurgewaveSnapshotSettings(Config config)
    {
        BootstrapServers = config.GetString("bootstrap-servers", "localhost:9092");
        Protocol = config.GetString("protocol", "auto");
        SnapshotTopic = config.GetString("snapshot-topic", "akka-snapshots");
        SnapshotTopicPartitions = config.GetInt("snapshot-topic-partitions", 16);
        SnapshotTopicReplicationFactor = config.GetInt("snapshot-topic-replication-factor", 3);
        TopicPrefix = config.GetString("topic-prefix", "");

        SerializationMode = config.GetString("serialization-mode", "opaque").ToLowerInvariant() switch
        {
            "json" => SerializationMode.Json,
            "proto" or "protobuf" => SerializationMode.Proto,
            "schema-registry" => SerializationMode.Json,
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
        }
    }

    public string BootstrapServers { get; init; }
    public string Protocol { get; init; }
    public string SnapshotTopic { get; init; }
    public int SnapshotTopicPartitions { get; init; }
    public int SnapshotTopicReplicationFactor { get; init; }
    public string TopicPrefix { get; init; }

    public SerializationMode SerializationMode { get; init; }
    public string? SchemaRegistryUrl { get; init; }
    public bool SchemaRegistryAutoRegister { get; init; } = true;
    public SubjectNameStrategyType SchemaRegistrySubjectStrategy { get; init; } = SubjectNameStrategyType.TopicRecordName;

    public string ResolveTopicName(string baseTopic) =>
        string.IsNullOrEmpty(TopicPrefix) ? baseTopic : $"{TopicPrefix}{baseTopic}";
}
