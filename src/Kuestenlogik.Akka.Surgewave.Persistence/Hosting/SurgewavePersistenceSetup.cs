namespace Kuestenlogik.Akka.Surgewave.Persistence.Hosting;

using Kuestenlogik.Akka.Surgewave.Persistence.Serialization;
using Kuestenlogik.Surgewave.Client.SchemaRegistry;

/// <summary>
/// Typed configuration object for Surgewave Persistence, used by WithSurgewavePersistence().
/// </summary>
public sealed class SurgewavePersistenceSetup
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Protocol { get; set; } = "auto";
    public string TopicPrefix { get; set; } = "";

    public SurgewaveJournalSetup Journal { get; } = new();
    public SurgewaveSnapshotSetup Snapshots { get; } = new();
    public SurgewaveSchemaRegistrySetup SchemaRegistry { get; } = new();
}

public sealed class SurgewaveJournalSetup
{
    public string Topic { get; set; } = "akka-journal";
    public string IndexTopic { get; set; } = "akka-journal-index";
    public int Partitions { get; set; } = 16;
    public int ReplicationFactor { get; set; } = 3;
    public SerializationMode SerializationMode { get; set; } = SerializationMode.Json;
    public int ProduceBatchSize { get; set; } = 100;
    public int ProduceLingerMs { get; set; } = 5;
    public TimeSpan ReplayTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public int ReplayReadBatchSize { get; set; } = 500;
    public bool EnableEos { get; set; }
}

public sealed class SurgewaveSnapshotSetup
{
    public string Topic { get; set; } = "akka-snapshots";
    public int Partitions { get; set; } = 16;
    public int ReplicationFactor { get; set; } = 3;
    public SerializationMode SerializationMode { get; set; } = SerializationMode.Json;
}

public sealed class SurgewaveSchemaRegistrySetup
{
    public string Url { get; set; } = "http://localhost:8081";
    public SubjectNameStrategyType SubjectStrategy { get; set; } = SubjectNameStrategyType.TopicRecordName;
    public bool AutoRegister { get; set; } = true;
    public string CompatibilityLevel { get; set; } = "backward";
}

public sealed class SurgewaveReadJournalSetup
{
    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromMilliseconds(250);
    public string ConsumerGroup { get; set; } = "akka-persistence-query";
}
