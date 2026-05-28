namespace Kuestenlogik.Akka.Surgewave.Persistence.Tests;

using global::Akka.Configuration;
using Kuestenlogik.Akka.Surgewave.Persistence.Journal;
using Kuestenlogik.Akka.Surgewave.Persistence.Serialization;
using Kuestenlogik.Surgewave.Client.SchemaRegistry;
using Xunit;

public class SurgewaveJournalSettingsTests
{
    [Fact]
    public void Should_use_sensible_defaults()
    {
        var config = ConfigurationFactory.Empty;
        var settings = new SurgewaveJournalSettings(config);

        Assert.Equal("localhost:9092", settings.BootstrapServers);
        Assert.Equal("auto", settings.Protocol);
        Assert.Equal("akka-journal", settings.JournalTopic);
        Assert.Equal("akka-journal-index", settings.IndexTopic);
        Assert.Equal(16, settings.JournalTopicPartitions);
        Assert.Equal(3, settings.JournalTopicReplicationFactor);
        Assert.Equal("", settings.TopicPrefix);
        Assert.Equal(SerializationMode.Hyperion, settings.SerializationMode);
        Assert.Equal(100, settings.ProduceBatchSize);
        Assert.Equal(5, settings.ProduceLingerMs);
        Assert.Equal(TimeSpan.FromSeconds(30), settings.ReplayTimeout);
        Assert.Equal(500, settings.ReplayReadBatchSize);
        Assert.False(settings.EnableEos);
    }

    [Fact]
    public void Should_parse_schema_registry_mode()
    {
        var config = ConfigurationFactory.ParseString("""
            serialization-mode = "schema-registry"
            schema-registry {
                url = "http://registry:8081"
                auto-register = false
                subject-strategy = "record-name"
                compatibility-level = "full"
            }
            """);

        var settings = new SurgewaveJournalSettings(config);

        Assert.Equal(SerializationMode.Json, settings.SerializationMode);
        Assert.Equal("http://registry:8081", settings.SchemaRegistryUrl);
        Assert.False(settings.SchemaRegistryAutoRegister);
        Assert.Equal(SubjectNameStrategyType.RecordName, settings.SchemaRegistrySubjectStrategy);
        Assert.Equal("full", settings.CompatibilityLevel);
    }

    [Fact]
    public void ResolveTopicName_should_add_prefix()
    {
        var config = ConfigurationFactory.ParseString("""
            topic-prefix = "myapp-"
            """);

        var settings = new SurgewaveJournalSettings(config);

        Assert.Equal("myapp-akka-journal", settings.ResolveTopicName("akka-journal"));
        Assert.Equal("myapp-akka-snapshots", settings.ResolveTopicName("akka-snapshots"));
    }

    [Fact]
    public void ResolveTopicName_should_return_base_when_no_prefix()
    {
        var config = ConfigurationFactory.Empty;
        var settings = new SurgewaveJournalSettings(config);

        Assert.Equal("akka-journal", settings.ResolveTopicName("akka-journal"));
    }

    [Fact]
    public void Should_parse_custom_topic_names()
    {
        var config = ConfigurationFactory.ParseString("""
            bootstrap-servers = "broker1:9092,broker2:9092"
            journal-topic = "custom-journal"
            index-topic = "custom-index"
            journal-topic-partitions = 32
            journal-topic-replication-factor = 5
            enable-eos = true
            replay-read-batch-size = 1000
            """);

        var settings = new SurgewaveJournalSettings(config);

        Assert.Equal("broker1:9092,broker2:9092", settings.BootstrapServers);
        Assert.Equal("custom-journal", settings.JournalTopic);
        Assert.Equal("custom-index", settings.IndexTopic);
        Assert.Equal(32, settings.JournalTopicPartitions);
        Assert.Equal(5, settings.JournalTopicReplicationFactor);
        Assert.True(settings.EnableEos);
        Assert.Equal(1000, settings.ReplayReadBatchSize);
    }
}
