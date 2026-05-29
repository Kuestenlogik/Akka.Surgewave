namespace Kuestenlogik.Akka.Surgewave.Persistence.Tests;

using global::Akka.Configuration;

/// <summary>
/// HOCON configuration for SurgewaveJournal TCK tests.
/// </summary>
public static class SurgewaveJournalSpecConfig
{
    public static Config Create(string bootstrapServers = "localhost:9092")
    {
        return ConfigurationFactory.ParseString($$"""
            akka.persistence {
                publish-plugin-commands = on
                journal {
                    plugin = "akka.persistence.journal.surgewave"
                    surgewave {
                        class = "Kuestenlogik.Akka.Surgewave.Persistence.Journal.SurgewaveJournal, Kuestenlogik.Akka.Surgewave.Persistence"
                        plugin-dispatcher = "akka.actor.default-dispatcher"
                        bootstrap-servers = "{{bootstrapServers}}"
                        protocol = "auto"
                        journal-topic = "akka-journal-test"
                        index-topic = "akka-journal-index-test"
                        journal-topic-partitions = 4
                        journal-topic-replication-factor = 1
                        serialization-mode = "opaque"
                        produce-batch-size = 10
                        produce-linger-ms = 1
                        replay-timeout = 10s
                        replay-read-batch-size = 100
                        enable-eos = false
                    }
                }
                snapshot-store {
                    plugin = "akka.persistence.snapshot-store.surgewave"
                    surgewave {
                        class = "Kuestenlogik.Akka.Surgewave.Persistence.Snapshot.SurgewaveSnapshotStore, Kuestenlogik.Akka.Surgewave.Persistence"
                        plugin-dispatcher = "akka.actor.default-dispatcher"
                        bootstrap-servers = "{{bootstrapServers}}"
                        protocol = "auto"
                        snapshot-topic = "akka-snapshots-test"
                        snapshot-topic-partitions = 4
                        snapshot-topic-replication-factor = 1
                        serialization-mode = "opaque"
                    }
                }
            }
            akka.test.single-expect-default = 10s
            """);
    }
}

public static class SurgewaveSnapshotSpecConfig
{
    public static Config Create(string bootstrapServers = "localhost:9092")
    {
        return SurgewaveJournalSpecConfig.Create(bootstrapServers);
    }
}
