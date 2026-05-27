namespace Kuestenlogik.Surgewave.AkkaPersistence.Hosting;

using Akka.Hosting;

/// <summary>
/// DI extensions for configuring Surgewave-backed Akka.NET Persistence via Akka.Hosting.
/// </summary>
public static class SurgewavePersistenceExtensions
{
    /// <summary>
    /// Configures Akka.Persistence to use Surgewave as the backend for Journal and Snapshot Store.
    /// </summary>
    public static AkkaConfigurationBuilder WithSurgewavePersistence(
        this AkkaConfigurationBuilder builder,
        Action<SurgewavePersistenceSetup> configure)
    {
        var setup = new SurgewavePersistenceSetup();
        configure(setup);

        var hocon = GenerateHocon(setup);
        builder.AddHocon(hocon, HoconAddMode.Prepend);

        return builder;
    }

    /// <summary>
    /// Configures the Surgewave Read Journal for Persistence Query.
    /// </summary>
    public static AkkaConfigurationBuilder WithSurgewaveReadJournal(
        this AkkaConfigurationBuilder builder,
        Action<SurgewaveReadJournalSetup>? configure = null)
    {
        var setup = new SurgewaveReadJournalSetup();
        configure?.Invoke(setup);

        var hocon = GenerateReadJournalHocon(setup);
        builder.AddHocon(hocon, HoconAddMode.Prepend);

        return builder;
    }

    private static string GenerateHocon(SurgewavePersistenceSetup setup)
    {
        return $$"""
            akka.persistence {
                journal {
                    plugin = "akka.persistence.journal.surgewave"
                    surgewave {
                        class = "Kuestenlogik.Surgewave.AkkaPersistence.Journal.SurgewaveJournal, Kuestenlogik.Surgewave.AkkaPersistence"
                        plugin-dispatcher = "akka.actor.default-dispatcher"
                        bootstrap-servers = "{{setup.BootstrapServers}}"
                        protocol = "{{setup.Protocol}}"
                        journal-topic = "{{setup.Journal.Topic}}"
                        index-topic = "{{setup.Journal.IndexTopic}}"
                        journal-topic-partitions = {{setup.Journal.Partitions}}
                        journal-topic-replication-factor = {{setup.Journal.ReplicationFactor}}
                        topic-prefix = "{{setup.TopicPrefix}}"
                        serialization-mode = "{{SerializationModeToString(setup.Journal.SerializationMode)}}"
                        produce-batch-size = {{setup.Journal.ProduceBatchSize}}
                        produce-linger-ms = {{setup.Journal.ProduceLingerMs}}
                        replay-timeout = {{setup.Journal.ReplayTimeout.TotalSeconds}}s
                        replay-read-batch-size = {{setup.Journal.ReplayReadBatchSize}}
                        enable-eos = {{setup.Journal.EnableEos.ToString().ToLowerInvariant()}}
                        schema-registry {
                            url = "{{setup.SchemaRegistry.Url}}"
                            subject-strategy = "{{setup.SchemaRegistry.SubjectStrategy.ToString().ToLowerInvariant()}}"
                            auto-register = {{setup.SchemaRegistry.AutoRegister.ToString().ToLowerInvariant()}}
                            compatibility-level = "{{setup.SchemaRegistry.CompatibilityLevel}}"
                        }
                        circuit-breaker {
                            max-failures = 5
                            call-timeout = 10s
                            reset-timeout = 30s
                        }
                    }
                }
                snapshot-store {
                    plugin = "akka.persistence.snapshot-store.surgewave"
                    surgewave {
                        class = "Kuestenlogik.Surgewave.AkkaPersistence.Snapshot.SurgewaveSnapshotStore, Kuestenlogik.Surgewave.AkkaPersistence"
                        plugin-dispatcher = "akka.actor.default-dispatcher"
                        bootstrap-servers = "{{setup.BootstrapServers}}"
                        protocol = "{{setup.Protocol}}"
                        snapshot-topic = "{{setup.Snapshots.Topic}}"
                        snapshot-topic-partitions = {{setup.Snapshots.Partitions}}
                        snapshot-topic-replication-factor = {{setup.Snapshots.ReplicationFactor}}
                        topic-prefix = "{{setup.TopicPrefix}}"
                        serialization-mode = "{{SerializationModeToString(setup.Snapshots.SerializationMode)}}"
                        schema-registry {
                            url = "{{setup.SchemaRegistry.Url}}"
                            subject-strategy = "{{setup.SchemaRegistry.SubjectStrategy.ToString().ToLowerInvariant()}}"
                            auto-register = {{setup.SchemaRegistry.AutoRegister.ToString().ToLowerInvariant()}}
                        }
                        circuit-breaker {
                            max-failures = 5
                            call-timeout = 10s
                            reset-timeout = 30s
                        }
                    }
                }
            }
            """;
    }

    private static string SerializationModeToString(Serialization.SerializationMode mode) => mode switch
    {
        Serialization.SerializationMode.Json => "json",
        Serialization.SerializationMode.Proto => "proto",
        _ => "hyperion"
    };

    private static string GenerateReadJournalHocon(SurgewaveReadJournalSetup setup)
    {
        return $$"""
            akka.persistence.query {
                surgewave-read-journal {
                    class = "Kuestenlogik.Surgewave.AkkaPersistence.Query.SurgewaveReadJournalProvider, Kuestenlogik.Surgewave.AkkaPersistence"
                    refresh-interval = {{setup.RefreshInterval.TotalMilliseconds}}ms
                    consumer-group = "{{setup.ConsumerGroup}}"
                }
            }
            """;
    }
}
