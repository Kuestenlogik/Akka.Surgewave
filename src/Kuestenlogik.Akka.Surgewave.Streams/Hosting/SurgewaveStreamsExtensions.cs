namespace Kuestenlogik.Akka.Surgewave.Streams.Hosting;

using global::Akka.Hosting;

/// <summary>
/// DI extensions for configuring Kuestenlogik.Akka.Surgewave.Streams via Akka.Hosting.
/// </summary>
public static class SurgewaveStreamsExtensions
{
    /// <summary>
    /// Configures Akka.Streams with Surgewave consumer, producer, and committer defaults.
    /// </summary>
    public static AkkaConfigurationBuilder WithSurgewaveStreams(
        this AkkaConfigurationBuilder builder,
        Action<SurgewaveStreamsSetup> configure)
    {
        var setup = new SurgewaveStreamsSetup();
        configure(setup);

        var hocon = GenerateHocon(setup);
        builder.AddHocon(hocon, HoconAddMode.Prepend);

        return builder;
    }

    private static string GenerateHocon(SurgewaveStreamsSetup setup)
    {
        return $$"""
            akka.surgewave {
                consumer {
                    bootstrap-servers = "{{setup.BootstrapServers}}"
                    protocol = "{{setup.Protocol}}"
                    poll-timeout = {{setup.Consumer.PollTimeout.TotalMilliseconds}}ms
                    stop-timeout = {{setup.Consumer.StopTimeout.TotalSeconds}}s
                    schema-registry {
                        url = "{{setup.SchemaRegistry.Url}}"
                    }
                }
                producer {
                    bootstrap-servers = "{{setup.BootstrapServers}}"
                    protocol = "{{setup.Protocol}}"
                    close-timeout = {{setup.Producer.CloseTimeout.TotalSeconds}}s
                    parallelism = {{setup.Producer.Parallelism}}
                    eos-commit-interval = {{setup.Producer.EosCommitInterval.TotalMilliseconds}}ms
                    schema-registry {
                        url = "{{setup.SchemaRegistry.Url}}"
                        auto-register = {{setup.SchemaRegistry.AutoRegister.ToString().ToLowerInvariant()}}
                    }
                }
                committer {
                    max-batch = {{setup.Committer.MaxBatch}}
                    max-interval = {{setup.Committer.MaxInterval.TotalSeconds}}s
                    parallelism = {{setup.Committer.Parallelism}}
                }
                default-dispatcher {
                    type = "Dispatcher"
                    executor = "default-executor"
                }
            }
            """;
    }
}
