namespace Kuestenlogik.Akka.Surgewave.Streams.Settings;

using global::Akka.Actor;
using global::Akka.Configuration;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Serialization;

/// <summary>
/// Typed configuration for Surgewave producer stages, built from HOCON or fluent API.
/// </summary>
public sealed class ProducerSettings<TKey, TValue>
{
    public string BootstrapServers { get; private set; } = "localhost:9092";
    public string Protocol { get; private set; } = "auto";
    public TimeSpan CloseTimeout { get; private set; } = TimeSpan.FromSeconds(60);
    public int Parallelism { get; private set; } = 100;
    public TimeSpan EosCommitInterval { get; private set; } = TimeSpan.FromMilliseconds(100);
    public string? SchemaRegistryUrl { get; private set; }
    public ISerializer<TKey>? KeySerializer { get; private set; }
    public ISerializer<TValue>? ValueSerializer { get; private set; }
    public IAsyncSerializer<TKey>? AsyncKeySerializer { get; private set; }
    public IAsyncSerializer<TValue>? AsyncValueSerializer { get; private set; }

    private ProducerSettings() { }

    /// <summary>
    /// Creates producer settings from HOCON configuration.
    /// </summary>
    public static ProducerSettings<TKey, TValue> Create(ActorSystem system)
    {
        var config = system.Settings.Config.GetConfig("akka.surgewave.producer");
        var settings = new ProducerSettings<TKey, TValue>();

        if (config is not null)
        {
            settings.BootstrapServers = config.GetString("bootstrap-servers", settings.BootstrapServers);
            settings.Protocol = config.GetString("protocol", settings.Protocol);
            settings.CloseTimeout = config.GetTimeSpan("close-timeout", settings.CloseTimeout);
            settings.Parallelism = config.GetInt("parallelism", settings.Parallelism);
            settings.EosCommitInterval = config.GetTimeSpan("eos-commit-interval", settings.EosCommitInterval);

            if (config.HasPath("schema-registry.url"))
                settings.SchemaRegistryUrl = config.GetString("schema-registry.url");
        }

        return settings;
    }

    public ProducerSettings<TKey, TValue> WithBootstrapServers(string servers)
    {
        BootstrapServers = servers;
        return this;
    }

    public ProducerSettings<TKey, TValue> WithProtocol(string protocol)
    {
        Protocol = protocol;
        return this;
    }

    public ProducerSettings<TKey, TValue> WithSchemaRegistry(string url)
    {
        SchemaRegistryUrl = url;
        return this;
    }

    public ProducerSettings<TKey, TValue> WithKeySerializer(ISerializer<TKey> serializer)
    {
        KeySerializer = serializer;
        return this;
    }

    public ProducerSettings<TKey, TValue> WithValueSerializer(ISerializer<TValue> serializer)
    {
        ValueSerializer = serializer;
        return this;
    }

    public ProducerSettings<TKey, TValue> WithAsyncKeySerializer(IAsyncSerializer<TKey> serializer)
    {
        AsyncKeySerializer = serializer;
        return this;
    }

    public ProducerSettings<TKey, TValue> WithAsyncValueSerializer(IAsyncSerializer<TValue> serializer)
    {
        AsyncValueSerializer = serializer;
        return this;
    }

    /// <summary>
    /// Creates a Surgewave producer from these settings.
    /// </summary>
    internal IProducer<TKey, TValue> CreateProducer()
    {
        return new SurgewaveProducer<TKey, TValue>(opts =>
        {
            opts.BootstrapServers = BootstrapServers;

            if (KeySerializer is not null)
                opts.KeySerializer = KeySerializer;
            if (ValueSerializer is not null)
                opts.ValueSerializer = ValueSerializer;
            if (AsyncKeySerializer is not null)
                opts.AsyncKeySerializer = AsyncKeySerializer;
            if (AsyncValueSerializer is not null)
                opts.AsyncValueSerializer = AsyncValueSerializer;
        });
    }
}
