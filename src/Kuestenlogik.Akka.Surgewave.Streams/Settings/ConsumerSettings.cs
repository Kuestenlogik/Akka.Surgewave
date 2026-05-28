namespace Kuestenlogik.Akka.Surgewave.Streams.Settings;

using global::Akka.Actor;
using global::Akka.Configuration;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Serialization;

/// <summary>
/// Typed configuration for Surgewave consumer stages, built from HOCON or fluent API.
/// </summary>
public sealed class ConsumerSettings<TKey, TValue>
{
    public string BootstrapServers { get; private set; } = "localhost:9092";
    public string Protocol { get; private set; } = "auto";
    public string? GroupId { get; private set; }
    public AutoOffsetReset AutoOffsetReset { get; private set; } = AutoOffsetReset.Latest;
    public bool EnableAutoCommit { get; private set; }
    public TimeSpan PollTimeout { get; private set; } = TimeSpan.FromMilliseconds(50);
    public TimeSpan StopTimeout { get; private set; } = TimeSpan.FromSeconds(30);
    public TimeSpan CloseTimeout { get; private set; } = TimeSpan.FromSeconds(20);
    public TimeSpan CommitTimeout { get; private set; } = TimeSpan.FromSeconds(15);
    public string? SchemaRegistryUrl { get; private set; }
    public IDeserializer<TKey>? KeyDeserializer { get; private set; }
    public IDeserializer<TValue>? ValueDeserializer { get; private set; }
    public IAsyncDeserializer<TKey>? AsyncKeyDeserializer { get; private set; }
    public IAsyncDeserializer<TValue>? AsyncValueDeserializer { get; private set; }

    private ConsumerSettings() { }

    /// <summary>
    /// Creates consumer settings from HOCON configuration.
    /// </summary>
    public static ConsumerSettings<TKey, TValue> Create(ActorSystem system)
    {
        var config = system.Settings.Config.GetConfig("akka.surgewave.consumer");
        var settings = new ConsumerSettings<TKey, TValue>();

        if (config is not null)
        {
            settings.BootstrapServers = config.GetString("bootstrap-servers", settings.BootstrapServers);
            settings.Protocol = config.GetString("protocol", settings.Protocol);
            settings.PollTimeout = config.GetTimeSpan("poll-timeout", settings.PollTimeout);
            settings.StopTimeout = config.GetTimeSpan("stop-timeout", settings.StopTimeout);
            settings.CloseTimeout = config.GetTimeSpan("close-timeout", settings.CloseTimeout);
            settings.CommitTimeout = config.GetTimeSpan("commit-timeout", settings.CommitTimeout);

            if (config.HasPath("schema-registry.url"))
                settings.SchemaRegistryUrl = config.GetString("schema-registry.url");
        }

        return settings;
    }

    public ConsumerSettings<TKey, TValue> WithBootstrapServers(string servers)
    {
        BootstrapServers = servers;
        return this;
    }

    public ConsumerSettings<TKey, TValue> WithProtocol(string protocol)
    {
        Protocol = protocol;
        return this;
    }

    public ConsumerSettings<TKey, TValue> WithGroupId(string groupId)
    {
        GroupId = groupId;
        return this;
    }

    public ConsumerSettings<TKey, TValue> WithAutoOffsetReset(AutoOffsetReset reset)
    {
        AutoOffsetReset = reset;
        return this;
    }

    public ConsumerSettings<TKey, TValue> WithSchemaRegistry(string url)
    {
        SchemaRegistryUrl = url;
        return this;
    }

    public ConsumerSettings<TKey, TValue> WithKeyDeserializer(IDeserializer<TKey> deserializer)
    {
        KeyDeserializer = deserializer;
        return this;
    }

    public ConsumerSettings<TKey, TValue> WithValueDeserializer(IDeserializer<TValue> deserializer)
    {
        ValueDeserializer = deserializer;
        return this;
    }

    public ConsumerSettings<TKey, TValue> WithAsyncKeyDeserializer(IAsyncDeserializer<TKey> deserializer)
    {
        AsyncKeyDeserializer = deserializer;
        return this;
    }

    public ConsumerSettings<TKey, TValue> WithAsyncValueDeserializer(IAsyncDeserializer<TValue> deserializer)
    {
        AsyncValueDeserializer = deserializer;
        return this;
    }

    /// <summary>
    /// Creates a Surgewave consumer from these settings.
    /// </summary>
    internal IConsumer<TKey, TValue> CreateConsumer()
    {
        return new SurgewaveConsumer<TKey, TValue>(opts =>
        {
            opts.BootstrapServers = BootstrapServers;
            opts.GroupId = GroupId;
            opts.AutoOffsetReset = AutoOffsetReset;
            opts.EnableAutoCommit = EnableAutoCommit;

            if (KeyDeserializer is not null)
                opts.KeyDeserializer = KeyDeserializer;
            if (ValueDeserializer is not null)
                opts.ValueDeserializer = ValueDeserializer;
            if (AsyncKeyDeserializer is not null)
                opts.AsyncKeyDeserializer = AsyncKeyDeserializer;
            if (AsyncValueDeserializer is not null)
                opts.AsyncValueDeserializer = AsyncValueDeserializer;
        });
    }
}
