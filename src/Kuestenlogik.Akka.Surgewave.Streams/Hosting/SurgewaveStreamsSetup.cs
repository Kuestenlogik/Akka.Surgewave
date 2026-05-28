namespace Kuestenlogik.Akka.Surgewave.Streams.Hosting;

using Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Typed configuration object for Surgewave Streams, used by WithSurgewaveStreams().
/// </summary>
public sealed class SurgewaveStreamsSetup
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Protocol { get; set; } = "auto";

    public SurgewaveStreamsConsumerSetup Consumer { get; } = new();
    public SurgewaveStreamsProducerSetup Producer { get; } = new();
    public SurgewaveStreamsSchemaRegistrySetup SchemaRegistry { get; } = new();
    public SurgewaveStreamsCommitterSetup Committer { get; } = new();
}

public sealed class SurgewaveStreamsConsumerSetup
{
    public string? GroupId { get; set; }
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromMilliseconds(50);
    public TimeSpan StopTimeout { get; set; } = TimeSpan.FromSeconds(30);
}

public sealed class SurgewaveStreamsProducerSetup
{
    public int Parallelism { get; set; } = 100;
    public int LingerMs { get; set; } = 5;
    public TimeSpan CloseTimeout { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan EosCommitInterval { get; set; } = TimeSpan.FromMilliseconds(100);
}

public sealed class SurgewaveStreamsSchemaRegistrySetup
{
    public string Url { get; set; } = "http://localhost:8081";
    public bool AutoRegister { get; set; } = true;
}

public sealed class SurgewaveStreamsCommitterSetup
{
    public int MaxBatch { get; set; } = 1000;
    public TimeSpan MaxInterval { get; set; } = TimeSpan.FromSeconds(5);
    public int Parallelism { get; set; } = 3;
}
