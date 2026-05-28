namespace Kuestenlogik.Akka.Surgewave.Streams.Tests;

using System.Text;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Runtime;
using Xunit;
using Xunit.Abstractions;

/// <summary>
/// End-to-end integration tests using an embedded in-memory Surgewave broker.
/// Verifies that Kuestenlogik.Akka.Surgewave.Streams can produce and consume via the Surgewave client.
/// </summary>
public sealed class EndToEndTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private SurgewaveRuntime? _surgewave;

    public EndToEndTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public async Task InitializeAsync()
    {
        _surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithStorageEngine("memory")
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();

        _output.WriteLine($"Surgewave broker started at {_surgewave.BootstrapServers}");
    }

    public async Task DisposeAsync()
    {
        if (_surgewave is not null)
            await _surgewave.DisposeAsync();
    }

    [Fact]
    public async Task Produce_then_consume_should_roundtrip()
    {
        var topic = $"e2e-roundtrip-{Guid.NewGuid():N}";

        // Produce 3 messages
        await using var producer = new SurgewaveProducer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _surgewave!.BootstrapServers;
        });

        await producer.ProduceAsync(topic, "k1", "hello"u8.ToArray());
        await producer.ProduceAsync(topic, "k2", "world"u8.ToArray());
        await producer.ProduceAsync(topic, "k3", "surgewave"u8.ToArray());
        await producer.FlushAsync(CancellationToken.None);

        _output.WriteLine("Produced 3 messages");

        // Consume and verify
        await using var consumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _surgewave!.BootstrapServers;
            opts.GroupId = $"e2e-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        await consumer.SubscribeAsync(CancellationToken.None, topic);

        var received = new List<(string Key, string Value)>();
        var timeout = DateTime.UtcNow.AddSeconds(10);

        while (received.Count < 3 && DateTime.UtcNow < timeout)
        {
            var result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(2), CancellationToken.None);
            if (result is null) continue;

            var key = result.Key ?? "";
            var value = Encoding.UTF8.GetString(result.Value);
            received.Add((key, value));
            _output.WriteLine($"  Received: key={key}, value={value}");
        }

        Assert.Equal(3, received.Count);
        Assert.Contains(received, r => r is ("k1", "hello"));
        Assert.Contains(received, r => r is ("k2", "world"));
        Assert.Contains(received, r => r is ("k3", "surgewave"));
    }

    [Fact]
    public async Task Produce_with_headers_should_preserve_headers()
    {
        var topic = $"e2e-headers-{Guid.NewGuid():N}";

        await using var producer = new SurgewaveProducer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _surgewave!.BootstrapServers;
        });

        var headers = new Dictionary<string, byte[]>
        {
            ["content-type"] = "application/json"u8.ToArray(),
            ["custom-header"] = "test-value"u8.ToArray()
        };

        await producer.ProduceAsync(topic, "key1", "{\"msg\":\"test\"}"u8.ToArray(), headers);
        await producer.FlushAsync(CancellationToken.None);

        _output.WriteLine("Produced message with headers");

        await using var consumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _surgewave!.BootstrapServers;
            opts.GroupId = $"e2e-headers-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        await consumer.SubscribeAsync(CancellationToken.None, topic);

        ConsumeResult<string, byte[]>? result = null;
        var timeout = DateTime.UtcNow.AddSeconds(10);

        while (result is null && DateTime.UtcNow < timeout)
        {
            result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(2), CancellationToken.None);
        }

        Assert.NotNull(result);
        Assert.Equal("key1", result!.Key);

        _output.WriteLine($"  Headers present: {result.Headers is not null}");
        if (result.Headers is not null)
        {
            foreach (var h in result.Headers)
                _output.WriteLine($"  Header: {h.Key} = {Encoding.UTF8.GetString(h.Value)}");

            Assert.True(result.Headers.ContainsKey("content-type"));
            Assert.Equal("application/json", Encoding.UTF8.GetString(result.Headers["content-type"]));
        }
    }
}
