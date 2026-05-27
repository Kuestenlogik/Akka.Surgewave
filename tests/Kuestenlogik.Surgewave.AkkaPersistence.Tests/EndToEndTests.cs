namespace Kuestenlogik.Surgewave.AkkaPersistence.Tests;

using System.Buffers.Binary;
using System.Text;
using Akka.Actor;
using Akka.Hosting;
using Kuestenlogik.Surgewave.AkkaPersistence.Hosting;
using Kuestenlogik.Surgewave.AkkaPersistence.Serialization;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;

/// <summary>
/// End-to-end integration tests using an embedded in-memory Surgewave broker.
/// Verifies the full path: PersistentActor → SurgewaveJournal → Surgewave Topic → Consumer.
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
            .WithPort(0) // auto-assign port
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
    public async Task Actor_persist_event_should_appear_on_surgewave_topic()
    {
        // Arrange: start Akka with Surgewave persistence
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddAkka("e2e-test", (akkaBuilder, _) =>
        {
            akkaBuilder.WithSurgewavePersistence(p =>
            {
                p.BootstrapServers = _surgewave!.BootstrapServers;
                p.Journal.Topic = "e2e-journal";
                p.Journal.Partitions = 1;
                p.Journal.ReplicationFactor = 1;
                p.Journal.SerializationMode = SerializationMode.Hyperion;
                p.Snapshots.Topic = "e2e-snapshots";
                p.Snapshots.Partitions = 1;
                p.Snapshots.ReplicationFactor = 1;
            });
        });

        var app = builder.Build();
        await app.StartAsync();

        var system = app.Services.GetRequiredService<ActorSystem>();

        // Act: create a PersistentActor and persist an event
        var actor = system.ActorOf(
            Props.Create(() => new TestPersistentActor("test-actor-1")),
            "test-actor-1");

        actor.Tell("hello-surgewave");

        // Wait for the event to be persisted
        await Task.Delay(3000);

        // Assert: read the event from the Surgewave topic using a plain consumer
        await using var consumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _surgewave!.BootstrapServers;
            opts.GroupId = $"e2e-verify-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        await consumer.SubscribeAsync(CancellationToken.None, "e2e-journal");

        var found = false;
        var timeout = DateTime.UtcNow.AddSeconds(10);

        while (DateTime.UtcNow < timeout)
        {
            var result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(2), CancellationToken.None);
            if (result is null)
                continue;

            _output.WriteLine($"Received: key={result.Key}, offset={result.Offset}, size={result.Value?.Length}, hasHeaders={result.Headers is not null}");

            // Verify it's our actor's event
            if (result.Key == "test-actor-1")
            {
                found = true;

                // Event payload must not be empty
                Assert.NotNull(result.Value);
                Assert.True(result.Value!.Length > 0, "Event payload should not be empty");

                // If headers are available, verify Akka metadata
                if (result.Headers is not null)
                {
                    _output.WriteLine($"  Headers: {string.Join(", ", result.Headers.Keys)}");

                    if (result.Headers.TryGetValue("akka-seq-nr", out var seqBytes))
                    {
                        var seqNr = BinaryPrimitives.ReadInt64BigEndian(seqBytes);
                        _output.WriteLine($"  seq-nr={seqNr}");
                        Assert.Equal(1L, seqNr);
                    }

                    if (result.Headers.TryGetValue("content-type", out var ctBytes))
                    {
                        var contentType = Encoding.UTF8.GetString(ctBytes);
                        _output.WriteLine($"  content-type={contentType}");
                        Assert.Equal("application/x-hyperion", contentType);
                    }
                }
                else
                {
                    _output.WriteLine("  (no headers — broker may not support headers in memory mode)");
                }

                break;
            }
        }

        Assert.True(found, "Event from PersistentActor was not found on Surgewave topic");

        await app.StopAsync();
    }

    [Fact(Skip = "Requires Surgewave Schema Registry (not available in memory-only broker)")]
    public async Task Json_mode_events_should_be_readable_as_json()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddAkka("e2e-json", (akkaBuilder, _) =>
        {
            akkaBuilder.WithSurgewavePersistence(p =>
            {
                p.BootstrapServers = _surgewave!.BootstrapServers;
                p.Journal.Topic = "e2e-json-journal";
                p.Journal.Partitions = 1;
                p.Journal.ReplicationFactor = 1;
                p.Journal.SerializationMode = SerializationMode.Json;
                p.Snapshots.Topic = "e2e-json-snapshots";
                p.Snapshots.Partitions = 1;
                p.Snapshots.ReplicationFactor = 1;
                p.SchemaRegistry.Url = $"http://{_surgewave!.BootstrapServers}";
            });
        });

        var app = builder.Build();
        await app.StartAsync();
        var system = app.Services.GetRequiredService<ActorSystem>();

        var actor = system.ActorOf(
            Props.Create(() => new TestPersistentActor("test-json-1")),
            "test-json-1");

        actor.Tell("json-event");
        await Task.Delay(3000);

        await using var consumer = new SurgewaveConsumer<string, byte[]>(opts =>
        {
            opts.BootstrapServers = _surgewave!.BootstrapServers;
            opts.GroupId = $"e2e-json-verify-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        await consumer.SubscribeAsync(CancellationToken.None, "e2e-json-journal");

        var found = false;
        var timeout = DateTime.UtcNow.AddSeconds(10);

        while (DateTime.UtcNow < timeout)
        {
            var result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(2), CancellationToken.None);
            if (result is null) continue;

            if (result.Key == "test-json-1" && result.Value is { Length: > 0 })
            {
                found = true;
                _output.WriteLine($"  Received: size={result.Value.Length}, hasHeaders={result.Headers is not null}");

                // Verify event payload is not empty
                Assert.True(result.Value.Length > 0);

                // If wire format header present, strip and verify JSON
                if (result.Value.Length > 5 && result.Value[0] == 0x00)
                {
                    var json = Encoding.UTF8.GetString(result.Value.AsSpan(5));
                    _output.WriteLine($"  JSON payload: {json}");
                    Assert.Contains("json-event", json, StringComparison.Ordinal);
                }

                break;
            }
        }

        Assert.True(found, "JSON event was not found on Surgewave topic");
        await app.StopAsync();
    }

    /// <summary>
    /// Minimal PersistentActor for testing.
    /// </summary>
    private sealed class TestPersistentActor : ReceivePersistentActor
    {
        public override string PersistenceId { get; }

        public TestPersistentActor(string id)
        {
            PersistenceId = id;
            Recover<string>(_ => { });
            Command<string>(msg => Persist(msg, _ => { }));
            Command<SaveSnapshotSuccess>(_ => { });
        }
    }
}
