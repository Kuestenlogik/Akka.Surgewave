# Akka.Surgewave

Akka.NET integration packages for [Surgewave](https://github.com/Kuestenlogik/Surgewave) — Akka.Streams sources/sinks/flows and an Akka.Persistence journal + snapshot store + read journal, both backed by Surgewave.

Two NuGet packages ship from this repository:

| Package | What it does | Analogous to |
|---|---|---|
| [`Kuestenlogik.Surgewave.AkkaStreams`](https://www.nuget.org/packages/Kuestenlogik.Surgewave.AkkaStreams) | Sources, Sinks and Flows for reactive Surgewave topic integration | [Akka.Streams.Kafka](https://github.com/akkadotnet/Akka.Streams.Kafka) (Alpakka) |
| [`Kuestenlogik.Surgewave.AkkaPersistence`](https://www.nuget.org/packages/Kuestenlogik.Surgewave.AkkaPersistence) | Journal, Snapshot Store and Persistence Query backed by Surgewave topics, with Schema Registry support | [Akka.Persistence.SqlServer](https://github.com/akkadotnet/Akka.Persistence.Sql) (SqlServer backend) |

> **Naming.** The `Akka.*` prefix on nuget.org is verified-reserved by the Akka.NET team (owner `Akka`). Third-party plugins either get donated to that account or ship under their own brand. Surgewave takes the second route — the `Kuestenlogik.Surgewave.Akka{Streams,Persistence}` ids and namespaces sit under the Surgewave brand. Repo and packages stay aligned (`AkkaStreams`/`AkkaPersistence` as single tokens) so the C# compiler doesn't confuse `using Akka.Streams.Dsl;` with our own namespace tree.

## Kuestenlogik.Surgewave.AkkaStreams

```bash
dotnet add package Kuestenlogik.Surgewave.AkkaStreams
```

```csharp
using Kuestenlogik.Surgewave.AkkaStreams;
```

### Features

- **PlainSource / CommittableSource** — Consumer sources with backpressure and offset commit
- **PlainSink / FlexiFlow** — Producer stages with delivery feedback and passthrough support
- **Transactional** — End-to-end exactly-once consume-transform-produce pipelines
- **Committer** — Batched offset commits with configurable intervals
- **Schema Registry** — Typed serialization/deserialization via Surgewave Schema Registry
- **Partitioned Sources** — Sub-source per partition for partition-local processing

### Quick Start

```csharp
var control = SurgewaveConsumer
    .CommittableSource(consumerSettings, Subscriptions.Topics("orders"))
    .SelectAsync(10, async msg =>
    {
        await ProcessOrder(msg.Record.Key, msg.Record.Value);
        return msg.CommittableOffset;
    })
    .ToMaterialized(
        Committer.Sink(CommitterSettings.Create(system)),
        DrainingControl<Done>.Create)
    .Run(materializer);
```

## Kuestenlogik.Surgewave.AkkaPersistence

```bash
dotnet add package Kuestenlogik.Surgewave.AkkaPersistence
```

```csharp
using Kuestenlogik.Surgewave.AkkaPersistence;
```

### Features

- **AsyncWriteJournal** — Surgewave-backed event journal with index-based fast replay
- **SnapshotStore** — Compacted topic for automatic snapshot lifecycle
- **Persistence Query** — EventsByPersistenceId, EventsByTag, AllEvents (live + current)
- **Two Serialization Modes** — Opaque (Akka serializer passthrough) and Schema Registry (Protobuf/Avro/JSON)
- **Schema Registry Integration** — Events become first-class citizens in the Surgewave ecosystem
- **Exactly-Once Semantics** — Optional transactional writes for AtomicWrite guarantees
- **Multi-Tenancy** — Topic prefix support for multiple actor systems on the same cluster

### Quick Start

```csharp
builder.Services.AddAkka("my-system", (akkaBuilder, sp) =>
{
    akkaBuilder
        .WithSurgewavePersistence(surgewave =>
        {
            surgewave.BootstrapServers = "localhost:9092";
            surgewave.Journal.Topic = "akka-journal";
            surgewave.Snapshots.Topic = "akka-snapshots";
            surgewave.SchemaRegistry.Url = "http://localhost:8081";
        })
        .WithSurgewaveReadJournal();
});
```

## History

This repository consolidates the previously separate `Akka.Streams.Surgewave` and `Akka.Persistence.Surgewave` repositories (each at v0.1.1 on nuget.org under `Kuestenlogik.Akka.Streams.Surgewave` / `Kuestenlogik.Akka.Persistence.Surgewave`). v0.2.0 ships from this combined repo with the new ids above; the old repos are archived with a pointer to this one.

## License

Apache-2.0
