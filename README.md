# Akka.Surgewave

Akka.NET integration packages for [Surgewave](https://github.com/Kuestenlogik/Surgewave) — Akka.Streams sources/sinks/flows and an Akka.Persistence journal + snapshot store + read journal, both backed by Surgewave.

Two NuGet packages ship from this repository:

| Package | What it does | Analogous to |
|---|---|---|
| [`Kuestenlogik.Akka.Surgewave.Streams`](https://www.nuget.org/packages/Kuestenlogik.Akka.Surgewave.Streams) | Sources, Sinks and Flows for reactive Surgewave topic integration | [Akka.Streams.Kafka](https://github.com/akkadotnet/Akka.Streams.Kafka) (Alpakka) |
| [`Kuestenlogik.Akka.Surgewave.Persistence`](https://www.nuget.org/packages/Kuestenlogik.Akka.Surgewave.Persistence) | Journal, Snapshot Store and Persistence Query backed by Surgewave topics, with Schema Registry support | [Akka.Persistence.SqlServer](https://github.com/akkadotnet/Akka.Persistence.Sql) (SqlServer backend) |

> **Naming.** The `Akka.*` prefix on nuget.org is verified-reserved by the Akka.NET team (owner `Akka`). Third-party plugins either get donated to that account or ship under their own brand. Surgewave takes the second route — the `Kuestenlogik.Akka.Surgewave.{Streams,Persistence}` ids and namespaces sit under the Kuestenlogik brand. The `.Surgewave.` segment between `Akka` and `Streams`/`Persistence` is deliberate: it means our namespace never contains the substring `Akka.Streams` or `Akka.Persistence`, so the C# compiler can't confuse `using Akka.Streams.Dsl;` (the external Akka.NET package) with our own namespace tree.

## Kuestenlogik.Akka.Surgewave.Streams

```bash
dotnet add package Kuestenlogik.Akka.Surgewave.Streams
```

```csharp
using Kuestenlogik.Akka.Surgewave.Streams;
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

## Kuestenlogik.Akka.Surgewave.Persistence

```bash
dotnet add package Kuestenlogik.Akka.Surgewave.Persistence
```

```csharp
using Kuestenlogik.Akka.Surgewave.Persistence;
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

This repository consolidates the previously separate `Akka.Streams.Surgewave` and `Akka.Persistence.Surgewave` repositories (each at v0.1.1 on nuget.org under `Kuestenlogik.Akka.Streams.Surgewave` / `Kuestenlogik.Akka.Persistence.Surgewave`). v0.3.0 ships from this combined repo with the ids above; the old repos are archived with a pointer to this one. (A short-lived v0.2.0 used the interim ids `Kuestenlogik.Surgewave.AkkaStreams` / `.AkkaPersistence`; those are unlisted in favour of the `Kuestenlogik.Akka.Surgewave.*` ids.)

## License

Apache-2.0
