// Akka root types (Done, NotUsed, ...) and Akka.Persistence types were
// implicitly in scope under the old `namespace Akka.Persistence.Surgewave.X`
// because Akka and Akka.Persistence were outer namespaces. After the
// v0.2.0 rename to Kuestenlogik.Surgewave.AkkaPersistence, those are no
// longer outer namespaces — we re-import them globally so the per-file
// using lists stay short and Akka-idiomatic.
global using Akka;
global using Akka.Persistence;
global using Akka.Streams;
