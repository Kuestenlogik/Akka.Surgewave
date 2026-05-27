// Akka root types (Done, NotUsed, ...) were implicitly in scope under the
// old `namespace Akka.Streams.Surgewave.X` because Akka was an outer
// namespace. After the v0.2.0 rename to Kuestenlogik.Surgewave.AkkaStreams,
// Akka is no longer an outer namespace — we re-import it globally so the
// per-file using lists stay short and Akka-idiomatic.
global using Akka;
global using Akka.Streams;
