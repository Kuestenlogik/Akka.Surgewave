// Akka root types (Done, NotUsed, ...) were implicitly in scope under the
// old `namespace Akka.Streams.Surgewave.X` because Akka was an outer
// namespace. After the v0.2.0 rename to Kuestenlogik.Akka.Surgewave.Streams,
// Akka is no longer an outer namespace — we re-import it globally so the
// per-file using lists stay short and Akka-idiomatic.
global using Akka;
global using Akka.Streams;
// `Dsl.Flow` / `Dsl.Sink` referenced the Akka.Streams.Dsl namespace via the
// old outer namespace `Akka.Streams`. With the new namespace this alias
// restores the `Dsl.`-qualified access.
global using Dsl = Akka.Streams.Dsl;
