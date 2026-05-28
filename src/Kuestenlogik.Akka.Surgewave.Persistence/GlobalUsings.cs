// Akka root types (Done, NotUsed, ...), Akka.Persistence and Akka.Configuration
// were implicitly in scope under the old `namespace Akka.Persistence.Surgewave.X`
// because Akka was an outer namespace. After the rename to
// Kuestenlogik.Akka.Surgewave.Persistence, `Akka` as a plain identifier resolves
// to our own `Kuestenlogik.Akka` prefix — so any qualified `Akka.Xyz` reference
// breaks. These global usings (evaluated in the global scope, where `Akka` is
// unambiguously the Akka.NET root) re-import the namespaces so per-file code
// stays unqualified and Akka-idiomatic.
global using Akka;
global using Akka.Persistence;
global using Akka.Streams;
