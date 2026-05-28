// Mirror the src project's global usings so test files keep resolving Akka
// root + Akka.Streams types that used to be in scope via the old
// `namespace Akka.Streams.Surgewave.Tests` outer namespace.
global using Akka;
global using Akka.Streams;
global using Dsl = Akka.Streams.Dsl;
