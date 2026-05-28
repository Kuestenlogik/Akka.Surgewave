// Mirror the src project's global usings so test files keep resolving Akka
// root + Akka.Persistence types (ReceivePersistentActor, ...) that used to be
// in scope via the old `namespace Akka.Persistence.Surgewave.Tests` outer
// namespace.
global using Akka;
global using Akka.Persistence;
global using Akka.Streams;
