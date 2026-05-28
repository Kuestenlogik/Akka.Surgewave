namespace Kuestenlogik.Akka.Surgewave.Persistence.Query;

using global::Akka.Actor;
using global::Akka.Configuration;
using global::Akka.Persistence.Query;

/// <summary>
/// Provider that creates SurgewaveReadJournal instances.
/// Registered via HOCON configuration.
/// </summary>
public sealed class SurgewaveReadJournalProvider : IReadJournalProvider
{
    private readonly ExtendedActorSystem _system;
    private readonly Config _config;

    public SurgewaveReadJournalProvider(ExtendedActorSystem system, Config config)
    {
        _system = system;
        _config = config;
    }

    public IReadJournal GetReadJournal()
    {
        return new SurgewaveReadJournal(_system, _config);
    }
}
