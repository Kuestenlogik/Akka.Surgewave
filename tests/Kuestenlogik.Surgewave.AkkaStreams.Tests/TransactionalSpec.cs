namespace Kuestenlogik.Surgewave.AkkaStreams.Tests;

using Xunit;

/// <summary>
/// Tests for Transactional source/flow (EOS).
/// Requires a running Surgewave broker.
/// </summary>
public class TransactionalSpec
{
    [Fact(Skip = "Requires running Surgewave broker")]
    public void TransactionalFlow_should_commit_offsets_atomically()
    {
        // Integration test placeholder
    }
}
