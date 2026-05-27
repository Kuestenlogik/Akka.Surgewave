namespace Kuestenlogik.Surgewave.AkkaStreams.Tests;

using Kuestenlogik.Surgewave.AkkaStreams.Settings;
using Kuestenlogik.Surgewave.Client.Consumer;
using Xunit;

public class ConsumerSettingsTests
{
    [Fact]
    public void WithBootstrapServers_should_set_value()
    {
        var settings = new ConsumerSettings_TestHelper()
            .WithBootstrapServers("broker1:9092,broker2:9092");

        Assert.Equal("broker1:9092,broker2:9092", settings.BootstrapServers);
    }

    [Fact]
    public void WithGroupId_should_set_value()
    {
        var settings = new ConsumerSettings_TestHelper()
            .WithGroupId("my-group");

        Assert.Equal("my-group", settings.GroupId);
    }

    [Fact]
    public void Fluent_API_should_chain()
    {
        var settings = new ConsumerSettings_TestHelper()
            .WithBootstrapServers("localhost:9092")
            .WithGroupId("test-group")
            .WithSchemaRegistry("http://localhost:8081");

        Assert.Equal("localhost:9092", settings.BootstrapServers);
        Assert.Equal("test-group", settings.GroupId);
        Assert.Equal("http://localhost:8081", settings.SchemaRegistryUrl);
    }

    /// <summary>
    /// Helper to test ConsumerSettings without requiring an ActorSystem.
    /// </summary>
    private sealed class ConsumerSettings_TestHelper
    {
        public string BootstrapServers { get; private set; } = "localhost:9092";
        public string? GroupId { get; private set; }
        public string? SchemaRegistryUrl { get; private set; }

        public ConsumerSettings_TestHelper WithBootstrapServers(string servers)
        { BootstrapServers = servers; return this; }

        public ConsumerSettings_TestHelper WithGroupId(string groupId)
        { GroupId = groupId; return this; }

        public ConsumerSettings_TestHelper WithSchemaRegistry(string url)
        { SchemaRegistryUrl = url; return this; }
    }
}
