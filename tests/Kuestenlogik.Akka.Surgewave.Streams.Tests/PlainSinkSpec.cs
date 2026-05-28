namespace Kuestenlogik.Akka.Surgewave.Streams.Tests;

using Kuestenlogik.Surgewave.Client.Abstractions;
using Xunit;

/// <summary>
/// Tests for PlainSink producer stage.
/// </summary>
public class PlainSinkSpec
{
    [Fact]
    public void ProducerRecord_should_carry_topic_key_value()
    {
        var record = new ProducerRecord<string, string>
        {
            Topic = "test-topic",
            Key = "key1",
            Value = "value1"
        };

        Assert.Equal("test-topic", record.Topic);
        Assert.Equal("key1", record.Key);
        Assert.Equal("value1", record.Value);
    }
}
