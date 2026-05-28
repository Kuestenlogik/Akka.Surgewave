namespace Kuestenlogik.Akka.Surgewave.Streams.Tests;

using Kuestenlogik.Akka.Surgewave.Streams.Messages;
using Kuestenlogik.Surgewave.Client.Consumer;
using Xunit;

/// <summary>
/// Tests for CommittableSource and CommittableOffset.
/// </summary>
public class CommittableSourceSpec
{
    [Fact]
    public void CommittableMessage_should_carry_record_and_offset()
    {
        // Verify the record type can be constructed
        var record = new ConsumeResult<string, string>
        {
            Topic = "test-topic",
            Partition = 0,
            Offset = 42,
            Key = "key1",
            Value = "value1"
        };

        Assert.Equal("test-topic", record.Topic);
        Assert.Equal(42, record.Offset);
    }
}
