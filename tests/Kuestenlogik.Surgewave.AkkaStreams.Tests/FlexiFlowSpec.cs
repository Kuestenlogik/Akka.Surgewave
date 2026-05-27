namespace Kuestenlogik.Surgewave.AkkaStreams.Tests;

using Kuestenlogik.Surgewave.AkkaStreams.Messages;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Xunit;

/// <summary>
/// Tests for FlexiFlow producer stage with passthrough support.
/// </summary>
public class FlexiFlowSpec
{
    [Fact]
    public void ProducerMessage_Single_should_carry_record_and_passthrough()
    {
        var record = new ProducerRecord<string, string>
        {
            Topic = "output-topic",
            Key = "key1",
            Value = "value1"
        };

        var message = ProducerMessage.Single(record, "my-passthrough");

        Assert.Equal("output-topic", message.Record.Topic);
        Assert.Equal("my-passthrough", message.PassThrough);
    }

    [Fact]
    public void ProducerMessage_Multi_should_carry_multiple_records()
    {
        var records = new ProducerRecord<string, string>[]
        {
            new() { Topic = "topic", Key = "k1", Value = "v1" },
            new() { Topic = "topic", Key = "k2", Value = "v2" }
        };

        var message = ProducerMessage.Multi<string, string, int>(records, 42);

        Assert.Equal(2, message.Records.Count);
        Assert.Equal(42, message.PassThrough);
    }

    [Fact]
    public void ProducerMessage_PassThroughOnly_should_carry_no_record()
    {
        var message = ProducerMessage.PassThroughOnly<string, string, string>("offset-123");

        Assert.Equal("offset-123", message.PassThrough);
    }

    [Fact]
    public void ProducerResult_should_carry_result_and_passthrough()
    {
        var produceResult = new ProduceResult
        {
            Topic = "output",
            Partition = 3,
            Offset = 100
        };

        var result = new ProducerResult<string, string, int>(produceResult, 42);

        Assert.Equal(100, result.Result.Offset);
        Assert.Equal(42, result.PassThrough);
    }
}
