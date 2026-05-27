namespace Kuestenlogik.Surgewave.AkkaStreams.Tests;

using Kuestenlogik.Surgewave.AkkaStreams.Messages;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Xunit;

public class ProducerMessageTests
{
    [Fact]
    public void Single_should_implement_IEnvelope()
    {
        var record = new ProducerRecord<string, string> { Topic = "t", Value = "v" };
        var msg = ProducerMessage.Single(record, 42);

        IEnvelope<string, string, int> envelope = msg;
        Assert.Equal(42, envelope.PassThrough);
    }

    [Fact]
    public void Multi_should_implement_IEnvelope()
    {
        var records = new[]
        {
            new ProducerRecord<string, string> { Topic = "t", Value = "v1" },
            new ProducerRecord<string, string> { Topic = "t", Value = "v2" }
        };
        var msg = ProducerMessage.Multi<string, string, string>(records, "pass");

        IEnvelope<string, string, string> envelope = msg;
        Assert.Equal("pass", envelope.PassThrough);
        Assert.Equal(2, msg.Records.Count);
    }

    [Fact]
    public void PassThroughOnly_should_implement_IEnvelope()
    {
        var msg = ProducerMessage.PassThroughOnly<string, string, int>(99);

        IEnvelope<string, string, int> envelope = msg;
        Assert.Equal(99, envelope.PassThrough);
    }

    [Fact]
    public void ProducerResult_should_implement_IResults()
    {
        var result = new ProducerResult<string, string, int>(
            new ProduceResult { Topic = "t", Partition = 0, Offset = 1 }, 42);

        IResults<string, string, int> iResult = result;
        Assert.Equal(42, iResult.PassThrough);
    }

    [Fact]
    public void PassThroughResult_should_implement_IResults()
    {
        var result = new PassThroughResult<string, string, string>("data");

        IResults<string, string, string> iResult = result;
        Assert.Equal("data", iResult.PassThrough);
    }
}
