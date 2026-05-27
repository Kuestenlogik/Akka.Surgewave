namespace Kuestenlogik.Surgewave.AkkaPersistence.Tests;

using Kuestenlogik.Surgewave.AkkaPersistence.Serialization;
using Xunit;

public class EventEnvelopeCodecTests
{
    [Fact]
    public void EncodeLong_DecodeLong_should_roundtrip()
    {
        var values = new long[] { 0, 1, -1, long.MinValue, long.MaxValue, 42, 1234567890L };

        foreach (var expected in values)
        {
            var encoded = EventEnvelopeCodec.EncodeLong(expected);
            Assert.Equal(8, encoded.Length);

            var decoded = EventEnvelopeCodec.DecodeLong(encoded);
            Assert.Equal(expected, decoded);
        }
    }

    [Fact]
    public void EncodeInt_DecodeInt_should_roundtrip()
    {
        var values = new[] { 0, 1, -1, int.MinValue, int.MaxValue, 42 };

        foreach (var expected in values)
        {
            var encoded = EventEnvelopeCodec.EncodeInt(expected);
            Assert.Equal(4, encoded.Length);

            var decoded = EventEnvelopeCodec.DecodeInt(encoded);
            Assert.Equal(expected, decoded);
        }
    }

    [Fact]
    public void EncodeString_DecodeString_should_roundtrip()
    {
        var values = new[] { "", "hello", "unit-PzGrenBtl212", "Umlaute: aeoeue", "emoji: \U0001F680" };

        foreach (var expected in values)
        {
            var encoded = EventEnvelopeCodec.EncodeString(expected);
            var decoded = EventEnvelopeCodec.DecodeString(encoded);
            Assert.Equal(expected, decoded);
        }
    }

    [Fact]
    public void EncodeTags_DecodeTags_should_roundtrip()
    {
        var tags = new[] { "position", "status", "combat" };
        var encoded = EventEnvelopeCodec.EncodeTags(tags);
        var decoded = EventEnvelopeCodec.DecodeTags(encoded);

        Assert.Equal(tags, decoded);
    }

    [Fact]
    public void DecodeTags_should_handle_empty_string()
    {
        var encoded = EventEnvelopeCodec.EncodeString("");
        var decoded = EventEnvelopeCodec.DecodeTags(encoded);
        Assert.Empty(decoded);
    }

    [Fact]
    public void DecodeTags_should_trim_whitespace()
    {
        var encoded = EventEnvelopeCodec.EncodeString("  position , status , combat  ");
        var decoded = EventEnvelopeCodec.DecodeTags(encoded);
        Assert.Equal(["position", "status", "combat"], decoded);
    }

    [Fact]
    public void EncodeLong_should_use_big_endian()
    {
        var encoded = EventEnvelopeCodec.EncodeLong(1L);
        // Big-endian: MSB first
        Assert.Equal(0, encoded[0]);
        Assert.Equal(0, encoded[1]);
        Assert.Equal(0, encoded[6]);
        Assert.Equal(1, encoded[7]);
    }

    [Theory]
    [InlineData(EventEnvelopeCodec.SequenceNrHeader, "akka-seq-nr")]
    [InlineData(EventEnvelopeCodec.ManifestHeader, "akka-manifest")]
    [InlineData(EventEnvelopeCodec.TagsHeader, "akka-tags")]
    [InlineData(EventEnvelopeCodec.WriterUuidHeader, "akka-writer-uuid")]
    [InlineData(EventEnvelopeCodec.TimestampHeader, "akka-timestamp")]
    [InlineData(EventEnvelopeCodec.SerializerIdHeader, "akka-serializer-id")]
    [InlineData(EventEnvelopeCodec.SnapshotSeqNrHeader, "akka-snapshot-seq-nr")]
    [InlineData(EventEnvelopeCodec.SnapshotTimestampHeader, "akka-snapshot-timestamp")]
    public void Header_constants_should_have_expected_values(string actual, string expected)
    {
        Assert.Equal(expected, actual);
    }
}
