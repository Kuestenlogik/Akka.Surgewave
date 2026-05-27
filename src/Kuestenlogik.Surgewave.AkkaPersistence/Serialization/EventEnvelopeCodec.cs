namespace Kuestenlogik.Surgewave.AkkaPersistence.Serialization;

using System.Buffers.Binary;
using System.Text;

/// <summary>
/// Encodes and decodes Akka metadata in Surgewave message headers.
/// Headers keep the event body as pure Protobuf/Avro/JSON — readable
/// for any Surgewave consumer without Akka dependency.
/// </summary>
public static class EventEnvelopeCodec
{
    public const string SequenceNrHeader = "akka-seq-nr";
    public const string ManifestHeader = "akka-manifest";
    public const string SenderHeader = "akka-sender";
    public const string TagsHeader = "akka-tags";
    public const string WriterUuidHeader = "akka-writer-uuid";
    public const string TimestampHeader = "akka-timestamp";
    public const string SerializerIdHeader = "akka-serializer-id";
    public const string SnapshotSeqNrHeader = "akka-snapshot-seq-nr";
    public const string SnapshotTimestampHeader = "akka-snapshot-timestamp";

    public static byte[] EncodeLong(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        return bytes;
    }

    public static long DecodeLong(ReadOnlySpan<byte> data)
    {
        return BinaryPrimitives.ReadInt64BigEndian(data);
    }

    public static byte[] EncodeInt(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        return bytes;
    }

    public static int DecodeInt(ReadOnlySpan<byte> data)
    {
        return BinaryPrimitives.ReadInt32BigEndian(data);
    }

    public static byte[] EncodeString(string value)
    {
        return Encoding.UTF8.GetBytes(value);
    }

    public static string DecodeString(ReadOnlySpan<byte> data)
    {
        return Encoding.UTF8.GetString(data);
    }

    public static string[] DecodeTags(ReadOnlySpan<byte> data)
    {
        var tagString = DecodeString(data);
        return string.IsNullOrEmpty(tagString)
            ? []
            : tagString.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    public static byte[] EncodeTags(IEnumerable<string> tags)
    {
        return EncodeString(string.Join(",", tags));
    }
}
