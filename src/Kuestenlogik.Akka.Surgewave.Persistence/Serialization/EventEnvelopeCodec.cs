namespace Kuestenlogik.Akka.Surgewave.Persistence.Serialization;

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

    // Tombstone markers for SnapshotStore.DeleteAsync. The record body is
    // empty; LoadAsync recognises any of these headers and removes the
    // matching snapshot(s) from its result set. We carry the criteria as
    // application-level headers instead of using a Kafka null-value
    // compaction tombstone because Akka.Persistence's Delete semantics
    // need to target a *specific* snapshot (by seqNr) or a *range* (by
    // criteria), not "drop everything for this key".
    public const string SnapshotTombstoneSeqNrHeader = "akka-snapshot-tombstone-seq-nr";
    // 0 ticks (DateTime.MinValue) means "ignore the stored timestamp" — the
    // tombstone matches every snapshot at that seqNr regardless of when it
    // was saved. Any other value must match the stored timestamp exactly.
    public const string SnapshotTombstoneTimestampHeader = "akka-snapshot-tombstone-timestamp";
    public const string SnapshotTombstoneMaxSeqNrHeader = "akka-snapshot-tombstone-max-seq-nr";
    public const string SnapshotTombstoneMinSeqNrHeader = "akka-snapshot-tombstone-min-seq-nr";
    public const string SnapshotTombstoneMaxTimestampHeader = "akka-snapshot-tombstone-max-timestamp";
    public const string SnapshotTombstoneMinTimestampHeader = "akka-snapshot-tombstone-min-timestamp";

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
