namespace Kuestenlogik.Surgewave.AkkaPersistence.Serialization;


/// <summary>
/// Abstraction over the two serialization modes.
/// Opaque mode passes through Akka's built-in serializer.
/// Schema mode uses Surgewave's Schema Registry.
/// </summary>
public interface ISurgewaveEventSerializer
{
    byte[] Serialize(IPersistentRepresentation persistent, out Dictionary<string, byte[]> headers);

    IPersistentRepresentation Deserialize(
        string persistenceId,
        long sequenceNr,
        byte[] data,
        IReadOnlyDictionary<string, byte[]> headers);

    byte[] SerializeSnapshot(object snapshot, out Dictionary<string, byte[]> headers);

    object DeserializeSnapshot(byte[] data, IReadOnlyDictionary<string, byte[]> headers);
}
