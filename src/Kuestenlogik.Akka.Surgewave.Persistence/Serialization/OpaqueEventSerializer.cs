namespace Kuestenlogik.Akka.Surgewave.Persistence.Serialization;

using System.Text;
using global::Akka.Actor;
using global::Akka.Persistence.Journal;
using global::Akka.Serialization;

/// <summary>
/// Opaque mode: passes through Akka's built-in serializer.
/// Events are stored as byte arrays with serializer metadata in headers.
/// Sets content-type header so Surgewave can auto-detect the format.
/// </summary>
public sealed class OpaqueEventSerializer : ISurgewaveEventSerializer
{
    private static readonly byte[] HyperionContentType = Encoding.UTF8.GetBytes("application/x-hyperion");

    private readonly Serialization _serialization;

    public OpaqueEventSerializer(ActorSystem system)
    {
        _serialization = new Serialization((ExtendedActorSystem)system);
    }

    public byte[] Serialize(IPersistentRepresentation persistent, out Dictionary<string, byte[]> headers)
    {
        var payload = persistent.Payload;

        if (payload is Tagged tagged)
            payload = tagged.Payload;

        var serializer = _serialization.FindSerializerFor(payload);
        var manifest = serializer switch
        {
            SerializerWithStringManifest s => s.Manifest(payload),
            { IncludeManifest: true } => payload.GetType().AssemblyQualifiedName ?? payload.GetType().FullName ?? "",
            _ => string.Empty
        };

        headers = new Dictionary<string, byte[]>
        {
            ["content-type"] = HyperionContentType,
            [EventEnvelopeCodec.SerializerIdHeader] = EventEnvelopeCodec.EncodeInt(serializer.Identifier),
            [EventEnvelopeCodec.ManifestHeader] = EventEnvelopeCodec.EncodeString(manifest)
        };

        return serializer.ToBinary(payload);
    }

    public IPersistentRepresentation Deserialize(
        string persistenceId,
        long sequenceNr,
        byte[] data,
        IReadOnlyDictionary<string, byte[]> headers)
    {
        var serializerId = EventEnvelopeCodec.DecodeInt(headers[EventEnvelopeCodec.SerializerIdHeader]);
        var manifest = headers.TryGetValue(EventEnvelopeCodec.ManifestHeader, out var manifestBytes)
            ? EventEnvelopeCodec.DecodeString(manifestBytes)
            : string.Empty;

        var payload = _serialization.Deserialize(data, serializerId, manifest);

        return new Persistent(payload, sequenceNr, persistenceId);
    }

    public byte[] SerializeSnapshot(object snapshot, out Dictionary<string, byte[]> headers)
    {
        var serializer = _serialization.FindSerializerFor(snapshot);
        var manifest = serializer switch
        {
            SerializerWithStringManifest s => s.Manifest(snapshot),
            { IncludeManifest: true } => snapshot.GetType().AssemblyQualifiedName ?? snapshot.GetType().FullName ?? "",
            _ => string.Empty
        };

        headers = new Dictionary<string, byte[]>
        {
            ["content-type"] = HyperionContentType,
            [EventEnvelopeCodec.SerializerIdHeader] = EventEnvelopeCodec.EncodeInt(serializer.Identifier),
            [EventEnvelopeCodec.ManifestHeader] = EventEnvelopeCodec.EncodeString(manifest)
        };

        return serializer.ToBinary(snapshot);
    }

    public object DeserializeSnapshot(byte[] data, IReadOnlyDictionary<string, byte[]> headers)
    {
        var serializerId = EventEnvelopeCodec.DecodeInt(headers[EventEnvelopeCodec.SerializerIdHeader]);
        var manifest = headers.TryGetValue(EventEnvelopeCodec.ManifestHeader, out var manifestBytes)
            ? EventEnvelopeCodec.DecodeString(manifestBytes)
            : string.Empty;

        return _serialization.Deserialize(data, serializerId, manifest);
    }
}
