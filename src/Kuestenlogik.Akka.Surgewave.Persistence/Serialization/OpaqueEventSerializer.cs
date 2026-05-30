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

        return global::Akka.Serialization.Serialization.WithTransport(
            _serialization.System,
            (serializer, payload),
            static state => state.serializer.ToBinary(state.payload));
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

        var payload = global::Akka.Serialization.Serialization.WithTransport(
            _serialization.System,
            (serialization: _serialization, data, serializerId, manifest),
            static state => state.serialization.Deserialize(state.data, state.serializerId, state.manifest));

        // Reconstruct the full envelope from the headers we wrote in
        // SurgewaveJournal.WriteMessagesAsync (writer-uuid + timestamp);
        // otherwise the replayed Persistent has empty metadata and the TCK
        // round-trip assertions fail (e.g. WriterGuid mismatch).
        var writerGuid = headers.TryGetValue(EventEnvelopeCodec.WriterUuidHeader, out var wgBytes)
            ? EventEnvelopeCodec.DecodeString(wgBytes)
            : string.Empty;
        var timestamp = headers.TryGetValue(EventEnvelopeCodec.TimestampHeader, out var tsBytes)
            ? EventEnvelopeCodec.DecodeLong(tsBytes)
            : 0L;

        return new Persistent(
            payload,
            sequenceNr,
            persistenceId,
            manifest: string.Empty,
            isDeleted: false,
            sender: global::Akka.Actor.ActorRefs.NoSender,
            writerGuid: writerGuid,
            timestamp: timestamp);
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

        // WithTransport wires CurrentTransportInformation onto the thread so
        // serializers that look up the actor system (e.g. ActorRef refs, the
        // TCK's TestSerializer) can resolve transport addresses. Without it
        // ToBinary throws InvalidOperationException.
        return global::Akka.Serialization.Serialization.WithTransport(
            _serialization.System,
            (serializer, snapshot),
            static state => state.serializer.ToBinary(state.snapshot));
    }

    public object DeserializeSnapshot(byte[] data, IReadOnlyDictionary<string, byte[]> headers)
    {
        var serializerId = EventEnvelopeCodec.DecodeInt(headers[EventEnvelopeCodec.SerializerIdHeader]);
        var manifest = headers.TryGetValue(EventEnvelopeCodec.ManifestHeader, out var manifestBytes)
            ? EventEnvelopeCodec.DecodeString(manifestBytes)
            : string.Empty;

        return global::Akka.Serialization.Serialization.WithTransport(
            _serialization.System,
            (serialization: _serialization, data, serializerId, manifest),
            static state => state.serialization.Deserialize(state.data, state.serializerId, state.manifest));
    }
}
