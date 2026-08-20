namespace Kuestenlogik.Akka.Surgewave.Persistence.Serialization;

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Akka.Surgewave.Persistence.Journal;
using Google.Protobuf;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Schema.Registry.Client;

/// <summary>
/// Schema-aware serializer supporting both JSON and Protobuf modes.
/// Events are stored in Confluent wire format [0x00][4-byte Schema-ID][payload].
/// In Json mode, payload is UTF-8 JSON. In Proto mode, payload is Protobuf binary.
/// </summary>
public sealed class SchemaRegistryEventSerializer : ISurgewaveEventSerializer
{
    private readonly ISchemaRegistryOperations _registry;
    private readonly ISubjectNameStrategy _subjectStrategy;
    private readonly bool _autoRegister;
    private readonly string _journalTopic;
    private readonly SerializationMode _mode;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly byte[] _contentTypeHeader;

    private readonly ConcurrentDictionary<Type, int> _schemaIdCache = new();

    public SchemaRegistryEventSerializer(SurgewaveJournalSettings settings)
    {
        _registry = settings.SchemaRegistryOperations
            ?? throw new InvalidOperationException(
                "SchemaRegistry operations not configured.");
        _autoRegister = settings.SchemaRegistryAutoRegister;
        _subjectStrategy = SubjectNameStrategies.Get(settings.SchemaRegistrySubjectStrategy);
        _journalTopic = settings.JournalTopic;
        _mode = settings.SerializationMode;
        _contentTypeHeader = _mode == SerializationMode.Proto
            ? "application/x-protobuf"u8.ToArray()
            : "application/json"u8.ToArray();
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
    }

    public byte[] Serialize(IPersistentRepresentation persistent, out Dictionary<string, byte[]> headers)
    {
        var payload = persistent.Payload;
        var payloadType = payload.GetType();

        headers = new Dictionary<string, byte[]>
        {
            ["content-type"] = _contentTypeHeader,
            [EventEnvelopeCodec.ManifestHeader] = EventEnvelopeCodec.EncodeString(
                payloadType.FullName ?? payloadType.Name)
        };

        var schemaId = GetOrRegisterSchema(payloadType);
        var payloadBytes = SerializePayload(payload, payloadType);

        // Confluent wire format: [magic byte][4-byte schema ID][payload]
        var result = new byte[SchemaRegistryWireFormat.HeaderSize + payloadBytes.Length];
        SchemaRegistryWireFormat.WriteHeader(result, schemaId);
        payloadBytes.CopyTo(result.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        return result;
    }

    public IPersistentRepresentation Deserialize(
        string persistenceId,
        long sequenceNr,
        byte[] data,
        IReadOnlyDictionary<string, byte[]> headers)
    {
        Type? targetType = null;
        if (headers.TryGetValue(EventEnvelopeCodec.ManifestHeader, out var manifestBytes))
        {
            var manifest = EventEnvelopeCodec.DecodeString(manifestBytes);
            targetType = ResolveType(manifest);
        }

        var payloadSpan = SchemaRegistryWireFormat.GetPayload(data);
        var result = DeserializePayload(payloadSpan, targetType, headers);

        return new Persistent(result, sequenceNr, persistenceId);
    }

    public byte[] SerializeSnapshot(object snapshot, out Dictionary<string, byte[]> headers)
    {
        var snapshotType = snapshot.GetType();

        headers = new Dictionary<string, byte[]>
        {
            ["content-type"] = _contentTypeHeader,
            [EventEnvelopeCodec.ManifestHeader] = EventEnvelopeCodec.EncodeString(
                snapshotType.FullName ?? snapshotType.Name)
        };

        var schemaId = GetOrRegisterSchema(snapshotType);
        var payloadBytes = SerializePayload(snapshot, snapshotType);

        var result = new byte[SchemaRegistryWireFormat.HeaderSize + payloadBytes.Length];
        SchemaRegistryWireFormat.WriteHeader(result, schemaId);
        payloadBytes.CopyTo(result.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        return result;
    }

    public object DeserializeSnapshot(byte[] data, IReadOnlyDictionary<string, byte[]> headers)
    {
        Type? targetType = null;
        if (headers.TryGetValue(EventEnvelopeCodec.ManifestHeader, out var manifestBytes))
        {
            var manifest = EventEnvelopeCodec.DecodeString(manifestBytes);
            targetType = ResolveType(manifest);
        }

        var payloadSpan = SchemaRegistryWireFormat.GetPayload(data);
        return DeserializePayload(payloadSpan, targetType, headers);
    }

    private byte[] SerializePayload(object payload, Type payloadType)
    {
        if (_mode == SerializationMode.Proto && payload is IMessage protoMessage)
            return protoMessage.ToByteArray();

        return JsonSerializer.SerializeToUtf8Bytes(payload, payloadType, _jsonOptions);
    }

    private object DeserializePayload(
        ReadOnlySpan<byte> payloadSpan, Type? targetType,
        IReadOnlyDictionary<string, byte[]> headers)
    {
        // Detect format from content-type header
        var isProto = false;
        if (headers.TryGetValue("content-type", out var ctBytes))
        {
            var ct = Encoding.UTF8.GetString(ctBytes);
            isProto = ct.Contains("protobuf", StringComparison.OrdinalIgnoreCase);
        }

        if (isProto && targetType is not null)
        {
            // Try Protobuf deserialization via reflection on the Parser property
            var parserProp = targetType.GetProperty("Parser",
                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (parserProp?.GetValue(null) is MessageParser parser)
                return parser.ParseFrom(payloadSpan);
        }

        // JSON deserialization
        var json = Encoding.UTF8.GetString(payloadSpan);
        if (targetType is not null)
        {
            return JsonSerializer.Deserialize(json, targetType, _jsonOptions)
                ?? throw new InvalidOperationException($"Deserialization returned null for {targetType}");
        }

        return JsonSerializer.Deserialize<object>(json, _jsonOptions)
            ?? throw new InvalidOperationException("Deserialization returned null");
    }

    private int GetOrRegisterSchema(Type type)
    {
        return _schemaIdCache.GetOrAdd(type, t =>
        {
            var recordName = t.FullName;
            var subject = _subjectStrategy.GetSubjectName(_journalTopic, isKey: false, recordName);
            var schemaType = (_mode == SerializationMode.Proto && typeof(IMessage).IsAssignableFrom(t))
                ? "PROTOBUF"
                : "JSON";

            if (_autoRegister)
            {
                var schemaString = schemaType == "PROTOBUF"
                    ? GenerateProtobufSchemaRef(t)
                    : GenerateJsonSchema(t);
                var result = _registry
                    .RegisterSchemaAsync(subject, schemaString, schemaType)
                    .GetAwaiter().GetResult();
                return result.SchemaId;
            }

            var versions = _registry.GetSubjectVersionsAsync(subject).GetAwaiter().GetResult();
            if (versions.Count == 0)
                throw new InvalidOperationException($"No schema registered for '{subject}'");
            var info = _registry.GetSchemaByVersionAsync(subject, versions.Max()).GetAwaiter().GetResult()
                ?? throw new InvalidOperationException($"Schema not found for '{subject}'");
            return info.Id;
        });
    }

    private static string GenerateProtobufSchemaRef(Type type)
    {
        // For Protobuf types, use the descriptor's proto file content
        var descriptorProp = type.GetProperty("Descriptor",
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        if (descriptorProp?.GetValue(null) is Google.Protobuf.Reflection.MessageDescriptor descriptor)
            return descriptor.File.SerializedData.ToBase64();

        return $"message {type.Name} {{}}";
    }

    private static string GenerateJsonSchema(Type type)
    {
        var properties = new Dictionary<string, object>();
        var required = new List<string>();

        foreach (var prop in type.GetProperties())
        {
            var jsonType = prop.PropertyType switch
            {
                var t when t == typeof(string) => "string",
                var t when t == typeof(int) || t == typeof(long) || t == typeof(short) => "integer",
                var t when t == typeof(double) || t == typeof(float) || t == typeof(decimal) => "number",
                var t when t == typeof(bool) => "boolean",
                var t when t == typeof(DateTimeOffset) || t == typeof(DateTime) => "string",
                var t when t.IsEnum => "string",
                _ => "object"
            };
            properties[JsonNamingPolicy.CamelCase.ConvertName(prop.Name)] = new { type = jsonType };
            required.Add(JsonNamingPolicy.CamelCase.ConvertName(prop.Name));
        }

        return JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["$schema"] = "http://json-schema.org/draft-07/schema#",
            ["title"] = type.Name,
            ["type"] = "object",
            ["properties"] = properties,
            ["required"] = required
        });
    }

    private static Type? ResolveType(string manifest)
    {
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            var type = assembly.GetType(manifest);
            if (type is not null)
                return type;
        }
        return null;
    }
}
