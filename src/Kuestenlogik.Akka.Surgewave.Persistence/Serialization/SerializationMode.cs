namespace Kuestenlogik.Akka.Surgewave.Persistence.Serialization;

/// <summary>
/// Determines how events are serialized before writing to Surgewave.
/// </summary>
public enum SerializationMode
{
    /// <summary>
    /// Akka Hyperion binary serialization.
    /// Events are stored as opaque byte arrays — backward compatible with existing projects.
    /// Surgewave can still deserialize via its Hyperion content-type handler.
    /// </summary>
    Hyperion,

    /// <summary>
    /// JSON with Schema Registry.
    /// Events are human-readable, easy to debug. Good for development.
    /// </summary>
    Json,

    /// <summary>
    /// Protocol Buffers with Schema Registry.
    /// Compact, fast, with strong schema evolution guarantees. Recommended for production.
    /// </summary>
    Proto
}
