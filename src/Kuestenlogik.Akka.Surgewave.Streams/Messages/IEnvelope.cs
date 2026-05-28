namespace Kuestenlogik.Akka.Surgewave.Streams.Messages;

/// <summary>
/// PassThrough pattern for FlexiFlow — allows data to be carried through
/// the produce flow without modification.
/// </summary>
public interface IEnvelope<out TKey, out TValue, out TPassThrough>
{
    TPassThrough PassThrough { get; }
}
