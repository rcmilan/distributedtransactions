namespace DT.ServiceA.Models;

// Represents a record in the transactional outbox table.
// Each row corresponds to one domain event that still needs to be published externally.
public class OutboxMessage
{
    // Stable identifier for this outbox entry; also used as a tracing/idempotency key.
    public Guid Id { get; set; }

    // Fully-qualified CLR type name of the event (e.g. "DT.Shared.Events.OrderCreatedEvent").
    // This allows the publisher to resolve how to route and interpret the payload.
    public string EventType { get; set; } = string.Empty;

    // Serialized (typically JSON) representation of the event.
    // OutboxPublisher does not re-hydrate it; it forwards this payload to the broker.
    public string Payload { get; set; } = string.Empty;

    // Logical occurrence time of the event; used for ordering and debugging.
    public DateTime OccurredAt { get; set; } = DateTime.UtcNow;

    // When null, the message is pending publication.
    // Once set, it indicates the message was (or was attempted to be) published.
    public DateTime? ProcessedAt { get; set; }

    // If publishing fails, the error message is stored here to aid diagnosis.
    public string? ErrorMessage { get; set; }
}