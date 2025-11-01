namespace DT.Shared.Events;

public record PaymentProcessedEvent(Guid OrderId, Guid PaymentId, bool Success, string? ErrorMessage, Guid CorrelationId, DateTime OccurredAt) : IEvent;
