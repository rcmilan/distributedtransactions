namespace DT.Shared.Events;

public record OrderCreatedEvent(Guid OrderId, Guid CustomerId, decimal Amount, Guid CorrelationId, DateTime OccurredAt) : IEvent;
