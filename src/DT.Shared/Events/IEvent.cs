namespace DT.Shared.Events;

/// <summary>
/// Interface base para eventos de domínio compartilhados entre serviços.
/// </summary>
public interface IEvent
{
    Guid CorrelationId { get; }
    DateTime OccurredAt { get; }
}
