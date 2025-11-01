namespace DT.ServiceA.Models;

/// <summary>
/// Transactional Outbox: registra eventos a serem publicados.
/// A publicação pode falhar, então consumer confiável lê e republica.
/// </summary>
public class OutboxMessage
{
    public Guid Id { get; set; }

    /// <summary>
    /// Type fully qualified: "Shared.Events.OrderCreatedEvent"
    /// </summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary>
    /// JSON serializado do evento
    /// </summary>
    public string Payload { get; set; } = string.Empty;

    public DateTime OccurredAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Null = não processado; data = processado (publicado em RabbitMQ)
    /// </summary>
    public DateTime? ProcessedAt { get; set; }

    /// <summary>
    /// Registra erro se publicação falha (para debugging)
    /// </summary>
    public string? ErrorMessage { get; set; }
}