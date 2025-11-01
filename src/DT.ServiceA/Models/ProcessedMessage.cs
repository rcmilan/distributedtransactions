namespace DT.ServiceA.Models;

/// <summary>
/// Idempotência: registra qual message ID já foi processado.
/// Garante que consumidores duplicados não tenham efeito duplo.
/// </summary>
public class ProcessedMessage
{
    public Guid Id { get; set; }
    public Guid MessageId { get; set; }
    public DateTime ProcessedAt { get; set; } = DateTime.UtcNow;
}