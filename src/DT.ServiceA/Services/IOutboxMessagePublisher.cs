using DT.ServiceA.Models;

namespace DT.ServiceA.Services;

/// <summary>
/// Abstraction for publishing Outbox messages to an external transport (e.g., message broker).
/// This keeps Outbox processing independent from any specific transport implementation.
/// </summary>
public interface IOutboxMessagePublisher
{
    /// <summary>
    /// Publishes the given outbox message to the external system.
    /// Implementations must:
    /// - be idempotent / safe for at-least-once invocation semantics.
    /// - throw on failure so caller can record the error and decide about retries.
    /// </summary>
    /// <param name="message">The outbox message to publish.</param>
    /// <param name="cancellationToken">Token to observe cancellation.</param>
    Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken);
}