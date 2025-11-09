using DT.ServiceA.Data;
using Microsoft.EntityFrameworkCore;

namespace DT.ServiceA.Services;

/// <summary>
/// Background worker that:
/// - polls the Outbox store for pending messages
/// - delegates actual transport publishing to an IOutboxMessagePublisher
/// It intentionally does NOT know about any specific broker implementation.
/// </summary>
public class OutboxPublisher(IServiceProvider serviceProvider, IOutboxMessagePublisher messagePublisher, ILogger<OutboxPublisher> logger)
    : BackgroundService
{
    // Controls how many messages we try to send in a single polling cycle.
    private const int DequeueBatchSize = 10;

    // Interval between polling cycles (in milliseconds).
    private const int DequeueIntervalMs = 5000;

    // Entry point for the background loop managed by ASP.NET Core.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("OutboxPublisher started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Try to publish any pending messages for this iteration.
                await PublishUnprocessedMessagesAsync(stoppingToken);

                // Wait before polling again to avoid hammering the database.
                await Task.Delay(DequeueIntervalMs, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Expected path when the application is shutting down.
                logger.LogInformation("OutboxPublisher cancelled");
                break;
            }
            catch (Exception ex)
            {
                // Any unexpected failure is logged; the loop continues (at-least-once semantics).
                logger.LogError(ex, "Error in OutboxPublisher loop");
                await Task.Delay(5000, stoppingToken);
            }
        }
    }

    // Reads a batch of unprocessed Outbox entries and attempts to publish each one via the abstraction.
    private async Task PublishUnprocessedMessagesAsync(CancellationToken cancellationToken)
    {
        using var scope = serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<OrderDbContext>();

        // Select messages that were persisted but not yet marked as processed.
        var unprocessedMessages = await context.OutboxMessages
            .Where(m => m.ProcessedAt == null)
            .OrderBy(m => m.OccurredAt)
            .Take(DequeueBatchSize)
            .ToListAsync(cancellationToken);

        if (unprocessedMessages.Count == 0)
            return;

        foreach (var message in unprocessedMessages)
        {
            try
            {
                await messagePublisher.PublishAsync(message, cancellationToken);

                // Mark as successfully processed; this prevents re-publication on next polls.
                message.ProcessedAt = DateTime.UtcNow;
                context.OutboxMessages.Update(message);

                logger.LogInformation(
                    "Message {MessageId} published by {Publisher}",
                    message.Id,
                    messagePublisher.GetType().Name);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // Any failure for one message is recorded on that row; others in the batch can still succeed.
                logger.LogError(
                    ex,
                    "Error while publishing outbox message {MessageId} via {Publisher}",
                    message.Id,
                    messagePublisher.GetType().Name);

                message.ErrorMessage = ex.Message;
                context.OutboxMessages.Update(message);
            }
        }

        // Persist updated ProcessedAt / ErrorMessage flags as part of this polling cycle.
        await context.SaveChangesAsync(cancellationToken);
    }
}