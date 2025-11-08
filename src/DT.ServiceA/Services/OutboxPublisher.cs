using DT.ServiceA.Data;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using DT.Shared.Events;
using System.Text;
using System.Collections.Concurrent;

namespace DT.ServiceA.Services;

// Background worker that reads pending Outbox messages and publishes them to RabbitMQ.
// This runs independently of the HTTP request that created the Outbox entries.
public class OutboxPublisher(IServiceProvider serviceProvider, ChannelPool channelPool, ILogger<OutboxPublisher> logger) : BackgroundService
{
    // Controls how many messages we try to send in a single polling cycle.
    private const int DequeueBatchSize = 10;

    // Interval between polling cycles (in milliseconds).
    private const int DequeueIntervalMs = 5000;

    // Cache to avoid repeatedly resolving event type names via reflection.
    private static readonly ConcurrentDictionary<string, Type?> _eventTypeCache = new();

    private static Type? GetOrAddEventType(string eventTypeName)
    {
        // Resolve the event type once and keep it cached; Outbox stores only the type name.
        return _eventTypeCache.GetOrAdd(eventTypeName, key =>
        {
            var type = Type.GetType(key);
            if (type != null) return type;

            // Fallback: search across loaded assemblies for the matching type.
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                type = assembly.GetType(key);
                if (type != null) return type;
            }

            // If we cannot resolve the type, we return null so the caller can log and skip.
            return null;
        });
    }

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

                // Wait before polling again to avoid hammering the database and broker.
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

    // Reads a batch of unprocessed Outbox entries and attempts to publish each one.
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

        // Obtain a RabbitMQ channel from the shared pool to avoid recreating connections.
        var channel = await channelPool.GetChannelAsync(cancellationToken);
        try
        {
            foreach (var message in unprocessedMessages)
            {
                try
                {
                    // Resolve the .NET event type from its stored type name.
                    var eventType = GetOrAddEventType(message.EventType);
                    if (eventType == null)
                    {
                        // If we cannot resolve the type, we log and skip; the row remains pending for investigation.
                        logger.LogError("Unknown event type: {EventType}", message.EventType);
                        continue;
                    }

                    // Use metadata helpers so routing is derived from the event type, not hard-coded.
                    var exchange = EventMetadata.GetExchangeName(eventType);
                    var routingKey = EventMetadata.GetRoutingKey(eventType);

                    // Ensure the exchange exists before publishing (idempotent operation on the broker).
                    await channel.ExchangeDeclareAsync(
                        exchange,
                        ExchangeType.Topic,
                        durable: true,
                        cancellationToken: cancellationToken);

                    var body = Encoding.UTF8.GetBytes(message.Payload);

                    // Mark messages as persistent so RabbitMQ will durably store them.
                    var properties = new BasicProperties
                    {
                        Persistent = true,
                        Headers = new Dictionary<string, object?>
                        {
                            // Include Outbox message id for tracing and idempotency downstream.
                            { "X-Message-Id", message.Id.ToString() }
                        }
                    };

                    await channel.BasicPublishAsync(
                        exchange,
                        routingKey,
                        mandatory: false,
                        basicProperties: properties,
                        body: body,
                        cancellationToken: cancellationToken);

                    logger.LogInformation(
                        "Message {MessageId} published: {EventType}",
                        message.Id,
                        message.EventType);

                    // Mark as successfully processed; this prevents re-publication on next polls.
                    message.ProcessedAt = DateTime.UtcNow;
                    context.OutboxMessages.Update(message);
                }
                catch (Exception ex)
                {
                    // Any failure for one message is recorded on that row; others in the batch can still succeed.
                    logger.LogError(
                        ex,
                        "Error while publishing outbox message {MessageId}",
                        message.Id);

                    message.ErrorMessage = ex.Message;
                    context.OutboxMessages.Update(message);
                }
            }
        }
        finally
        {
            // Always return the channel to the pool so it can be reused.
            channelPool.ReturnChannel(channel);
        }

        // Persist updated ProcessedAt / ErrorMessage flags as part of this polling cycle.
        await context.SaveChangesAsync(cancellationToken);
    }
}