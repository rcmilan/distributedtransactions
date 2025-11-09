using DT.ServiceA.Models;
using DT.Shared.Events;
using RabbitMQ.Client;
using System.Collections.Concurrent;
using System.Text;

namespace DT.ServiceA.Services;

/// <summary>
/// RabbitMQ-based implementation of <see cref="IOutboxMessagePublisher"/>.
/// Encapsulates all broker-specific logic so the OutboxPublisher remains transport-agnostic.
/// </summary>
public class RabbitMqOutboxMessagePublisher(ChannelPool channelPool, ILogger<RabbitMqOutboxMessagePublisher> logger) : IOutboxMessagePublisher
{
    // Cache to avoid repeatedly resolving event type names via reflection.
    private static readonly ConcurrentDictionary<string, Type?> EventTypeCache = new();

    public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        // Resolve the .NET event type from its stored type name.
        var eventType = GetOrAddEventType(message.EventType);
        if (eventType is null)
        {
            // If we cannot resolve the type, this is considered a configuration/contract issue.
            // Let the caller record the error; do not silently mark as processed.
            logger.LogError("Unknown event type: {EventType}", message.EventType);
            throw new InvalidOperationException($"Unknown event type: {message.EventType}");
        }

        var exchange = EventMetadata.GetExchangeName(eventType);
        var routingKey = EventMetadata.GetRoutingKey(eventType);

        var channel = await channelPool.GetChannelAsync(cancellationToken);
        try
        {
            // Ensure the exchange exists before publishing (idempotent on the broker).
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
                    // Include Outbox message id for tracing and downstream idempotency.
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
                "Outbox message {MessageId} published to RabbitMQ: {EventType} - {Exchange} / {RoutingKey}",
                message.Id,
                message.EventType,
                exchange,
                routingKey);
        }
        finally
        {
            channelPool.ReturnChannel(channel);
        }
    }

    private static Type? GetOrAddEventType(string eventTypeName)
    {
        return EventTypeCache.GetOrAdd(eventTypeName, key =>
        {
            var type = Type.GetType(key);
            if (type != null) return type;

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                type = assembly.GetType(key);
                if (type != null) return type;
            }

            return null;
        });
    }
}