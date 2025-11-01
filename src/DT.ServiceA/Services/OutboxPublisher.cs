using DT.ServiceA.Data;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using DT.Shared.Events;
using System.Text;

namespace DT.ServiceA.Services;

/// <summary>
/// Background service que consome Outbox e publica em RabbitMQ.
/// Implementa At-Least-Once semantics: mesmo que falhe, retentará.
/// </summary>
public class OutboxPublisher(IServiceProvider serviceProvider, IConnection rabbitMqConnection, ILogger<OutboxPublisher> logger) : BackgroundService
{
    private const int DequeueBatchSize = 10;
    private const int DequeueIntervalMs = 5000; // 5 segundos

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("OutboxPublisher iniciado");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PublishUnprocessedMessagesAsync(stoppingToken);
                await Task.Delay(DequeueIntervalMs, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("OutboxPublisher cancelado");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Erro no OutboxPublisher");
                await Task.Delay(5000, stoppingToken); // Backoff antes de retry
            }
        }
    }

    private async Task PublishUnprocessedMessagesAsync(CancellationToken cancellationToken)
    {
        using var scope = serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<OrderDbContext>();

        // Buscar mensagens não processadas
        var unprocessedMessages = await context.OutboxMessages
            .Where(m => m.ProcessedAt == null)
            .OrderBy(m => m.OccurredAt)
            .Take(DequeueBatchSize)
            .ToListAsync(cancellationToken);

        if (unprocessedMessages.Count == 0)
            return;

        await using var channel = await rabbitMqConnection.CreateChannelAsync(cancellationToken: cancellationToken);

        foreach (var message in unprocessedMessages)
        {
            try
            {
                // Resolve the event type from the EventType string
                var eventType = Type.GetType(message.EventType);
                if (eventType == null)
                {
                    // Fallback: search through loaded assemblies using FullName
                    foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                    {
                        eventType = assembly.GetType(message.EventType);
                        if (eventType != null)
                            break;
                    }
                    if (eventType == null)
                    {
                        logger.LogError("Unknown event type: {EventType}", message.EventType);
                        continue;
                    }
                }

                // Get exchange and routing key using EventMetadata
                var exchange = EventMetadata.GetExchangeName(eventType);
                var routingKey = EventMetadata.GetRoutingKey(eventType);

                // Declare the exchange dynamically
                await channel.ExchangeDeclareAsync(exchange, ExchangeType.Topic, durable: true, cancellationToken: cancellationToken);

                var body = Encoding.UTF8.GetBytes(message.Payload);
                var properties = new BasicProperties
                {
                    Persistent = true,
                    Headers = new Dictionary<string, object?>
                    {
                        { "X-Message-Id", message.Id.ToString() }
                    }
                };

                await channel.BasicPublishAsync(
                    exchange,
                    routingKey,
                    false, // mandatory
                    properties,
                    body,
                    cancellationToken: cancellationToken);

                logger.LogInformation(
                    "Mensagem {MessageId} publicada: {EventType}",
                    message.Id,
                    message.EventType);

                // Marcar como processado
                message.ProcessedAt = DateTime.UtcNow;
                context.OutboxMessages.Update(message);
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Erro ao publicar mensagem {MessageId}",
                    message.Id);
                message.ErrorMessage = ex.Message;
                context.OutboxMessages.Update(message);
            }
        }

        await context.SaveChangesAsync(cancellationToken);
    }
}