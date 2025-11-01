using DT.ServiceA.Data;
using DT.ServiceA.IO;
using DT.ServiceA.Models;
using DT.Shared.Events;
using System.Text.Json;

namespace DT.ServiceA.UseCases;

public class CreateOrderUseCase(ILogger<CreateOrderUseCase> logger, OrderDbContext dbContext)
{
    public async Task<Order> Execute(CreateOrderRequest input, Guid correlationId, CancellationToken cancellationToken)
    {
        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerId = input.CustomerId,
            AmountInCents = input.AmountInCents,
            CreatedAt = DateTime.UtcNow,
        };

        logger.LogInformation("[{CorrelationId}] Order.CreatedAt.Kind: {Kind}", correlationId, order.CreatedAt.Kind);

        // Criar evento
        var evt = new OrderCreatedEvent(order.Id, order.CustomerId, order.AmountInCents, correlationId, DateTime.UtcNow);

        logger.LogInformation("[{CorrelationId}] Event.OccurredAt.Kind: {Kind}", correlationId, evt.OccurredAt.Kind);

        // Iniciar transação para garantir atomicidade
        await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
        try
        {
            // 1. Persist order
            dbContext.Orders.Add(order);
            await dbContext.SaveChangesAsync(cancellationToken);

            // 2. Persist event no outbox (mesma transação)
            var outboxMessage = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = typeof(OrderCreatedEvent).AssemblyQualifiedName!,
                Payload = JsonSerializer.Serialize(evt),
                OccurredAt = DateTime.UtcNow,
            };

            logger.LogInformation("[{CorrelationId}] OutboxMessage.OccurredAt.Kind: {Kind}", correlationId, outboxMessage.OccurredAt.Kind);
            dbContext.OutboxMessages.Add(outboxMessage);
            await dbContext.SaveChangesAsync(cancellationToken);

            // 3. Commit de ambos atomicamente
            await transaction.CommitAsync(cancellationToken);

            logger.LogInformation(
                "[{CorrelationId}] Pedido {OrderId} criado com sucesso. Evento no Outbox: {OutboxId}",
                correlationId,
                order.Id,
                outboxMessage.Id);

            return order;
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(cancellationToken);
            logger.LogError(ex, "[{CorrelationId}] Erro ao criar pedido", correlationId);
            throw;
        }
    }
}
