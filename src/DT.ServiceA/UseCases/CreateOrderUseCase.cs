using DT.ServiceA.Data;
using DT.ServiceA.IO;
using DT.ServiceA.Models;
using DT.Shared.Events;
using System.Text.Json;

namespace DT.ServiceA.UseCases;

public class CreateOrderUseCase(ILogger<CreateOrderUseCase> logger, OrderDbContext dbContext)
{
    // Orchestrates the creation of an Order and its corresponding domain event
    // in a single database transaction (Transactional Outbox pattern).
    public async Task<Order> Execute(CreateOrderRequest input, Guid correlationId, CancellationToken cancellationToken)
    {
        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerId = input.CustomerId,
            AmountInCents = input.AmountInCents,
            CreatedAt = DateTime.UtcNow,
        };

        // Build the domain event that will later be published to the message broker.
        var evt = new OrderCreatedEvent(order.Id, order.CustomerId, order.AmountInCents, correlationId, DateTime.UtcNow);

        logger.LogDebug("[{CorrelationId}] Event.OccurredAt.Kind: {Kind}", correlationId, evt.OccurredAt.Kind);

        // Begin a database transaction so that order + outbox message are committed atomically.
        await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
        try
        {
            // Step 1: Persist the new order.
            dbContext.Orders.Add(order);
            await dbContext.SaveChangesAsync(cancellationToken);

            // Step 2: Persist the event as an outbox message in the same transaction.
            // This guarantees that if the order exists, the corresponding event record also exists.
            var outboxMessage = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = typeof(OrderCreatedEvent).AssemblyQualifiedName!,
                Payload = JsonSerializer.Serialize(evt),
                OccurredAt = DateTime.UtcNow,
            };

            logger.LogDebug("[{CorrelationId}] OutboxMessage.OccurredAt.Kind: {Kind}", correlationId, outboxMessage.OccurredAt.Kind);

            dbContext.OutboxMessages.Add(outboxMessage);
            await dbContext.SaveChangesAsync(cancellationToken);

            // Step 3: Commit once so both the order and outbox message are durably stored together.
            await transaction.CommitAsync(cancellationToken);

            logger.LogDebug(
                "[{CorrelationId}] Order {OrderId} created. Outbox message: {OutboxId}",
                correlationId,
                order.Id,
                outboxMessage.Id);

            return order;
        }
        catch (Exception ex)
        {
            // Roll back the transaction so we do not end up with a partial write (order without outbox entry or vice versa).
            await transaction.RollbackAsync(cancellationToken);
            logger.LogError(ex, "[{CorrelationId}] Error while creating order", correlationId);
            throw;
        }
    }
}
