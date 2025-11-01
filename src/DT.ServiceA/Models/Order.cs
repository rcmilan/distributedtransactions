namespace DT.ServiceA.Models;

public class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public required Guid CustomerId { get; set; }
    public required int AmountInCents { get; set; }
    public OrderStatus Status { get; set; } = OrderStatus.Pending;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; }
    /// <summary>
    /// Rastreamento de falhas
    /// </summary>
    public string? ErrorMessage { get; set; }
}

public enum OrderStatus
{
    Pending,
    PaymentProcessing,
    Confirmed,
    Cancelled
}
