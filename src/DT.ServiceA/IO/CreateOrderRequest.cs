namespace DT.ServiceA.IO;

public record CreateOrderRequest(Guid CustomerId, int AmountInCents);
