using DT.ServiceA.IO;
using DT.ServiceA.UseCases;
using Microsoft.AspNetCore.Mvc;

namespace DT.ServiceA.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController(ILogger<OrdersController> logger) : ControllerBase
{
    private const string CorrelationIdHeaderName = "X-Correlation-ID";

    [HttpPost("create")]
    public async Task<IActionResult> CreateOrder([FromServices] CreateOrderUseCase useCase, [FromBody] CreateOrderRequest request, CancellationToken cancellationToken)
    {
        // Obter correlation ID do middleware
        var correlationId = HttpContext.Items.ContainsKey(CorrelationIdHeaderName)
            ? Guid.Parse(HttpContext.Items[CorrelationIdHeaderName]!.ToString()!)
            : Guid.NewGuid();

        try
        {
            var order = await useCase.Execute(request, correlationId, cancellationToken);

            return Accepted(new { orderId = order.Id, correlationId });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{CorrelationId}] Erro ao criar pedido", correlationId);
            return StatusCode(500, new { error = ex.Message });
        }
    }
}
