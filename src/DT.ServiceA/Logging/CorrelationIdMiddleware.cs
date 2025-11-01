namespace DT.ServiceA.Logging;

/// <summary>
/// Middleware que propaga Correlation ID através do sistema.
/// Permite rastreamento de requests entre serviços.
/// </summary>
public class CorrelationIdMiddleware(RequestDelegate next, ILogger<CorrelationIdMiddleware> logger)
{
    private const string CorrelationIdHeaderName = "X-Correlation-ID";

    public async Task InvokeAsync(HttpContext context)
    {
        // Se não tem correlation ID, gera um novo
        if (!context.Request.Headers.TryGetValue(CorrelationIdHeaderName, out Microsoft.Extensions.Primitives.StringValues value))
        {
            value = Guid.NewGuid().ToString();
            context.Request.Headers[CorrelationIdHeaderName] = value;
        }

        var correlationId = value.ToString();
        context.Items[CorrelationIdHeaderName] = correlationId;
        context.Response.OnStarting(() =>
        {
            context.Response.Headers[CorrelationIdHeaderName] = correlationId;
            return Task.CompletedTask;
        });

        logger.LogInformation(
            "[{CorrelationId}] {Method} {Path} iniciado",
            correlationId,
            context.Request.Method,
            context.Request.Path);

        try
        {
            await next(context);
            logger.LogInformation(
                "[{CorrelationId}] {Method} {Path} completado com status {StatusCode}",
                correlationId,
                context.Request.Method,
                context.Request.Path,
                context.Response.StatusCode);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "[{CorrelationId}] {Method} {Path} falhou com erro",
                correlationId,
                context.Request.Method,
                context.Request.Path);
            throw;
        }
    }
}