using DT.ServiceA.Data;
using DT.ServiceA.Logging;
using DT.ServiceA.Services;
using DT.ServiceA.UseCases;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using Serilog;

namespace DT.ServiceA;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Serilog: logging estruturado
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.Console(
                outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz}] [{Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
            .CreateLogger();

        builder.Host.UseSerilog();

        // Add services to the container.

        builder.Services.AddControllers();
        // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
        builder.Services.AddOpenApi();


        // PostgreSQL + EF Core
        var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
            ?? "Host=localhost;Port=5432;Database=orders_db;Username=postgres;Password=postgres";

        builder.Services.AddDbContext<OrderDbContext>(options =>
            options.UseNpgsql(connectionString));

        // RabbitMQ
        var rabbitMqHost = builder.Configuration.GetValue<string>("RabbitMq:Host") ?? "localhost";
        var rabbitMqPort = builder.Configuration.GetValue<int?>("RabbitMq:Port") ?? 5672;

        var factory = new ConnectionFactory()
        {
            HostName = rabbitMqHost,
            Port = rabbitMqPort,
            UserName = "guest",
            Password = "guest",
            AutomaticRecoveryEnabled = true
        };

        builder.Services.AddSingleton(await factory.CreateConnectionAsync());

        // Servi�os
        builder.Services.AddScoped<CreateOrderUseCase>();
        builder.Services.AddHostedService<OutboxPublisher>();
        builder.Services.AddSwaggerGen();

        var app = builder.Build();

        // Middleware customizado para Correlation ID
        app.UseMiddleware<CorrelationIdMiddleware>();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment())
        {
            app.MapOpenApi();
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        app.UseHttpsRedirection();

        app.UseAuthorization();

        app.MapControllers();
        
        // Executar migrations na inicializa��o
        using (var scope = app.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
            await db.Database.MigrateAsync();
            Log.Information("Migrations executadas para ServiceA");
        }

        app.Run();
    }
}
