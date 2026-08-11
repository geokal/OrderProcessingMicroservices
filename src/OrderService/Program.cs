using Microsoft.AspNetCore.RateLimiting;
using System.Text.Json;
using System.Threading.RateLimiting;
using Dapper;
using Npgsql;
using Shared.Db;
using Shared.Kafka;
using Shared.Models;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("OrdersDb")
    ?? "Host=localhost;Port=5432;Database=ordersdb;Username=orders;Password=orders";
var kafkaBootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

builder.Services.AddSingleton<IDbConnectionFactory>(new DbConnectionFactory(connectionString));
builder.Services.AddHealthChecks();

// Add rate limiting for order submissions
builder.Services.AddRateLimiter(options =>
{
    options.AddFixedWindowLimiter(policyName: "fixed", options =>
    {
        options.PermitLimit = 100;
        options.Window = TimeSpan.FromMinutes(1);
    });
    options.RejectionStatusCode = 429;
});

var app = builder.Build();

// Ensure Database Tables and Kafka Topics exist on startup
using (var scope = app.Services.CreateScope())
{
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    var dbFactory = scope.ServiceProvider.GetRequiredService<IDbConnectionFactory>();

    // Retry loop for DB connection during container startup
    for (int i = 0; i < 10; i++)
    {
        try
        {
            using var db = dbFactory.CreateConnection();
            db.Execute(@"
                CREATE TABLE IF NOT EXISTS orders (
                    id VARCHAR(100) PRIMARY KEY,
                    amount NUMERIC(18,2) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS outbox (
                    id UUID PRIMARY KEY,
                    aggregate_id VARCHAR(100) NOT NULL,
                    event_type VARCHAR(100) NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    key VARCHAR(255) PRIMARY KEY,
                    order_id VARCHAR(100) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                );
            ");
            logger.LogInformation("Database tables initialized successfully.");
            break;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Attempt {Attempt}: Database not ready yet, retrying in 2 seconds...", i + 1);
            await Task.Delay(2000);
        }
    }

    var topics = new[] { "order.created", "payment.retry", "payment.dlt", "order.paid" };
    _ = KafkaTopicInitializer.EnsureTopicsCreatedAsync(kafkaBootstrapServers, topics, logger);
}

app.MapHealthChecks("/health");

app.MapPost("/orders", async (HttpContext context, CreateOrderRequest request, 
    IDbConnectionFactory dbFactory, CancellationToken ct) =>
{
    if (!context.Request.Headers.TryGetValue("Idempotency-Key", out var idempotencyKeyValues) ||
        string.IsNullOrWhiteSpace(idempotencyKeyValues.ToString()))
    {
        return Results.BadRequest(new { error = "Header 'Idempotency-Key' is required." });
    }

    string idempotencyKey = idempotencyKeyValues.ToString().Trim();

    if (string.IsNullOrWhiteSpace(request.OrderId))
    {
        return Results.BadRequest(new { error = "Field 'OrderId' is required." });
    }

    if (request.Amount <= 0)
    {
        return Results.BadRequest(new { error = "Field 'Amount' must be greater than zero." });
    }

    using var connection = dbFactory.CreateConnection();
    connection.Open();

    // 1. Idempotency Check
    var existingOrderId = await connection.QueryFirstOrDefaultAsync<string>(
        "SELECT order_id FROM idempotency_keys WHERE key = @Key;",
        new { Key = idempotencyKey });

    if (!string.IsNullOrEmpty(existingOrderId))
    {
        return Results.Accepted(null, new
        {
            orderId = existingOrderId,
            status = "ACCEPTED",
            message = "Duplicate request processed idempotently"
        });
    }

    // 2. Transactional insert (Order + Outbox + IdempotencyKey)
    
    // Check for client disconnect/graceful shutdown
    if (ct.IsCancellationRequested)
    {
        return Results.StatusCode(499); // Client closed request
    }
    
    using var transaction = connection.BeginTransaction();
    try
    {
        var now = DateTime.UtcNow;

        // Insert Order
        await connection.ExecuteAsync(
            "INSERT INTO orders (id, amount, status, created_at) VALUES (@Id, @Amount, 'PENDING', @CreatedAt);",
            new { Id = request.OrderId, Amount = request.Amount, CreatedAt = now },
            transaction);

        // Insert Outbox record
        var outboxPayload = JsonSerializer.Serialize(new
        {
            Id = request.OrderId,
            Amount = request.Amount,
            Status = "PENDING",
            CreatedAt = now
        });

        await connection.ExecuteAsync(
            "INSERT INTO outbox (id, aggregate_id, event_type, payload, created_at) VALUES (@Id, @AggregateId, 'order.created', @Payload::jsonb, @CreatedAt);",
            new
            {
                Id = Guid.NewGuid(),
                AggregateId = request.OrderId,
                Payload = outboxPayload,
                CreatedAt = now
            },
            transaction);

        // Insert Idempotency Key
        await connection.ExecuteAsync(
            "INSERT INTO idempotency_keys (key, order_id, created_at) VALUES (@Key, @OrderId, @CreatedAt);",
            new { Key = idempotencyKey, OrderId = request.OrderId, CreatedAt = now },
            transaction);

        transaction.Commit();

        return Results.Accepted(null, new
        {
            orderId = request.OrderId,
            status = "PENDING"
        });
    }
    catch (PostgresException ex) when (ex.SqlState == "23505") // Unique constraint violation
    {
        transaction.Rollback();
        // Handle concurrent duplicate idempotency request
        var orderId = await connection.QueryFirstOrDefaultAsync<string>(
            "SELECT order_id FROM idempotency_keys WHERE key = @Key;",
            new { Key = idempotencyKey });

        return Results.Accepted(null, new
        {
            orderId = orderId ?? request.OrderId,
            status = "ACCEPTED",
            message = "Duplicate request processed idempotently"
        });
    }
    catch (Exception)
    {
        transaction.Rollback();
        throw;
    }
})
.RequireRateLimiting("fixed");

app.UseRateLimiter();

app.Run();

public record CreateOrderRequest(string OrderId, decimal Amount);
