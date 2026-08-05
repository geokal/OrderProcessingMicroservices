using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Dapper;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shared.Db;
using Shared.Kafka;

namespace PaymentService;

public class PaymentWorker : BackgroundService
{
    private readonly ILogger<PaymentWorker> _logger;
    private readonly IDbConnectionFactory _dbFactory;
    private readonly string _bootstrapServers;
    private readonly string _groupId;

    // In-memory idempotency check for processed payments
    private readonly ConcurrentDictionary<string, bool> _processedPayments = new();

    public PaymentWorker(ILogger<PaymentWorker> logger, IDbConnectionFactory dbFactory, Microsoft.Extensions.Configuration.IConfiguration configuration)
    {
        _logger = logger;
        _dbFactory = dbFactory;
        _bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        _groupId = configuration["Kafka:GroupId"] ?? "payment-service-group";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("PaymentWorker starting with BootstrapServers={BootstrapServers}...", _bootstrapServers);

        var topics = new[] { "order.created", "payment.retry", "payment.dlt", "order.paid" };
        await KafkaTopicInitializer.EnsureTopicsCreatedAsync(_bootstrapServers, topics, _logger);

        // Start processing background consumer task for order.created and payment.retry
        var createdTask = Task.Run(() => ConsumeOrdersAsync("order.created", stoppingToken), stoppingToken);
        var retryTask = Task.Run(() => ConsumeOrdersAsync("payment.retry", stoppingToken), stoppingToken);

        await Task.WhenAll(createdTask, retryTask);
    }

    private async Task ConsumeOrdersAsync(string topic, CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"{_groupId}-{topic}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe(topic);
        _logger.LogInformation("Subscribed to Kafka topic '{Topic}'.", topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (consumeResult == null || consumeResult.IsPartitionEOF)
                    continue;

                var messageKey = consumeResult.Message.Key ?? string.Empty;
                var messageValue = consumeResult.Message.Value ?? string.Empty;
                var headers = consumeResult.Message.Headers;

                int retryCount = GetRetryCount(headers);

                _logger.LogInformation("Received message from topic '{Topic}' | Key: '{Key}' | RetryCount: {RetryCount}",
                    topic, messageKey, retryCount);

                // Extract order data
                if (!TryParseOrderMessage(messageValue, messageKey, out var orderId, out var amount))
                {
                    _logger.LogWarning("Failed to parse order message from topic '{Topic}': {Value}", topic, messageValue);
                    consumer.Commit(consumeResult);
                    continue;
                }

                // If coming from payment.retry topic, apply exponential backoff delay before processing
                if (topic == "payment.retry")
                {
                    int backoffMs = (int)Math.Pow(2, retryCount) * 1000;
                    _logger.LogInformation("Applying exponential backoff of {BackoffMs}ms for OrderId '{OrderId}' (Retry #{RetryCount})",
                        backoffMs, orderId, retryCount);
                    await Task.Delay(backoffMs, stoppingToken);
                }

                // Idempotency check: Skip if already processed successfully
                if (_processedPayments.TryGetValue(orderId, out var processed) && processed)
                {
                    _logger.LogInformation("OrderId '{OrderId}' already processed successfully. Skipping.", orderId);
                    consumer.Commit(consumeResult);
                    continue;
                }

                // Execute payment processing logic
                var result = SimulatePaymentGateway(orderId, amount, retryCount);

                if (result == PaymentResult.Success)
                {
                    // Update Order status in DB to PAID
                    using (var db = _dbFactory.CreateConnection())
                    {
                        await db.ExecuteAsync(
                            "UPDATE orders SET status = 'PAID' WHERE id = @Id;",
                            new { Id = orderId });
                    }

                    // Direct Kafka producer for order.paid event
                    // Note: For full atomicity in production, an Outbox table should be used here as well.
                    var paidEventPayload = JsonSerializer.Serialize(new
                    {
                        OrderId = orderId,
                        Amount = amount,
                        Status = "PAID",
                        PaidAt = DateTime.UtcNow
                    });

                    await producer.ProduceAsync("order.paid", new Message<string, string>
                    {
                        Key = orderId,
                        Value = paidEventPayload
                    }, stoppingToken);

                    _processedPayments[orderId] = true;
                    _logger.LogInformation("Payment successful for OrderId '{OrderId}'. Produced 'order.paid' event.", orderId);
                }
                else if (result == PaymentResult.TransientFailure)
                {
                    if (retryCount >= 3)
                    {
                        _logger.LogError("OrderId '{OrderId}' reached max retries ({RetryCount}). Publishing to 'payment.dlt'.", orderId, retryCount);
                        await PublishToDltAsync(producer, orderId, amount, retryCount, "Max retries reached on transient failure", stoppingToken);
                    }
                    else
                    {
                        int nextRetryCount = retryCount + 1;
                        _logger.LogWarning("Transient payment failure for OrderId '{OrderId}'. Publishing to 'payment.retry' (Next Retry: #{NextRetry})",
                            orderId, nextRetryCount);

                        var retryHeaders = new Headers
                        {
                            new Header("retry_count", Encoding.UTF8.GetBytes(nextRetryCount.ToString()))
                        };

                        await producer.ProduceAsync("payment.retry", new Message<string, string>
                        {
                            Key = orderId,
                            Value = messageValue,
                            Headers = retryHeaders
                        }, stoppingToken);
                    }
                }
                else // PermanentFailure
                {
                    _logger.LogError("Permanent payment failure for OrderId '{OrderId}'. Publishing to 'payment.dlt'.", orderId);
                    await PublishToDltAsync(producer, orderId, amount, retryCount, "Permanent payment gateway error", stoppingToken);
                }

                consumer.Commit(consumeResult);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Kafka message on topic '{Topic}'. Retrying in 2s...", topic);
                await Task.Delay(2000, stoppingToken);
            }
        }

        consumer.Close();
    }

    private static int GetRetryCount(Headers? headers)
    {
        if (headers != null)
        {
            var header = headers.FirstOrDefault(h => h.Key == "retry_count");
            if (header != null && header.GetValueBytes() is byte[] headerBytes)
            {
                if (int.TryParse(Encoding.UTF8.GetString(headerBytes), out var count))
                    return count;
            }
        }
        return 0;
    }

    private static bool TryParseOrderMessage(string value, string keyFallback, out string orderId, out decimal amount)
    {
        orderId = keyFallback;
        amount = 0;

        try
        {
            using var doc = JsonDocument.Parse(value);
            var root = doc.RootElement;

            if (root.ValueKind == JsonValueKind.Object)
            {
                // Debezium outbox event or standard JSON payload
                if (root.TryGetProperty("payload", out var payloadProp))
                {
                    if (payloadProp.ValueKind == JsonValueKind.String)
                    {
                        using var innerDoc = JsonDocument.Parse(payloadProp.GetString()!);
                        orderId = innerDoc.RootElement.GetProperty("Id").GetString()!;
                        if (innerDoc.RootElement.TryGetProperty("Amount", out var amt)) amount = amt.GetDecimal();
                        return true;
                    }
                    else if (payloadProp.ValueKind == JsonValueKind.Object)
                    {
                        orderId = payloadProp.GetProperty("Id").GetString()!;
                        if (payloadProp.TryGetProperty("Amount", out var amt)) amount = amt.GetDecimal();
                        return true;
                    }
                }
                else if (root.TryGetProperty("after", out var afterProp) && afterProp.ValueKind == JsonValueKind.Object)
                {
                    if (afterProp.TryGetProperty("payload", out var rawPayload))
                    {
                        var str = rawPayload.GetString();
                        if (!string.IsNullOrEmpty(str))
                        {
                            using var innerDoc = JsonDocument.Parse(str);
                            orderId = innerDoc.RootElement.GetProperty("Id").GetString()!;
                            if (innerDoc.RootElement.TryGetProperty("Amount", out var amt)) amount = amt.GetDecimal();
                            return true;
                        }
                    }
                    if (afterProp.TryGetProperty("aggregate_id", out var aggId))
                    {
                        orderId = aggId.GetString()!;
                        return true;
                    }
                }
                else if (root.TryGetProperty("Id", out var idProp))
                {
                    orderId = idProp.GetString()!;
                    if (root.TryGetProperty("Amount", out var amt)) amount = amt.GetDecimal();
                    return true;
                }
            }
        }
        catch
        {
            // Fallback to key
        }

        return !string.IsNullOrEmpty(orderId);
    }

    private enum PaymentResult { Success, TransientFailure, PermanentFailure }

    private static PaymentResult SimulatePaymentGateway(string orderId, decimal amount, int retryCount)
    {
        // Testing deterministic scenarios:
        if (orderId.StartsWith("fail-dlt", StringComparison.OrdinalIgnoreCase))
            return PaymentResult.PermanentFailure;

        if (orderId.StartsWith("retry-fail", StringComparison.OrdinalIgnoreCase))
            return PaymentResult.TransientFailure; // Always transient failure -> goes to max retries -> DLT

        if (orderId.StartsWith("retry-pass", StringComparison.OrdinalIgnoreCase))
        {
            // Fails twice transiently, succeeds on 3rd attempt (retryCount >= 2)
            return retryCount >= 2 ? PaymentResult.Success : PaymentResult.TransientFailure;
        }

        // Default scenario: Success
        return PaymentResult.Success;
    }

    private static async Task PublishToDltAsync(IProducer<string, string> producer, string orderId, decimal amount, int retryCount, string reason, CancellationToken cancellationToken)
    {
        var dltPayload = JsonSerializer.Serialize(new
        {
            OrderId = orderId,
            Amount = amount,
            Reason = reason,
            RetryCount = retryCount,
            FailedAt = DateTime.UtcNow
        });

        await producer.ProduceAsync("payment.dlt", new Message<string, string>
        {
            Key = orderId,
            Value = dltPayload
        }, cancellationToken);
    }
}
