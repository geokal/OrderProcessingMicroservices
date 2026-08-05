using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shared.Kafka;

namespace NotificationService;

public class NotificationWorker : BackgroundService
{
    private readonly ILogger<NotificationWorker> _logger;
    private readonly string _bootstrapServers;
    private readonly string _groupId;

    // In-memory idempotency check for sent notifications
    private readonly ConcurrentDictionary<string, bool> _notifiedOrders = new();

    public NotificationWorker(ILogger<NotificationWorker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        _groupId = configuration["Kafka:GroupId"] ?? "notification-service-group";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("NotificationWorker starting with BootstrapServers={BootstrapServers}...", _bootstrapServers);

        var topics = new[] { "order.paid" };
        await KafkaTopicInitializer.EnsureTopicsCreatedAsync(_bootstrapServers, topics, _logger);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe("order.paid");

        _logger.LogInformation("Subscribed to Kafka topic 'order.paid'. Waiting for events...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (consumeResult == null || consumeResult.IsPartitionEOF)
                    continue;

                var messageKey = consumeResult.Message.Key ?? string.Empty;
                var messageValue = consumeResult.Message.Value ?? string.Empty;

                _logger.LogInformation("Received 'order.paid' message | Key: '{Key}'", messageKey);

                if (TryParsePaidEvent(messageValue, messageKey, out var orderId, out var amount))
                {
                    // Idempotency Check
                    if (_notifiedOrders.TryGetValue(orderId, out var notified) && notified)
                    {
                        _logger.LogInformation("Notification for OrderId '{OrderId}' already sent. Skipping duplicate.", orderId);
                    }
                    else
                    {
                        // Mock sending Email / SMS
                        _logger.LogInformation("📢 NOTIFICATION SENT: Order '{OrderId}' of amount ${Amount:F2} has been successfully PAID. Email/SMS dispatched to customer.",
                            orderId, amount);

                        _notifiedOrders[orderId] = true;
                    }
                }
                else
                {
                    _logger.LogWarning("Could not parse 'order.paid' payload: {Value}", messageValue);
                }

                consumer.Commit(consumeResult);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing 'order.paid' event. Retrying in 2s...");
                await Task.Delay(2000, stoppingToken);
            }
        }

        consumer.Close();
    }

    private static bool TryParsePaidEvent(string value, string keyFallback, out string orderId, out decimal amount)
    {
        orderId = keyFallback;
        amount = 0;

        try
        {
            using var doc = JsonDocument.Parse(value);
            var root = doc.RootElement;

            if (root.TryGetProperty("OrderId", out var idProp))
                orderId = idProp.GetString()!;
            else if (root.TryGetProperty("id", out var lowerIdProp))
                orderId = lowerIdProp.GetString()!;

            if (root.TryGetProperty("Amount", out var amtProp))
                amount = amtProp.GetDecimal();
            else if (root.TryGetProperty("amount", out var lowerAmtProp))
                amount = lowerAmtProp.GetDecimal();

            return !string.IsNullOrEmpty(orderId);
        }
        catch
        {
            return !string.IsNullOrEmpty(orderId);
        }
    }
}
