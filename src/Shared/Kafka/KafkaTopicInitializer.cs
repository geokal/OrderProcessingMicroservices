using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;

namespace Shared.Kafka;

public static class KafkaTopicInitializer
{
    public static async Task EnsureTopicsCreatedAsync(string bootstrapServers, IEnumerable<string> topics, ILogger logger, int numPartitions = 10, short replicationFactor = 1)
    {
        var config = new AdminClientConfig { BootstrapServers = bootstrapServers };
        using var adminClient = new AdminClientBuilder(config).Build();

        foreach (var topic in topics)
        {
            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                if (metadata.Topics.Any(t => t.Topic == topic && t.Error.Code == ErrorCode.NoError))
                {
                    logger.LogInformation("Kafka topic '{Topic}' already exists.", topic);
                    continue;
                }

                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification
                    {
                        Name = topic,
                        NumPartitions = numPartitions,
                        ReplicationFactor = replicationFactor
                    }
                });
                logger.LogInformation("Successfully created Kafka topic '{Topic}'.", topic);
            }
            catch (CreateTopicsException e) when (e.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                logger.LogInformation("Kafka topic '{Topic}' already exists.", topic);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Could not ensure creation of Kafka topic '{Topic}': {Message}", topic, ex.Message);
            }
        }
    }
}
