namespace Shared.Models;

public class OutboxRecord
{
    public Guid Id { get; set; }
    public string AggregateId { get; set; } = default!;
    public string EventType { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public DateTime CreatedAt { get; set; }
}
