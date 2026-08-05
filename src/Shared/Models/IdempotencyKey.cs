namespace Shared.Models;

public class IdempotencyKeyRecord
{
    public string Key { get; set; } = default!;
    public string OrderId { get; set; } = default!;
    public DateTime CreatedAt { get; set; }
}
