namespace Order.Persistence.Models;

public class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string EventType { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? PublishedAt { get; set; }
}

public class ProcessedMessage
{
    public Guid Id { get; set; }
    public DateTime ProcessedAt { get; set; } = DateTime.UtcNow;
}
