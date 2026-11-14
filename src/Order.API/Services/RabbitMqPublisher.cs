using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using OrderSystem.Shared;

namespace Order.API.Services;

public class RabbitMqPublisher
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<RabbitMqPublisher> _logger;

    public RabbitMqPublisher(IConfiguration configuration, ILogger<RabbitMqPublisher> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public async Task PublishOrderCreatedAsync(OrderCreatedEvent orderEvent)
    {
        var hostName = _configuration["RabbitMQ:HostName"] ?? "localhost";
        var port = int.Parse(_configuration["RabbitMQ:Port"] ?? "5672");
        var userName = _configuration["RabbitMQ:UserName"] ?? "guest";
        var password = _configuration["RabbitMQ:Password"] ?? "guest";

        var factory = new ConnectionFactory
        {
            HostName = hostName,
            Port = port,
            UserName = userName,
            Password = password
        };

        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: RabbitMqConstants.OrderCreatedQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        var jsonMessage = JsonSerializer.Serialize(orderEvent);
        var body = Encoding.UTF8.GetBytes(jsonMessage);

        var props = new BasicProperties
        {
            Persistent = true
        };

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: RabbitMqConstants.OrderCreatedQueue,
            mandatory: true,
            basicProperties: props,
            body: body
        );

        _logger.LogInformation("Published OrderCreatedEvent for OrderId: {OrderId}", orderEvent.OrderId);
    }
}