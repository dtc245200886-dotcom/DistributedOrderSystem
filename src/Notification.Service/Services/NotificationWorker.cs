using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Notification.Service.Hubs;
using OrderSystem.Shared;

namespace Notification.Service.Services;

public class NotificationWorker : BackgroundService
{
    private readonly ILogger<NotificationWorker> _logger;
    private readonly IHubContext<OrderNotificationHub> _hubContext;
    private readonly IConfiguration _configuration;

    public NotificationWorker(ILogger<NotificationWorker> logger, IHubContext<OrderNotificationHub> hubContext, IConfiguration configuration)
    {
        _logger = logger;
        _hubContext = hubContext;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
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

        IConnection? connection = null;
        IChannel? channel = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                connection = await factory.CreateConnectionAsync(stoppingToken);
                channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

                await channel.QueueDeclareAsync(
                    queue: RabbitMqConstants.OrderStatusChangedQueue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: stoppingToken
                );

                _logger.LogInformation("NotificationWorker connected to RabbitMQ successfully.");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "NotificationWorker connecting to RabbitMQ failed. Retrying in 5s...");
                await Task.Delay(5000, stoppingToken);
            }
        }

        if (channel == null || connection == null) return;

        var consumer = new AsyncEventingBasicConsumer(channel);
        
        consumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var json = Encoding.UTF8.GetString(body);
                var statusEvent = JsonSerializer.Deserialize<OrderStatusChangedEvent>(json);

                if (statusEvent != null)
                {
                    _logger.LogInformation("Received StatusChanged for Order: {Id} -> {Status}", statusEvent.OrderId, statusEvent.NewStatus);

                    await _hubContext.Clients.All.SendAsync("ReceiveOrderNotification", statusEvent, cancellationToken: stoppingToken);
                    await _hubContext.Clients.Group(statusEvent.OrderId.ToString()).SendAsync("ReceiveOrderNotification", statusEvent, cancellationToken: stoppingToken);
                }

                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing status notification message.");
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
            }
        };

        await channel.BasicConsumeAsync(
            queue: RabbitMqConstants.OrderStatusChangedQueue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }
}