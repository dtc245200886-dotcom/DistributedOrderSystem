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
    private readonly NotificationWorkerStatus _workerStatus;

    public NotificationWorker(
        ILogger<NotificationWorker> logger,
        IHubContext<OrderNotificationHub> hubContext,
        IConfiguration configuration,
        NotificationWorkerStatus workerStatus)
    {
        _logger = logger;
        _hubContext = hubContext;
        _configuration = configuration;
        _workerStatus = workerStatus;
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
            Password = password,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
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
                if (channel is not null)
                {
                    await channel.DisposeAsync();
                    channel = null;
                }

                if (connection is not null)
                {
                    await connection.DisposeAsync();
                    connection = null;
                }

                _logger.LogWarning(ex, "NotificationWorker connecting to RabbitMQ failed. Retrying in 5s...");
                await Task.Delay(5000, stoppingToken);
            }
        }

        if (channel == null || connection == null) return;

        try
        {
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var json = Encoding.UTF8.GetString(body);
                    var statusEvent = JsonSerializer.Deserialize<OrderStatusChangedEvent>(json);

                    if (statusEvent is null ||
                        statusEvent.OrderId == Guid.Empty ||
                        string.IsNullOrWhiteSpace(statusEvent.CustomerName) ||
                        string.IsNullOrWhiteSpace(statusEvent.OldStatus) ||
                        string.IsNullOrWhiteSpace(statusEvent.NewStatus) ||
                        string.IsNullOrWhiteSpace(statusEvent.Message))
                    {
                        _logger.LogWarning("Received an invalid OrderStatusChangedEvent message; discarding it.");
                        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                        return;
                    }

                    _logger.LogInformation("Received StatusChanged for Order: {Id} -> {Status}", statusEvent.OrderId, statusEvent.NewStatus);

                    await _hubContext.Clients.Group(statusEvent.OrderId.ToString()).SendAsync("ReceiveOrderNotification", statusEvent, cancellationToken: stoppingToken);

                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Stopping notification processing; the unacknowledged message will be requeued.");
                }
                catch (JsonException ex)
                {
                    _logger.LogError(ex, "Invalid OrderStatusChangedEvent JSON; discarding message.");
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing status notification message.");
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        return;
                    }

                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
                }
            };

            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: stoppingToken);
            await channel.BasicConsumeAsync(
                queue: RabbitMqConstants.OrderStatusChangedQueue,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken
            );
            _workerStatus.SetConsumer(connection, channel);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
        finally
        {
            _workerStatus.Clear();
            await channel.DisposeAsync();
            await connection.DisposeAsync();
        }
    }
}