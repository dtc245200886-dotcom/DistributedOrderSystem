using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Order.Persistence.Data;
using Order.Persistence.Models;
using OrderSystem.Shared;

namespace Order.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IConfiguration _configuration;

    public Worker(ILogger<Worker> logger, IServiceProvider serviceProvider, IConfiguration configuration)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Order.Worker is starting at: {time}", DateTimeOffset.Now);

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

        // Đảm bảo kết nối RabbitMQ thành công (có cơ chế retry nếu RabbitMQ chưa kịp khởi động)
        IConnection? connection = null;
        IChannel? channel = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                connection = await factory.CreateConnectionAsync(stoppingToken);
                channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

                await channel.QueueDeclareAsync(
                    queue: RabbitMqConstants.OrderCreatedQueue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: stoppingToken
                );

                _logger.LogInformation("Order.Worker connected to RabbitMQ successfully and listening to queue.");
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

                _logger.LogWarning(ex, "Could not connect to RabbitMQ. Retrying in 5 seconds...");
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
                    var messageJson = Encoding.UTF8.GetString(body);
                    var orderEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(messageJson);

                    if (orderEvent is null || orderEvent.OrderId == Guid.Empty)
                    {
                        _logger.LogWarning("Received an invalid OrderCreatedEvent message; discarding it.");
                        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                        return;
                    }

                    _logger.LogInformation("Received OrderCreatedEvent for OrderId: {OrderId}", orderEvent.OrderId);
                    var messageId = Guid.TryParse(ea.BasicProperties.MessageId, out var parsedMessageId)
                        ? parsedMessageId
                        : orderEvent.OrderId;
                    await ProcessOrderAsync(orderEvent, messageId, stoppingToken);

                    // Xác nhận đã xử lý xong message (Acknowledge)
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Stopping order processing; the unacknowledged message will be requeued.");
                }
                catch (JsonException ex)
                {
                    _logger.LogError(ex, "Invalid OrderCreatedEvent JSON; discarding message.");
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message from queue.");
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
                queue: RabbitMqConstants.OrderCreatedQueue,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken
            );

            // Giữ Worker chạy liên tục
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
        finally
        {
            await channel.DisposeAsync();
            await connection.DisposeAsync();
        }
    }

    private async Task ProcessOrderAsync(
        OrderCreatedEvent orderEvent,
        Guid messageId,
        CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();

        if (await dbContext.ProcessedMessages.AnyAsync(message => message.Id == messageId, stoppingToken))
        {
            _logger.LogInformation(
                "Ignoring duplicate order event {MessageId} for order {OrderId}.",
                messageId,
                orderEvent.OrderId);
            return;
        }

        var order = await dbContext.Orders
            .Include(candidate => candidate.Items)
            .FirstOrDefaultAsync(candidate => candidate.Id == orderEvent.OrderId, stoppingToken);
        if (order == null)
        {
            _logger.LogWarning("Order with ID {OrderId} not found in database.", orderEvent.OrderId);
            dbContext.ProcessedMessages.Add(new ProcessedMessage { Id = messageId });
            await dbContext.SaveChangesAsync(stoppingToken);
            return;
        }

        if (order.Status is nameof(OrderStatus.Completed) or nameof(OrderStatus.Failed))
        {
            _logger.LogInformation("Ignoring duplicate event for terminal order {OrderId} in state {Status}.", order.Id, order.Status);
            dbContext.ProcessedMessages.Add(new ProcessedMessage { Id = messageId });
            await dbContext.SaveChangesAsync(stoppingToken);
            return;
        }

        if (string.IsNullOrWhiteSpace(order.CustomerName) ||
            order.Items.Count == 0 ||
            order.Items.Any(item =>
                item is null ||
                string.IsNullOrWhiteSpace(item.ProductName) ||
                item.Quantity <= 0 ||
                item.UnitPrice < 0))
        {
            await ChangeStatusAndEnqueueEventAsync(
                dbContext,
                order,
                OrderStatus.Failed,
                "Đơn hàng không hợp lệ nên không thể xử lý.",
                stoppingToken,
                messageId);
            _logger.LogWarning("Order {OrderId} failed validation during processing.", order.Id);
            return;
        }

        if (order.Status == nameof(OrderStatus.Pending))
        {
            await ChangeStatusAndEnqueueEventAsync(
                dbContext,
                order,
                OrderStatus.Processing,
                "Đơn hàng đang được xử lý.",
                stoppingToken);
        }
        else if (order.Status != nameof(OrderStatus.Processing))
        {
            var previousStatus = order.Status;
            await ChangeStatusAndEnqueueEventAsync(
                dbContext,
                order,
                OrderStatus.Failed,
                "Đơn hàng có trạng thái không hợp lệ nên không thể xử lý.",
                stoppingToken,
                messageId);
            _logger.LogError("Order {OrderId} had unsupported status {OldStatus} and was marked failed.", order.Id, previousStatus);
            return;
        }

        // Giả lập thời gian xử lý nghiệp vụ (ví dụ: đóng gói, kiểm tra kho...) trong 3 giây
        await Task.Delay(3000, stoppingToken);

        // Bước 2: Chuyển sang Completed
        await ChangeStatusAndEnqueueEventAsync(
            dbContext,
            order,
            OrderStatus.Completed,
            "Đơn hàng đã hoàn thành và sẵn sàng giao!",
            stoppingToken,
            messageId);

        _logger.LogInformation("Order {OrderId} successfully completed.", order.Id);
    }

    private static async Task ChangeStatusAndEnqueueEventAsync(
        OrderDbContext dbContext,
        OrderEntity order,
        OrderStatus newStatus,
        string message,
        CancellationToken stoppingToken,
        Guid? processedMessageId = null)
    {
        var statusEvent = new OrderStatusChangedEvent(
            order.Id,
            order.CustomerName,
            order.Status,
            newStatus.ToString(),
            message,
            DateTime.UtcNow
        );

        await using var transaction = await dbContext.Database.BeginTransactionAsync(stoppingToken);
        order.Status = newStatus.ToString();
        order.UpdatedAt = DateTime.UtcNow;
        dbContext.OutboxMessages.Add(new OutboxMessage
        {
            EventType = OrderEventTypes.OrderStatusChanged,
            Payload = JsonSerializer.Serialize(statusEvent)
        });
        if (processedMessageId is not null)
        {
            dbContext.ProcessedMessages.Add(new ProcessedMessage { Id = processedMessageId.Value });
        }

        await dbContext.SaveChangesAsync(stoppingToken);
        await transaction.CommitAsync(stoppingToken);
    }
}