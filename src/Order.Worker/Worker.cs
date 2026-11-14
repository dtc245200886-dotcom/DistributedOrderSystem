using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Order.API.Data;
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
            Password = password
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

                await channel.QueueDeclareAsync(
                    queue: RabbitMqConstants.OrderStatusChangedQueue,
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

                    if (orderEvent != null)
                    {
                        _logger.LogInformation("Received OrderCreatedEvent for OrderId: {OrderId}", orderEvent.OrderId);

                        // Xử lý đơn hàng bất đồng bộ và cập nhật Database qua Entity Framework Core
                        await ProcessOrderAsync(orderEvent, channel, stoppingToken);
                    }

                    // Xác nhận đã xử lý xong message (Acknowledge)
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message from queue.");
                    // Từ chối message và đưa lại hàng đợi nếu lỗi tạm thời
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
                }
            };

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

    private async Task ProcessOrderAsync(OrderCreatedEvent orderEvent, IChannel channel, CancellationToken stoppingToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();

        var order = await dbContext.Orders.FindAsync(new object[] { orderEvent.OrderId }, stoppingToken);
        if (order == null)
        {
            _logger.LogWarning("Order with ID {OrderId} not found in database.", orderEvent.OrderId);
            return;
        }

        // Bước 1: Chuyển sang trạng thái Processing
        string oldStatus = order.Status;
        order.Status = OrderStatus.Processing.ToString();
        order.UpdatedAt = DateTime.UtcNow;
        await dbContext.SaveChangesAsync(stoppingToken);

        _logger.LogInformation("Order {OrderId} status changed from {Old} to {New}", order.Id, oldStatus, order.Status);
        await PublishStatusChangedEventAsync(channel, order.Id, order.CustomerName, oldStatus, order.Status, "Đơn hàng đang được xử lý.");

        // Giả lập thời gian xử lý nghiệp vụ (ví dụ: đóng gói, kiểm tra kho...) trong 3 giây
        await Task.Delay(3000, stoppingToken);

        // Bước 2: Chuyển sang Completed (hoặc Failed tùy logic ngẫu nhiên/thực tế)
        oldStatus = order.Status;
        order.Status = OrderStatus.Completed.ToString();
        order.UpdatedAt = DateTime.UtcNow;
        await dbContext.SaveChangesAsync(stoppingToken);

        _logger.LogInformation("Order {OrderId} successfully completed.", order.Id);
        await PublishStatusChangedEventAsync(channel, order.Id, order.CustomerName, oldStatus, order.Status, "Đơn hàng đã hoàn thành và sẵn sàng giao!");
    }

    private async Task PublishStatusChangedEventAsync(IChannel channel, Guid orderId, string customerName, string oldStatus, string newStatus, string message)
    {
        var statusEvent = new OrderStatusChangedEvent(
            orderId,
            customerName,
            oldStatus,
            newStatus,
            message,
            DateTime.UtcNow
        );

        var json = JsonSerializer.Serialize(statusEvent);
        var body = Encoding.UTF8.GetBytes(json);

        var props = new BasicProperties { Persistent = true };

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: RabbitMqConstants.OrderStatusChangedQueue,
            mandatory: true,
            basicProperties: props,
            body: body
        );
    }
}