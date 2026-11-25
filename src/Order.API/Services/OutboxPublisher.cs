using System.Text;
using Microsoft.EntityFrameworkCore;
using Order.Persistence.Data;
using Order.Persistence.Models;
using OrderSystem.Shared;
using RabbitMQ.Client;

namespace Order.API.Services;

public class OutboxPublisher : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _configuration;
    private readonly ILogger<OutboxPublisher> _logger;

    public OutboxPublisher(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<OutboxPublisher> logger)
    {
        _scopeFactory = scopeFactory;
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _configuration["RabbitMQ:HostName"] ?? "localhost",
            Port = int.Parse(_configuration["RabbitMQ:Port"] ?? "5672"),
            UserName = _configuration["RabbitMQ:UserName"] ?? "guest",
            Password = _configuration["RabbitMQ:Password"] ?? "guest",
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };

        while (!stoppingToken.IsCancellationRequested)
        {
            IConnection? connection = null;
            IChannel? channel = null;

            try
            {
                connection = await factory.CreateConnectionAsync(stoppingToken);
                channel = await connection.CreateChannelAsync(
                    new CreateChannelOptions(true, true),
                    stoppingToken);

                await channel.QueueDeclareAsync(
                    queue: RabbitMqConstants.OrderCreatedQueue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: stoppingToken);

                await channel.QueueDeclareAsync(
                    queue: RabbitMqConstants.OrderStatusChangedQueue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: stoppingToken);

                _logger.LogInformation("Outbox publisher connected to RabbitMQ.");
                await PublishPendingMessagesAsync(channel, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Outbox publisher unavailable; retrying in 5 seconds.");
            }
            finally
            {
                if (channel is not null)
                {
                    await channel.DisposeAsync();
                }

                if (connection is not null)
                {
                    await connection.DisposeAsync();
                }
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task PublishPendingMessagesAsync(IChannel channel, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
            var message = await db.OutboxMessages
                .Where(candidate => candidate.PublishedAt == null)
                .OrderBy(candidate => candidate.CreatedAt)
                .FirstOrDefaultAsync(stoppingToken);

            if (message is null)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                continue;
            }

            var queue = message.EventType switch
            {
                OrderEventTypes.OrderCreated => RabbitMqConstants.OrderCreatedQueue,
                OrderEventTypes.OrderStatusChanged => RabbitMqConstants.OrderStatusChangedQueue,
                _ => throw new InvalidOperationException($"Unknown outbox event type '{message.EventType}'.")
            };

            var properties = new BasicProperties
            {
                Persistent = true,
                MessageId = message.Id.ToString()
            };

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queue,
                mandatory: true,
                basicProperties: properties,
                body: Encoding.UTF8.GetBytes(message.Payload),
                cancellationToken: stoppingToken);

            message.PublishedAt = DateTime.UtcNow;
            await db.SaveChangesAsync(stoppingToken);
            _logger.LogInformation("Published outbox message {MessageId} of type {EventType}.", message.Id, message.EventType);
        }
    }
}
