using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Client;

namespace Notification.Service.Services;

public sealed class NotificationWorkerStatus
{
    private readonly object _sync = new();
    private IConnection? _connection;
    private IChannel? _channel;

    public bool IsReady
    {
        get
        {
            lock (_sync)
            {
                return _connection?.IsOpen == true && _channel?.IsOpen == true;
            }
        }
    }

    public void SetConsumer(IConnection connection, IChannel channel)
    {
        lock (_sync)
        {
            _connection = connection;
            _channel = channel;
        }
    }

    public void Clear()
    {
        lock (_sync)
        {
            _connection = null;
            _channel = null;
        }
    }
}

public sealed class NotificationConsumerHealthCheck(NotificationWorkerStatus status) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var result = status.IsReady
            ? HealthCheckResult.Healthy("The RabbitMQ notification consumer is ready.")
            : HealthCheckResult.Unhealthy("The RabbitMQ notification consumer is not ready.");

        return Task.FromResult(result);
    }
}
