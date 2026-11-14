namespace OrderSystem.Shared;

public static class RabbitMqConstants
{
    // Queue 1: Order.API đẩy yêu cầu xử lý -> Order.Worker nhận
    public const string OrderCreatedQueue = "order.created.queue";

    // Queue 2: Order.Worker báo đổi trạng thái -> Notification.Service nhận
    public const string OrderStatusChangedQueue = "order.status.changed.queue";
}

// Message 1: Gửi khi có đơn hàng mới tạo
public record OrderCreatedEvent(
    Guid OrderId,
    string CustomerName,
    decimal TotalAmount,
    DateTime CreatedAt
);

// Message 2: Gửi mỗi khi Worker cập nhật trạng thái đơn hàng
public record OrderStatusChangedEvent(
    Guid OrderId,
    string CustomerName,
    string OldStatus,
    string NewStatus,
    string Message,
    DateTime Timestamp
);