using Microsoft.AspNetCore.SignalR;

namespace Notification.Service.Hubs;

public class OrderNotificationHub : Hub
{
    public async Task TrackOrder(string orderId)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, orderId);
    }
}