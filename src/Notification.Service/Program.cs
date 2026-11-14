using Notification.Service.Hubs;
using Notification.Service.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR();
builder.Services.AddHostedService<NotificationWorker>();

builder.Services.AddCors(options =>
{
    options.AddPolicy("CorsPolicy", policy =>
    {
        policy.AllowAnyHeader()
              .AllowAnyMethod()
              .SetIsOriginAllowed(_ => true)
              .AllowCredentials();
    });
});

var app = builder.Build();

app.UseCors("CorsPolicy");

app.MapHub<OrderNotificationHub>("/orderHub");

app.MapGet("/", () => "Notification Service is running!");

app.Run();