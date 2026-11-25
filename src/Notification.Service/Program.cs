using Notification.Service.Hubs;
using Notification.Service.Services;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();

builder.Services.AddSignalR();
builder.Services.AddSingleton<NotificationWorkerStatus>();
builder.Services.AddHostedService<NotificationWorker>();
builder.Services.AddHealthChecks()
    .AddCheck<NotificationConsumerHealthCheck>("rabbitmq-consumer", tags: ["ready"]);

var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>() ?? [];
builder.Services.AddCors(options =>
{
    options.AddPolicy("CorsPolicy", policy =>
    {
        policy.WithOrigins(allowedOrigins)
              .AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials();
    });
});

var app = builder.Build();

app.UseCors("CorsPolicy");
app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = _ => false
}).WithTags("health");
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready")
}).WithTags("health");

app.MapHub<OrderNotificationHub>("/orderHub");

app.MapGet("/", () => "Notification Service is running!");

app.Run();