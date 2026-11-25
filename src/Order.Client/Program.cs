var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();
builder.Services.AddHealthChecks();
var orderApiUrl = builder.Configuration["Services:OrderApiUrl"] ?? "http://localhost:5227";
var notificationHubUrl = builder.Configuration["Services:NotificationHubUrl"] ?? "http://localhost:5144/orderHub";

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/config", () => new
{
    OrderApiUrl = orderApiUrl,
    NotificationHubUrl = notificationHubUrl
});
app.MapHealthChecks("/health/live");

app.MapFallbackToFile("index.html");

app.Run();
