using Microsoft.EntityFrameworkCore;
using Order.Persistence.Data;
using Order.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
