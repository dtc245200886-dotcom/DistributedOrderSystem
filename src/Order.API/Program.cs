using Microsoft.EntityFrameworkCore;
using Order.API.Data;
using Order.API.Models;
using Order.API.Services;
using OrderSystem.Shared;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// Auto-migrate database on startup for convenience
using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
    dbContext.Database.EnsureCreated();
}

// Endpoint 1: POST /orders - Tạo đơn hàng mới
app.MapPost("/orders", async (CreateOrderRequest request, OrderDbContext db, RabbitMqPublisher publisher, ILogger<Program> logger) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerName) || request.Items == null || !request.Items.Any())
    {
        return Results.BadRequest(new { message = "Invalid order data. Customer name and items are required." });
    }

    var totalAmount = request.Items.Sum(i => i.Quantity * i.UnitPrice);

    var orderEntity = new OrderEntity
    {
        CustomerName = request.CustomerName.Trim(),
        TotalAmount = totalAmount,
        Status = OrderStatus.Pending.ToString(),
        CreatedAt = DateTime.UtcNow,
        Items = request.Items.Select(i => new OrderItemEntity
        {
            ProductName = i.ProductName,
            Quantity = i.Quantity,
            UnitPrice = i.UnitPrice
        }).ToList()
    };

    db.Orders.Add(orderEntity);
    await db.SaveChangesAsync();

    logger.LogInformation("Order created successfully with ID: {OrderId}", orderEntity.Id);

    // Đẩy message vào RabbitMQ để Worker xử lý bất đồng bộ
    var orderEvent = new OrderCreatedEvent(
        orderEntity.Id,
        orderEntity.CustomerName,
        orderEntity.TotalAmount,
        orderEntity.CreatedAt
    );

    try
    {
        await publisher.PublishOrderCreatedAsync(orderEvent);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Failed to publish OrderCreatedEvent for Order ID: {OrderId}", orderEntity.Id);
        // Vẫn trả về thành công vì đơn đã lưu DB, có thể retry sau hoặc xử lý bù
    }

    var response = new OrderResponse
    {
        Id = orderEntity.Id,
        CustomerName = orderEntity.CustomerName,
        TotalAmount = orderEntity.TotalAmount,
        Status = orderEntity.Status,
        CreatedAt = orderEntity.CreatedAt,
        Items = orderEntity.Items.Select(i => new OrderItemDto
        {
            ProductName = i.ProductName,
            Quantity = i.Quantity,
            UnitPrice = i.UnitPrice
        }).ToList()
    };

    return Results.Created($"/orders/{response.Id}", response);
})
.WithName("CreateOrder");

// Endpoint 2: GET /orders - Lấy danh sách đơn hàng
app.MapGet("/orders", async (OrderDbContext db) =>
{
    var orders = await db.Orders
        .Include(o => o.Items)
        .OrderByDescending(o => o.CreatedAt)
        .Select(o => new OrderResponse
        {
            Id = o.Id,
            CustomerName = o.CustomerName,
            TotalAmount = o.TotalAmount,
            Status = o.Status,
            CreatedAt = o.CreatedAt,
            UpdatedAt = o.UpdatedAt,
            Items = o.Items.Select(i => new OrderItemDto
            {
                ProductName = i.ProductName,
                Quantity = i.Quantity,
                UnitPrice = i.UnitPrice
            }).ToList()
        })
        .ToListAsync();

    return Results.Ok(orders);
})
.WithName("GetOrders");

// Endpoint 3: GET /orders/{id} - Lấy chi tiết đơn hàng
app.MapGet("/orders/{id:guid}", async (Guid id, OrderDbContext db) =>
{
    var order = await db.Orders
        .Include(o => o.Items)
        .FirstOrDefaultAsync(o => o.Id == id);

    if (order == null)
    {
        return Results.NotFound(new { message = $"Order with ID {id} not found." });
    }

    var response = new OrderResponse
    {
        Id = order.Id,
        CustomerName = order.CustomerName,
        TotalAmount = order.TotalAmount,
        Status = order.Status,
        CreatedAt = order.CreatedAt,
        UpdatedAt = order.UpdatedAt,
        Items = order.Items.Select(i => new OrderItemDto
        {
            ProductName = i.ProductName,
            Quantity = i.Quantity,
            UnitPrice = i.UnitPrice
        }).ToList()
    };

    return Results.Ok(response);
})
.WithName("GetOrderById");

app.Run();