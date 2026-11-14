using Microsoft.EntityFrameworkCore;
using Order.API.Services;
using Order.Persistence.Data;
using Order.Persistence.Models;
using OrderSystem.Shared;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>() ?? [];
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
        policy.WithOrigins(allowedOrigins)
            .AllowAnyHeader()
            .AllowAnyMethod());
});

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

app.UseCors();

// Auto-migrate database on startup for convenience
using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
    dbContext.Database.EnsureCreated();
}

// Endpoint 1: POST /orders - Tạo đơn hàng mới
app.MapPost("/orders", async (CreateOrderRequest request, OrderDbContext db, RabbitMqPublisher publisher, ILogger<Program> logger) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerName) || request.Items is null || request.Items.Count == 0)
    {
        return Results.BadRequest(new { message = "Invalid order data. Customer name and items are required." });
    }

    if (request.Items.Any(item =>
            item is null ||
            string.IsNullOrWhiteSpace(item.ProductName) ||
            item.Quantity <= 0 ||
            item.UnitPrice < 0))
    {
        return Results.BadRequest(new
        {
            message = "Each item must have a product name, a positive quantity, and a non-negative unit price."
        });
    }

    decimal totalAmount;
    try
    {
        totalAmount = request.Items.Sum(item => item.Quantity * item.UnitPrice);
    }
    catch (OverflowException)
    {
        return Results.BadRequest(new { message = "The order total is too large." });
    }

    var orderEntity = new OrderEntity
    {
        CustomerName = request.CustomerName.Trim(),
        TotalAmount = totalAmount,
        Status = OrderStatus.Pending.ToString(),
        CreatedAt = DateTime.UtcNow,
        Items = request.Items.Select(i => new OrderItemEntity
        {
            ProductName = i.ProductName.Trim(),
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
        return Results.Problem(
            title: "Order was saved but could not be queued for processing.",
            detail: $"The order remains pending. Retry or inspect order {orderEntity.Id}.",
            statusCode: StatusCodes.Status503ServiceUnavailable,
            extensions: new Dictionary<string, object?> { ["orderId"] = orderEntity.Id });
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