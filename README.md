# Distributed Order System

A .NET 10 order-processing demo with an ASP.NET Core Minimal API, PostgreSQL, RabbitMQ, a background worker, a SignalR notification service, and a web client.

## Run with Docker Compose

Requirements: Docker Desktop with Docker Compose.

From the repository root:

```powershell
docker compose up --build
```

Open the client at <http://localhost:5003>. The API is available at <http://localhost:5000>, SignalR uses `/orderHub` on port `5002`, and RabbitMQ Management is at <http://localhost:15672>.

The default Compose credentials are for local development only. Override them with environment variables or a local `.env` file:

| Variable | Default |
| --- | --- |
| `POSTGRES_DB` | `order_db` |
| `POSTGRES_USER` | `postgres` |
| `POSTGRES_PASSWORD` | `postgres` |
| `RABBITMQ_USER` | `orderapp` |
| `RABBITMQ_PASSWORD` | `orderapp_dev_password` |
| `ORDER_API_PORT` | `5000` |
| `NOTIFICATION_PORT` | `5002` |
| `ORDER_CLIENT_PORT` | `5003` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_MANAGEMENT_PORT` | `15672` |
| `POSTGRES_PORT` | `5432` |

`docker compose down` stops and removes the containers but preserves the PostgreSQL volume. `docker compose down -v` also deletes that data.

## Services and request flow

1. The client sends `POST /orders` to Order API.
2. The API validates the order, stores it as `Pending`, and publishes an `OrderCreatedEvent`.
3. Order Worker consumes the event and updates the order through `Processing` to `Completed`, or to `Failed` when the persisted order is invalid.
4. The Worker publishes status changes. Notification Service forwards them to SignalR clients tracking that order.

The API also provides:

- `GET /orders`
- `GET /orders/{id}`

## Build

```powershell
dotnet restore DistributedOrderSystem.slnx
dotnet build DistributedOrderSystem.slnx --no-restore
docker compose config
docker compose build
```

Each runnable .NET service has its own root-level Dockerfile: `Dockerfile.api`, `Dockerfile.worker`, `Dockerfile.notification`, and `Dockerfile.client`.
