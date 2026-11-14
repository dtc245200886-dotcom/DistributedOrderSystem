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
2. The API validates the order and stores it as `Pending` together with an outbox event in one database transaction.
3. Order Worker consumes the event and updates the order through `Processing` to `Completed`, or to `Failed` when the persisted order is invalid.
4. The Worker records each status change and its outbox event in one database transaction. The API's outbox publisher sends pending events to durable RabbitMQ queues and retries after broker interruptions.
5. Notification Service forwards status events to SignalR clients tracking that order. The client updates the order status live.

The outbox makes order creation and status updates recoverable when RabbitMQ is temporarily unavailable. Delivery is at-least-once: a process crash after RabbitMQ confirms a message but before the outbox row is marked published can cause a duplicate event. Consumers should therefore remain idempotent.

`Failed` is currently used for invalid persisted order data or an unsupported stored status. The API rejects invalid requests, so normal client-created orders follow the successful path unless business failure rules are added.

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

For an end-to-end smoke test, start Compose, submit a valid order in the web client, and confirm its status reaches `Completed` with `Processing` and `Completed` notifications. To test broker recovery, stop RabbitMQ temporarily, create an order (the API should still accept and persist it), then start RabbitMQ again and confirm the order completes.

The API smoke test can be run from the repository root:

```powershell
.\tests\SmokeTest.ps1
```

To also stop and restart the Compose RabbitMQ service during the test:

```powershell
.\tests\SmokeTest.ps1 -TestRabbitMqRecovery
```

Each runnable .NET service has its own root-level Dockerfile: `Dockerfile.api`, `Dockerfile.worker`, `Dockerfile.notification`, and `Dockerfile.client`.
