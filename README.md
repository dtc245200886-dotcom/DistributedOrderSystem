# Hệ thống xử lý đơn hàng phân tán

Dự án minh họa xử lý đơn hàng trên .NET 10, gồm ASP.NET Core Minimal API, PostgreSQL, RabbitMQ, worker chạy nền, dịch vụ thông báo SignalR và ứng dụng web cho khách hàng.

## Chạy bằng Docker Compose

Yêu cầu: Docker Desktop có hỗ trợ Docker Compose.

Mở PowerShell tại thư mục gốc của repository và chạy:

```powershell
docker compose up --build
```

Mở ứng dụng khách tại <http://localhost:5003>. Order API chạy tại <http://localhost:5000>, SignalR sử dụng đường dẫn `/orderHub` trên cổng `5002`, và RabbitMQ Management chạy tại <http://localhost:15672>.

Thông tin đăng nhập mặc định trong Compose chỉ dùng cho môi trường phát triển cục bộ. Có thể thay đổi bằng biến môi trường hoặc file `.env` cục bộ:

| Biến | Giá trị mặc định |
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

Lệnh `docker compose down` dừng và xóa các container nhưng giữ lại volume PostgreSQL. Lệnh `docker compose down -v` cũng xóa dữ liệu trong volume đó.

## Các service và luồng xử lý

1. Ứng dụng khách gửi `POST /orders` đến Order API.
2. API kiểm tra dữ liệu, sau đó lưu đơn hàng ở trạng thái `Pending` cùng event trong outbox bằng một transaction cơ sở dữ liệu.
3. Order Worker nhận event và cập nhật đơn hàng từ `Pending` sang `Processing`, rồi `Completed`; đơn có dữ liệu đã lưu không hợp lệ hoặc trạng thái không được hỗ trợ sẽ chuyển sang `Failed`.
4. Worker ghi thay đổi trạng thái và event tương ứng vào outbox trong cùng transaction. Outbox publisher của API gửi các event đang chờ vào RabbitMQ và thử lại khi broker tạm thời gián đoạn.
5. Notification Service chuyển tiếp event trạng thái đến các client SignalR đang theo dõi đơn hàng. Giao diện client cập nhật trạng thái theo thời gian thực.

Outbox giúp hệ thống có thể tiếp tục gửi event khi RabbitMQ tạm thời không hoạt động. Cơ chế gửi là **at-least-once**: nếu tiến trình dừng sau khi RabbitMQ xác nhận event nhưng trước khi outbox được đánh dấu đã gửi, event có thể được gửi lặp lại. Vì vậy, consumer nên xử lý được trường hợp nhận event trùng.

Hiện tại, `Failed` được dùng khi dữ liệu đơn đã lưu không hợp lệ hoặc trạng thái lưu không được hỗ trợ. API từ chối request không hợp lệ ngay từ đầu, vì vậy các đơn tạo bình thường sẽ theo luồng thành công trừ khi bổ sung quy tắc thất bại nghiệp vụ.

Order API cung cấp các endpoint:

- `POST /orders`
- `GET /orders`
- `GET /orders/{id}`

## Build và kiểm thử

Chạy các lệnh sau từ thư mục gốc:

```powershell
dotnet restore DistributedOrderSystem.slnx
dotnet build DistributedOrderSystem.slnx --no-restore
docker compose config
docker compose build
```

Để kiểm thử toàn luồng, khởi chạy Compose, tạo một đơn hợp lệ trên giao diện web, rồi xác nhận trạng thái chuyển đến `Completed` và nhận được thông báo `Processing` cùng `Completed`. Để kiểm tra khả năng phục hồi, tạm dừng RabbitMQ, tạo đơn (API vẫn lưu đơn), sau đó khởi động lại RabbitMQ và xác nhận đơn được xử lý hoàn tất.

Có thể chạy smoke test cho API từ thư mục gốc:

```powershell
.\tests\SmokeTest.ps1
```

Để smoke test đồng thời tạm dừng và khởi động lại service RabbitMQ trong Compose:

```powershell
.\tests\SmokeTest.ps1 -TestRabbitMqRecovery
```

Mỗi service .NET có thể chạy độc lập đều có Dockerfile riêng ở thư mục gốc: `Dockerfile.api`, `Dockerfile.worker`, `Dockerfile.notification` và `Dockerfile.client`.
