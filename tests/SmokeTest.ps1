param(
    [string]$ApiUrl = "http://localhost:5000",
    [string]$NotificationUrl = "http://localhost:5002",
    [string]$ClientUrl = "http://localhost:5003",
    [switch]$TestRabbitMqRecovery,
    [switch]$TestContainerRestart
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http
$httpClient = [System.Net.Http.HttpClient]::new()

function Send-JsonRequest {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body
    )

    $request = [System.Net.Http.HttpRequestMessage]::new(
        [System.Net.Http.HttpMethod]::new($Method),
        "$ApiUrl$Path")

    if ($null -ne $Body) {
        $json = ConvertTo-Json -InputObject $Body -Depth 10
        $request.Content = [System.Net.Http.StringContent]::new(
            $json,
            [System.Text.Encoding]::UTF8,
            "application/json")
    }

    return $httpClient.SendAsync($request).GetAwaiter().GetResult()
}

function New-TestOrder {
    $body = @{
        customerName = "Smoke Test"
        items = @(
            @{
                productName = "Test Product One"
                quantity = 1
                unitPrice = 10
            },
            @{
                productName = "Test Product Two"
                quantity = 2
                unitPrice = 20
            }
        )
    }

    $response = Send-JsonRequest -Method "POST" -Path "/orders" -Body $body
    if ([int]$response.StatusCode -ne 201) {
        $details = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        throw "Expected HTTP 201 from POST /orders, got $([int]$response.StatusCode): $details"
    }

    return $response.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
}

function Wait-ForOrderStatus {
    param(
        [string]$OrderId,
        [string]$ExpectedStatus,
        [int]$TimeoutSeconds = 60
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $response = Send-JsonRequest -Method "GET" -Path "/orders/$OrderId"
        if ([int]$response.StatusCode -ne 200) {
            $details = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            throw "GET /orders/$OrderId returned HTTP $([int]$response.StatusCode): $details"
        }

        $order = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
        if ($order.status -eq $ExpectedStatus) {
            return $order
        }

        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)

    throw "Order $OrderId did not reach '$ExpectedStatus' within $TimeoutSeconds seconds."
}

function Assert-HealthEndpoint {
    param(
        [string]$BaseUrl,
        [string]$Path
    )

    $request = [System.Net.Http.HttpRequestMessage]::new(
        [System.Net.Http.HttpMethod]::Get,
        "$BaseUrl$Path")
    $response = $httpClient.SendAsync($request).GetAwaiter().GetResult()
    if ([int]$response.StatusCode -ne 200) {
        $details = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        throw "Expected $BaseUrl$Path to return HTTP 200, got $([int]$response.StatusCode): $details"
    }

    $response.Dispose()
    $request.Dispose()
}

function Wait-ForHealthStatus {
    param(
        [string]$BaseUrl,
        [string]$Path,
        [int]$ExpectedStatusCode,
        [int]$TimeoutSeconds = 60
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $request = [System.Net.Http.HttpRequestMessage]::new(
            [System.Net.Http.HttpMethod]::Get,
            "$BaseUrl$Path")
        try {
            $response = $httpClient.SendAsync($request).GetAwaiter().GetResult()
            $statusCode = [int]$response.StatusCode
            $response.Dispose()
        }
        finally {
            $request.Dispose()
        }

        if ($statusCode -eq $ExpectedStatusCode) {
            return
        }

        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)

    throw "Expected $BaseUrl$Path to return HTTP $ExpectedStatusCode within $TimeoutSeconds seconds, got $statusCode."
}

try {
    Assert-HealthEndpoint -BaseUrl $ApiUrl -Path "/health/live"
    Assert-HealthEndpoint -BaseUrl $ApiUrl -Path "/health/ready"
    Assert-HealthEndpoint -BaseUrl $NotificationUrl -Path "/health/ready"
    Assert-HealthEndpoint -BaseUrl $ClientUrl -Path "/health/live"
    Write-Output "Service health checks: passed"

    $invalidBody = @{
        customerName = "Invalid Smoke Test"
        items = @(@{
            productName = ""
            quantity = 0
            unitPrice = -1
        })
    }
    $invalidResponse = Send-JsonRequest -Method "POST" -Path "/orders" -Body $invalidBody
    if ([int]$invalidResponse.StatusCode -ne 400) {
        throw "Expected invalid order to return HTTP 400, got $([int]$invalidResponse.StatusCode)."
    }
    Write-Output "Invalid order validation: passed"

    $order = New-TestOrder
    $detailResponse = Send-JsonRequest -Method "GET" -Path "/orders/$($order.id)"
    if ([int]$detailResponse.StatusCode -ne 200) {
        $details = $detailResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        throw "Expected GET /orders/$($order.id) to return HTTP 200, got $([int]$detailResponse.StatusCode): $details"
    }

    $detail = $detailResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
    if ($detail.id -ne $order.id -or @($detail.items).Count -ne 2) {
        throw "GET /orders/$($order.id) did not return the order with both product items."
    }
    Write-Output "Order detail lookup with multiple products: passed"

    $missingId = [guid]::NewGuid()
    $missingResponse = Send-JsonRequest -Method "GET" -Path "/orders/$missingId"
    if ([int]$missingResponse.StatusCode -ne 404) {
        throw "Expected an unknown order to return HTTP 404, got $([int]$missingResponse.StatusCode)."
    }
    Write-Output "Missing order lookup: passed"

    $listResponse = Send-JsonRequest -Method "GET" -Path "/orders"
    if ([int]$listResponse.StatusCode -ne 200) {
        throw "Expected GET /orders to return HTTP 200, got $([int]$listResponse.StatusCode)."
    }

    $orders = $listResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
    if ($null -eq ($orders | Where-Object { $_.id -eq $order.id })) {
        throw "GET /orders did not contain the newly created order $($order.id)."
    }
    Write-Output "Order list lookup: passed"

    $completedOrder = Wait-ForOrderStatus -OrderId $order.id -ExpectedStatus "Completed"
    Write-Output "Order processing: $($completedOrder.id) reached $($completedOrder.status)"

    if ($TestRabbitMqRecovery) {
        docker compose stop rabbitmq
        if ($LASTEXITCODE -ne 0) {
            throw "Could not stop the RabbitMQ Compose service."
        }

        try {
            Wait-ForHealthStatus -BaseUrl $NotificationUrl -Path "/health/ready" -ExpectedStatusCode 503
            Assert-HealthEndpoint -BaseUrl $ApiUrl -Path "/health/ready"
            Write-Output "Health status during RabbitMQ outage: passed"

            $pendingOrder = New-TestOrder
            Start-Sleep -Seconds 2
            $response = Send-JsonRequest -Method "GET" -Path "/orders/$($pendingOrder.id)"
            $pendingState = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
            if ($pendingState.status -ne "Pending") {
                throw "Expected the order to remain Pending while RabbitMQ is stopped, got '$($pendingState.status)'."
            }
            Write-Output "Outbox persistence while RabbitMQ is stopped: passed"
        }
        finally {
            docker compose start rabbitmq
            if ($LASTEXITCODE -ne 0) {
                throw "Could not restart the RabbitMQ Compose service."
            }
        }

        Wait-ForHealthStatus -BaseUrl $NotificationUrl -Path "/health/ready" -ExpectedStatusCode 200
        Write-Output "Notification readiness after RabbitMQ recovery: passed"

        $recoveredOrder = Wait-ForOrderStatus -OrderId $pendingOrder.id -ExpectedStatus "Completed"
        Write-Output "Outbox recovery: $($recoveredOrder.id) reached $($recoveredOrder.status)"
    }

    if ($TestContainerRestart) {
        $containers = @(
            "order_postgres",
            "order_rabbitmq",
            "order_api",
            "order_worker",
            "notification_service",
            "order_client"
        )
        foreach ($container in $containers) {
            $restartPolicy = docker inspect -f '{{.HostConfig.RestartPolicy.Name}}' $container
            if ($LASTEXITCODE -ne 0 -or $restartPolicy -ne "unless-stopped") {
                throw "Container '$container' does not have the expected restart policy."
            }
        }

        $initialRestartCount = [int](docker inspect -f '{{.RestartCount}}' order_worker)
        docker exec order_worker sh -c 'kill -TERM 1'
        if ($LASTEXITCODE -ne 0) {
            throw "Could not stop the Order Worker process for the restart test."
        }

        $deadline = [DateTime]::UtcNow.AddSeconds(60)
        do {
            $workerState = docker inspect -f '{{.State.Status}}' order_worker
            $restartCount = [int](docker inspect -f '{{.RestartCount}}' order_worker)
            if ($workerState -eq "running" -and $restartCount -gt $initialRestartCount) {
                break
            }

            Start-Sleep -Seconds 1
        } while ([DateTime]::UtcNow -lt $deadline)

        if ($workerState -ne "running" -or $restartCount -le $initialRestartCount) {
            throw "Order Worker did not automatically restart within 60 seconds."
        }
        Write-Output "Order Worker container automatic restart: passed"

        $restartedOrder = New-TestOrder
        $completedAfterRestart = Wait-ForOrderStatus -OrderId $restartedOrder.id -ExpectedStatus "Completed"
        Write-Output "Order processing after Worker restart: $($completedAfterRestart.id) reached $($completedAfterRestart.status)"
    }
}
finally {
    $httpClient.Dispose()
}
