param(
    [string]$ApiUrl = "http://localhost:5000",
    [switch]$TestRabbitMqRecovery
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
        items = @(@{
            productName = "Test Product"
            quantity = 1
            unitPrice = 10
        })
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

try {
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

        $recoveredOrder = Wait-ForOrderStatus -OrderId $pendingOrder.id -ExpectedStatus "Completed"
        Write-Output "Outbox recovery: $($recoveredOrder.id) reached $($recoveredOrder.status)"
    }
}
finally {
    $httpClient.Dispose()
}
