# Start 3 MiniMQ brokers (run each in a separate terminal)
$jar = Join-Path $PSScriptRoot "..\mq-demo\target\mq-demo-1.0.0-SNAPSHOT.jar"
if (-not (Test-Path $jar)) {
    Write-Host "Build first: mvn -pl mq-demo package -gs .mvn/settings.xml -s .mvn/settings.xml"
    exit 1
}

Write-Host "Starting broker-0 (controller) on 9092..."
Start-Process java -ArgumentList "-jar", $jar, "broker", "--broker-id", "0", "--port", "9092", "--data-dir", "./data/broker-0", "--controller"

Start-Sleep -Seconds 2
Write-Host "Starting broker-1 on 9093..."
Start-Process java -ArgumentList "-jar", $jar, "broker", "--broker-id", "1", "--port", "9093", "--data-dir", "./data/broker-1", "--controller-addr", "localhost:9092"

Start-Sleep -Seconds 1
Write-Host "Starting broker-2 on 9094..."
Start-Process java -ArgumentList "-jar", $jar, "broker", "--broker-id", "2", "--port", "9094", "--data-dir", "./data/broker-2", "--controller-addr", "localhost:9092"

Write-Host "All brokers started."
