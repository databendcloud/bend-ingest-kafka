# Deployment Guide

## Prerequisites

- Linux with systemd (Ubuntu 20.04+, CentOS 7+, Debian 10+)
- Network access to Kafka cluster and Databend instance
- Go 1.23+ (for building from source) or pre-built binary from [releases](https://github.com/databendcloud/bend-ingest-kafka/releases)

## Build

```bash
git clone https://github.com/databendcloud/bend-ingest-kafka.git
cd bend-ingest-kafka
CGO_ENABLED=1 go build -o bend-ingest-kafka .
```

## Install

```bash
# Create installation directory
sudo mkdir -p /opt/bend-ingest-kafka

# Copy binary
sudo cp bend-ingest-kafka /opt/bend-ingest-kafka/
sudo chmod +x /opt/bend-ingest-kafka/bend-ingest-kafka

# Copy and edit config
sudo cp config/conf.json /opt/bend-ingest-kafka/config.json
sudo vi /opt/bend-ingest-kafka/config.json

# Create dedicated service user
sudo useradd -r -s /sbin/nologin -d /opt/bend-ingest-kafka databend
sudo chown -R databend:databend /opt/bend-ingest-kafka
```

## Configuration

Edit `/opt/bend-ingest-kafka/config.json`:

```json
{
  "kafkaBootstrapServers": "kafka1:9092,kafka2:9092,kafka3:9092",
  "kafkaTopic": "your-topic",
  "KafkaConsumerGroup": "your-consumer-group",
  "isSASL": true,
  "saslUser": "kafka-user",
  "saslPassword": "kafka-password",
  "databendDSN": "http://user:password@databend-host:8002",
  "databendTable": "db.target_table",
  "isJsonTransform": true,
  "batchSize": 5000,
  "batchMaxInterval": 30,
  "dataFormat": "json",
  "workers": 2,
  "metricsPort": 2112
}
```

Key parameters to tune for production:
- `batchSize`: larger batches = higher throughput, higher latency (recommended: 5000-10000)
- `batchMaxInterval`: max seconds to wait before flushing an incomplete batch
- `workers`: number of parallel consumers (match Kafka partition count for best throughput)

## systemd Service

### Install Service

```bash
sudo cp deploy/bend-ingest-kafka.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable bend-ingest-kafka
sudo systemctl start bend-ingest-kafka
```

### Service Management

```bash
# Start
sudo systemctl start bend-ingest-kafka

# Stop (graceful — waits for current batch to finish)
sudo systemctl stop bend-ingest-kafka

# Restart
sudo systemctl restart bend-ingest-kafka

# Check status
sudo systemctl status bend-ingest-kafka

# Reload config (requires restart)
sudo systemctl restart bend-ingest-kafka
```

### Logs

```bash
# Follow logs in real-time
journalctl -u bend-ingest-kafka -f

# View recent logs
journalctl -u bend-ingest-kafka --since "30 minutes ago"

# View logs from last boot
journalctl -u bend-ingest-kafka -b

# Export logs to file
journalctl -u bend-ingest-kafka --since today > /tmp/ingest-kafka.log
```

## Auto-restart & Failure Handling

The systemd service is configured with:

| Setting | Value | Description |
|---------|-------|-------------|
| `Restart` | `on-failure` | Restarts automatically on non-zero exit |
| `RestartSec` | `5` | Wait 5 seconds between restarts |
| `StartLimitBurst` | `5` | Max 5 restarts per interval |
| `StartLimitIntervalSec` | `60` | Interval for burst limit (1 minute) |
| `TimeoutStopSec` | `30` | Allows 30s for graceful shutdown |
| `KillSignal` | `SIGTERM` | Triggers graceful shutdown |

If the service crashes more than 5 times in 60 seconds, systemd stops trying. To reset:

```bash
sudo systemctl reset-failed bend-ingest-kafka
sudo systemctl start bend-ingest-kafka
```

## Monitoring

### Prometheus Metrics

The service exposes metrics at `http://<host>:2112/metrics`.

Add to your Prometheus `scrape_configs`:

```yaml
scrape_configs:
  - job_name: 'bend-ingest-kafka'
    static_configs:
      - targets: ['your-host:2112']
```

### Health Check

A simple health check script for external monitoring:

```bash
#!/bin/bash
# Check if process is running
systemctl is-active --quiet bend-ingest-kafka || exit 1

# Check if metrics endpoint responds
curl -sf http://localhost:2112/metrics > /dev/null || exit 1

exit 0
```

### Alerting Recommendations

| Alert | Condition | Severity |
|-------|-----------|----------|
| Service down | `systemctl is-active` != active | Critical |
| High error rate | `rate(bend_ingest_kafka_ingest_errors_total[5m]) > 0.1` | Warning |
| Ingest stalled | `rate(bend_ingest_kafka_ingest_rows_total[5m]) == 0` | Critical |
| High upload latency | `histogram_quantile(0.95, bend_ingest_kafka_upload_stage_duration_seconds) > 10` | Warning |
| High copy latency | `histogram_quantile(0.95, bend_ingest_kafka_copy_into_duration_seconds) > 10` | Warning |

## Upgrade

```bash
# Build or download new version
CGO_ENABLED=1 go build -o bend-ingest-kafka .

# Replace binary and restart
sudo cp bend-ingest-kafka /opt/bend-ingest-kafka/
sudo systemctl restart bend-ingest-kafka

# Verify
sudo systemctl status bend-ingest-kafka
journalctl -u bend-ingest-kafka --since "1 minute ago"
```

## Troubleshooting

### Service won't start

```bash
# Check logs for error details
journalctl -u bend-ingest-kafka -n 50 --no-pager

# Common issues:
# - Config file not found: check path in ExecStart
# - Permission denied: check file ownership (databend:databend)
# - Port conflict: check metricsPort not in use
```

### Kafka connection issues

```bash
# Test connectivity from the host
nc -z kafka-host 9092 && echo "OK" || echo "UNREACHABLE"

# Check for SASL/TLS misconfiguration in logs
journalctl -u bend-ingest-kafka | grep -i "sasl\|tls\|auth"
```

### Databend connection issues

```bash
# Test Databend health
curl -u user:password http://databend-host:8000/v1/health

# Check DSN format in config
# Correct: "http://user:password@host:8002"
```

### Consumer lag growing

If Kafka consumer lag keeps increasing:
1. Increase `workers` to match partition count
2. Increase `batchSize` to reduce per-batch overhead
3. Check Databend performance (upload/copy latency in metrics)
4. Consider scaling horizontally with multiple instances (each with unique consumer group or partitions)
