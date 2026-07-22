# bend-ingest-kafka Auto Rebalance 端到端验证指南

本文档用于在本地重复验证以下完整链路：

1. 启动真实 Kafka 和 Databend。
2. 运行依赖真实服务的 Go 测试。
3. 启动 `bend-ingest-kafka`，消费一个初始为 2 partitions 的 topic。
4. 在线将 topic 扩容到 8 partitions，验证 consumer group 自动 rebalance。
5. 写入 10 万条数据，验证无丢失、无重复、新 partitions 被消费且最终 lag 为 0。

文档中的默认路径与本地开发环境一致：

```bash
export REPO=/Users/hanshanjie/git-works/bend-ingest-kafka
export DATABEND_COMPOSE_DIR=/Users/hanshanjie/databend/local-test/databend/docker
export KAFKA_COMPOSE_DIR="$REPO/scripts"
```

## 1. 前置条件

- Docker Desktop
- Docker Compose
- Go 1.23+
- `curl`
- `python3`

确认 Docker Engine 正常：

```bash
docker info --format '{{.ServerVersion}}'
```

如果出现 Docker socket `Internal Server Error`，重启 Docker Desktop：

```bash
osascript -e 'quit app "Docker"'
sleep 5
open -a Docker

# 等待 Engine 恢复
until docker info >/dev/null 2>&1; do sleep 3; done
docker info --format '{{.ServerVersion}}'
```

## 2. 启动 Databend

```bash
cd "$DATABEND_COMPOSE_DIR"
docker compose up -d --remove-orphans
```

等待 SQL API 可查询：

```bash
until curl -fsS -u databend:databend \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT 1"}' \
  http://127.0.0.1:8002/v1/query \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d.get("data") == [["1"]] and not d.get("error")' \
do
  sleep 2
done

docker compose ps
```

> compose 中的 Databend healthcheck 使用容器内部 admin 端口 `8080`。宿主机
> `8002` 映射的是 query HTTP 端口 `8000`，因此不要用
> `http://127.0.0.1:8002/v1/health` 判断 readiness。

本地连接信息：

- HTTP Query API：`http://127.0.0.1:8002`
- 用户名：`databend`
- 密码：`databend`

验证 SQL API：

```bash
curl -fsS -u databend:databend \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT version()"}' \
  http://127.0.0.1:8002/v1/query
```

## 3. 启动 Kafka

```bash
cd "$KAFKA_COMPOSE_DIR"
docker compose up -d --remove-orphans
```

等待 Kafka 健康：

```bash
until [ "$(docker inspect kafka --format '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do
  sleep 2
done

docker inspect kafka --format '{{.State.Health.Status}}'
```

Kafka 对外地址为 `127.0.0.1:9092`。

### 同名 Kafka 容器冲突

如果 compose 报错：

```text
Conflict. The container name "/kafka" is already in use
```

先检查已有容器：

```bash
docker inspect kafka --format \
  'image={{.Config.Image}} status={{.State.Status}} health={{.State.Health.Status}} ports={{json .NetworkSettings.Ports}}'
```

如果它就是本项目使用的 Kafka，并且 `9092` 可用，可以直接复用。只有确认它是无用的旧容器后，才执行：

```bash
docker rm -f kafka
cd "$KAFKA_COMPOSE_DIR"
docker compose up -d --remove-orphans
```

## 4. 运行真实依赖测试

在项目根目录运行：

```bash
cd "$REPO"

TEST_DATABEND_DSN='http://databend:databend@localhost:8002?presigned_url_disabled=true' \
TEST_KAFKA_BROKER='127.0.0.1:9092' \
go test . ./config ./message -count=1 -timeout=15m
```

单独运行 Kafka batch reader integration test：

```bash
TEST_KAFKA_BROKER='127.0.0.1:9092' \
go test . -run '^TestKafkaBatchReader_Integration$' -count=1 -timeout=180s
```

> 当前不要使用 `go test ./...` 作为验收命令。`scripts/create_topic.go` 和
> `scripts/produce_demo.go` 位于同一个目录，并且都声明了 `main`，Go 会把它们当作
> 同一个 package 编译，从而报 `main redeclared`。这与 Kafka/Databend 集成链路无关。

## 5. 初始化本次 E2E 运行参数

使用唯一的 run ID，避免与历史 topic、consumer group 和 Databend 表冲突：

```bash
export RUN_ID=$(date +%Y%m%d_%H%M%S)
export TOPIC="e2e_auto_rebalance_${RUN_ID}"
export TABLE="e2e_auto_rebalance_${RUN_ID}"
export GROUP="e2e-auto-rebalance-${RUN_ID}"

printf 'TOPIC=%s\nTABLE=%s\nGROUP=%s\n' "$TOPIC" "$TABLE" "$GROUP"
```

后续命令应在同一个终端执行。如果打开新终端，需要重新导出这些变量。

## 6. 创建初始 2-partition topic

```bash
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic "$TOPIC" \
  --partitions 2 \
  --replication-factor 1
```

检查 topic：

```bash
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic "$TOPIC"
```

## 7. 准备 E2E 配置

创建一次性配置 `/tmp/bend-ingest-kafka-e2e.json`：

```bash
cat > /tmp/bend-ingest-kafka-e2e.json <<EOF
{
  "kafkaBootstrapServers": "127.0.0.1:9092",
  "kafkaTopic": "$TOPIC",
  "KafkaConsumerGroup": "$GROUP",
  "isSASL": false,
  "disableTLS": true,
  "isJsonTransform": false,
  "databendDSN": "http://databend:databend@localhost:8002",
  "databendTable": "default.$TABLE",
  "batchSize": 1000,
  "batchMaxInterval": 2,
  "dataFormat": "json",
  "workers": 4,
  "copyPurge": true,
  "copyForce": false,
  "disableVariantCheck": true,
  "minBytes": 1,
  "maxBytes": 20971520,
  "maxWait": 1,
  "useReplaceMode": false,
  "useStreamingLoad": true,
  "copyIntoUploadCompression": true,
  "maxRetryDelay": 30,
  "metricsPort": 22112,
  "enableRebalanceOptimization": true,
  "partitionAssignmentStrategy": "range,roundrobin",
  "topicMetadataRefreshIntervalMs": 5000
}
EOF
```

这里使用 raw mode（`isJsonTransform=false`），应用会自动创建目标表。使用
`range,roundrobin` 是为了保持与 librdkafka 旧版本和滚动升级兼容。

## 8. 准备可指定 partition 的生产器

在线扩容测试必须明确向新增 partitions 写数据。项目自带的 demo producer 使用固定
topic 且不能指定 partition，因此创建一次性生产器 `/tmp/e2e_producer.go`：

```bash
cat > /tmp/e2e_producer.go <<'EOF'
package main

import (
    "flag"
    "fmt"
    "os"
    "sync/atomic"
    "time"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
    topic := flag.String("topic", "", "topic")
    start := flag.Int("start", 0, "starting sequence")
    count := flag.Int("count", 0, "message count")
    partitions := flag.Int("partitions", 1, "partition count")
    flag.Parse()

    if *topic == "" || *count <= 0 || *partitions <= 0 {
        flag.Usage()
        os.Exit(2)
    }

    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers":            "127.0.0.1:9092",
        "queue.buffering.max.messages": 200000,
        "batch.num.messages":           10000,
        "linger.ms":                    20,
        "compression.type":             "lz4",
    })
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    var failed atomic.Int64
    done := make(chan struct{})
    go func() {
        for event := range producer.Events() {
            if message, ok := event.(*kafka.Message); ok && message.TopicPartition.Error != nil {
                failed.Add(1)
            }
        }
        close(done)
    }()

    started := time.Now()
    for i := 0; i < *count; i++ {
        sequence := *start + i
        partition := int32(sequence % *partitions)
        value := []byte(fmt.Sprintf(
            `{"sequence":%d,"partition":%d,"name":"load-%08d","payload":"abcdefghijklmnopqrstuvwxyz0123456789"}`,
            sequence, partition, sequence,
        ))

        for {
            err = producer.Produce(&kafka.Message{
                TopicPartition: kafka.TopicPartition{
                    Topic: topic,
                    Partition: partition,
                },
                Key:   []byte(fmt.Sprintf("key-%d", sequence)),
                Value: value,
            }, nil)
            if err == nil {
                break
            }
            kafkaErr, ok := err.(kafka.Error)
            if !ok || kafkaErr.Code() != kafka.ErrQueueFull {
                fmt.Fprintln(os.Stderr, err)
                os.Exit(1)
            }
            time.Sleep(10 * time.Millisecond)
        }
    }

    remaining := producer.Flush(60000)
    producer.Close()
    <-done

    elapsed := time.Since(started)
    produced := *count - remaining
    fmt.Printf(
        "produced=%d remaining=%d failed=%d elapsed=%s rate=%.0f msg/s\n",
        produced, remaining, failed.Load(), elapsed,
        float64(produced)/elapsed.Seconds(),
    )
    if remaining != 0 || failed.Load() != 0 {
        os.Exit(1)
    }
}
EOF

gofmt -w /tmp/e2e_producer.go
```

## 9. 启动 bend-ingest-kafka

```bash
cd "$REPO"
go build -o /tmp/bend-ingest-kafka-e2e .

rm -f /tmp/bend-ingest-kafka-e2e.log /tmp/bend-ingest-kafka-e2e.pid
nohup /tmp/bend-ingest-kafka-e2e \
  -f /tmp/bend-ingest-kafka-e2e.json \
  >/tmp/bend-ingest-kafka-e2e.log 2>&1 &

echo $! >/tmp/bend-ingest-kafka-e2e.pid
```

等待 metrics endpoint：

```bash
until curl -fsS http://127.0.0.1:22112/metrics >/dev/null; do
  if ! kill -0 "$(cat /tmp/bend-ingest-kafka-e2e.pid)" 2>/dev/null; then
    cat /tmp/bend-ingest-kafka-e2e.log
    exit 1
  fi
  sleep 1
done
```

初始只有 2 partitions、4 个 workers，因此日志中两个 workers 分配到 partition，另外
两个 workers 收到空 assignment 是预期行为：

```bash
grep 'kafka_rebalance' /tmp/bend-ingest-kafka-e2e.log
```

## 10. 第一阶段：向 2 partitions 写入 1 万条

```bash
cd "$REPO"
go run /tmp/e2e_producer.go \
  -topic "$TOPIC" \
  -start 0 \
  -count 10000 \
  -partitions 2
```

成功输出应满足：

```text
produced=10000 remaining=0 failed=0 ...
```

查询 Databend 行数：

```bash
curl -fsS -u databend:databend \
  -H 'Content-Type: application/json' \
  -d "{\"sql\":\"SELECT count(*) FROM default.$TABLE\"}" \
  http://127.0.0.1:8002/v1/query
```

等待结果达到 `10000` 后再继续扩容。

检查当前 consumer lag：

```bash
docker exec kafka /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group "$GROUP"
```

第一阶段验收标准：

- Databend 行数为 `10000`。
- partitions `0` 和 `1` 的 lag 都为 `0`。
- metrics 中 ingest errors 为 `0`。

## 11. 在线扩容到 8 partitions

记录扩容开始时间并修改 topic：

```bash
date '+alter_started=%Y-%m-%dT%H:%M:%S%z'

docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic "$TOPIC" \
  --partitions 8
```

观察 rebalance 日志：

```bash
tail -f /tmp/bend-ingest-kafka-e2e.log | grep --line-buffered 'kafka_rebalance'
```

预期在 metadata refresh 和 group coordination 完成后看到：

- 原 partitions 被 revoked。
- 新 assignment 覆盖 partitions `0–7`。
- 4 个 workers 最终各分配约 2 个 partitions。

示例：

```text
Partitions assigned: [...[0] ...[1]]
Partitions assigned: [...[2] ...[3]]
Partitions assigned: [...[4] ...[5]]
Partitions assigned: [...[6] ...[7]]
```

按 `Ctrl+C` 只停止 `tail -f`，不要停止 bend-ingest-kafka。

> `topicMetadataRefreshIntervalMs=5000` 不代表一定在 5 秒内完成全部 rebalance，
> metadata refresh、group coordination 和 assignment 都需要时间。本机实测约 10–13 秒，
> 建议以 30 秒内覆盖全部新 partitions 作为本地验收标准。

## 12. 第二阶段：向 8 partitions 写入 9 万条

```bash
cd "$REPO"
go run /tmp/e2e_producer.go \
  -topic "$TOPIC" \
  -start 10000 \
  -count 90000 \
  -partitions 8
```

成功输出应满足：

```text
produced=90000 remaining=0 failed=0 ...
```

等待 Databend 总行数达到 `100000`：

```bash
while true; do
  RESPONSE=$(curl -fsS -u databend:databend \
    -H 'Content-Type: application/json' \
    -d "{\"sql\":\"SELECT count(*) FROM default.$TABLE\"}" \
    http://127.0.0.1:8002/v1/query)
  echo "$RESPONSE"
  echo "$RESPONSE" | grep -q '"100000"' && break
  sleep 2
done
```

## 13. 最终验收

### 13.1 校验总数、唯一性和 sequence 范围

```bash
curl -fsS -u databend:databend \
  -H 'Content-Type: application/json' \
  -d "{\"sql\":\"SELECT count(*) AS total_rows, count(DISTINCT raw_data['sequence']::UInt64) AS distinct_sequences, min(raw_data['sequence']::UInt64) AS min_sequence, max(raw_data['sequence']::UInt64) AS max_sequence FROM default.$TABLE\"}" \
  http://127.0.0.1:8002/v1/query
```

预期结果：

```text
total_rows        = 100000
distinct_sequences = 100000
min_sequence      = 0
max_sequence      = 99999
```

这可以证明没有数据丢失或重复。

### 13.2 校验 Databend partition 分布

```bash
curl -fsS -u databend:databend \
  -H 'Content-Type: application/json' \
  -d "{\"sql\":\"SELECT kpartition, count(*) AS rows, min(koffset), max(koffset) FROM default.$TABLE GROUP BY kpartition ORDER BY kpartition\"}" \
  http://127.0.0.1:8002/v1/query
```

使用本文档中的生产方式时，预期分布为：

```text
partition 0: 16250
partition 1: 16250
partition 2: 11250
partition 3: 11250
partition 4: 11250
partition 5: 11250
partition 6: 11250
partition 7: 11250
```

partitions `2–7` 有数据，证明在线扩容后新增 partitions 已被消费。

### 13.3 校验 Kafka lag

```bash
docker exec kafka /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group "$GROUP"
```

验收标准：partitions `0–7` 的 `LAG` 全部为 `0`。

### 13.4 校验应用 metrics 和错误日志

```bash
curl -fsS http://127.0.0.1:22112/metrics \
  | grep -E '^bend_ingest_kafka_ingest_(rows|errors)_total '

grep -E 'level=(error|fatal)|panic|ingest failed|streaming load failed' \
  /tmp/bend-ingest-kafka-e2e.log
```

验收标准：

- `bend_ingest_kafka_ingest_rows_total 100000`
- `bend_ingest_kafka_ingest_errors_total 0`
- 错误日志查询没有输出

### 13.5 可选：记录容器资源使用

```bash
docker stats --no-stream \
  --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}' \
  kafka docker-databend-1
```

## 14. 本机实测基线

2026-07-22 在 macOS arm64、Docker Desktop 环境完成一次验证：

- 第一阶段：10,000 条，生产约 `33.9k msg/s`。
- topic 从 2 扩容到 8 partitions 后，约 10–13 秒完成新 assignment。
- 第二阶段：90,000 条，生产约 `137k msg/s`。
- Databend 最终行数：`100000`。
- 唯一 sequence：`100000`。
- sequence 范围：`0–99999`。
- partitions `0–7` 最终 lag：全部为 `0`。
- ingest errors：`0`。

该数据仅作为本地回归参考。性能会受到 Docker 配额、磁盘、Kafka 状态和 Databend
镜像版本影响，不应作为严格性能门槛。

## 15. 清理

### 15.1 停止测试消费者

```bash
if [ -f /tmp/bend-ingest-kafka-e2e.pid ]; then
  kill -TERM "$(cat /tmp/bend-ingest-kafka-e2e.pid)" 2>/dev/null || true
fi
```

### 15.2 删除测试 topic

```bash
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic "$TOPIC"
```

### 15.3 删除 Databend 测试表

```bash
curl -fsS -u databend:databend \
  -H 'Content-Type: application/json' \
  -d "{\"sql\":\"DROP TABLE IF EXISTS default.$TABLE\"}" \
  http://127.0.0.1:8002/v1/query
```

### 15.4 删除临时文件

```bash
rm -f \
  /tmp/bend-ingest-kafka-e2e \
  /tmp/bend-ingest-kafka-e2e.json \
  /tmp/bend-ingest-kafka-e2e.log \
  /tmp/bend-ingest-kafka-e2e.pid \
  /tmp/e2e_producer.go
```

### 15.5 可选：停止基础服务

如果后续不再测试：

```bash
cd "$KAFKA_COMPOSE_DIR"
docker compose down

cd "$DATABEND_COMPOSE_DIR"
docker compose down
```

不要添加 `-v`，除非明确需要同时删除 Kafka 和 Databend 的持久化数据。
