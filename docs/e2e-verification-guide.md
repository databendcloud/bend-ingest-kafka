# bend-ingest-kafka 端到端验证指南

本文档描述如何在本地验证 Kafka 数据通过 bend-ingest-kafka 写入 Databend 的完整流程。

## 前置条件

- Docker & Docker Compose
- Go 1.23+
- 本地 Databend 实例（默认 `http://localhost:8002`）

## 1. 启动 Kafka

```bash
cd /Users/hanshanjie/databend/kafka-demo
docker compose up -d
```

等待 Kafka 健康就绪（约 30 秒）：

```bash
docker compose ps
```

确认 `kafka` 容器状态为 `healthy`。

Kafka 对外暴露端口 `localhost:9092`，AKHQ 管理界面在 `http://localhost:12080/kafkaprodhk`。

## 2. 创建 Kafka Topic

在项目目录下运行：

```bash
cd /Users/hanshanjie/git-works/bend-ingest-kafka
go run scripts/create_topic.go
```

输出 `Topic demo-ingest: Success` 即创建成功。

## 3. 在 Databend 中创建目标表

连接 Databend 执行：

```bash
curl -u databend:databend "http://localhost:8002/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE IF NOT EXISTS default.demo_kafka_ingest (name String, age Int32, city String)"}'
```

## 4. 准备配置文件

创建 `config/demo.json`：

```json
{
  "kafkaBootstrapServers": "localhost:9092",
  "kafkaTopic": "demo-ingest",
  "KafkaConsumerGroup": "demo-group",
  "isSASL": false,
  "disableTLS": true,
  "isJsonTransform": true,
  "databendDSN": "http://databend:databend@localhost:8002",
  "databendTable": "default.demo_kafka_ingest",
  "batchSize": 10,
  "batchMaxInterval": 5,
  "dataFormat": "json",
  "workers": 1,
  "copyPurge": false,
  "copyForce": false,
  "disableVariantCheck": false,
  "minBytes": 1024,
  "maxBytes": 10485760,
  "maxWait": 10,
  "copyIntoUploadCompression": true,
  "maxRetryDelay": 1800
}
```

## 5. 启动 bend-ingest-kafka

编译并启动：

```bash
cd /Users/hanshanjie/git-works/bend-ingest-kafka

go build -o bend-ingest-kafka .

./bend-ingest-kafka -f config/demo.json
```

程序会阻塞等待 Kafka 消息。

## 6. 向 Kafka 写入测试数据

打开另一个终端，运行项目自带的生产者脚本，向 topic 写入 10 万条测试数据：

```bash
cd /Users/hanshanjie/git-works/bend-ingest-kafka
go run scripts/produce_demo.go
```

输出类似：
```
Produced 10000/100000 messages...
Produced 20000/100000 messages...
...
Done! Sent 100000 messages in 3.07s (0 unsent)
```

## 7. 观察 bend-ingest-kafka 日志

回到 bend-ingest-kafka 终端，应看到类似输出：

```
time="..." level=info msg="Batch complete" batch_size=5 ...
time="..." level=info msg="upload to stage @~/batch/..., cost: ..."
time="..." level=info msg="copy into default.demo_kafka_ingest, cost: ..."
2026/06/30 consume 5 rows (... rows/s), ... bytes (... bytes/s) in ... ms
2026/06/30 process 5 rows (... rows/s) in ... ms
```

## 8. 验证 Databend 中的数据

查询总行数：

```bash
curl -u databend:databend "http://localhost:8002/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT count(*) FROM default.demo_kafka_ingest"}'
```

预期返回 `100000`（与 produce_demo.go 发送的数量一致）。

查看部分数据：

```bash
curl -u databend:databend "http://localhost:8002/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM default.demo_kafka_ingest LIMIT 5"}'
```

预期返回类似：

```json
{"data":[["Alice","30","Beijing"],["Bob","25","Shanghai"],["Charlie","35","Shenzhen"],["David","28","Hangzhou"],["Eve","32","Guangzhou"]]}
```

> 注意：`isJsonTransform=true` 模式下，Kafka 消息的 JSON key 需要与目标表的列名完全匹配。
> 表必须提前手动创建（步骤 3），程序不会自动建表。

## 9. 持续写入验证

再次运行生产者脚本验证持续消费能力：

```bash
go run scripts/produce_demo.go
```

等待几秒后再次查询：

```bash
curl -u databend:databend "http://localhost:8002/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT count(*) FROM default.demo_kafka_ingest"}'
```

应返回 `200000`（两次各 10 万条）。

## 10. 清理

停止 bend-ingest-kafka（Ctrl+C），然后清理资源：

```bash
# 删除 Databend 测试表
curl -u databend:databend "http://localhost:8002/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "DROP TABLE IF EXISTS default.demo_kafka_ingest"}'

# 停止 Kafka
cd /Users/hanshanjie/databend/kafka-demo
docker compose down
```
