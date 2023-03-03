# bend-ingest-kafka

Ingest kafka data into databend

## Installation

```shell
go get https://databendcloud/bend-ingest-kafka
```

## Usage

```shell
bend-ingest-kafka
  --kafka-bootstrap-servers="127.0.0.1:9092,127.0.0.2:9092"\
  --kafka-topic="Your Topic"\
  --kafka-consumer-group= "consumer group"\
  --databend-dsn="http://root:root@127.0.0.1:8000"\
  --databend-table="db1.tbl" \
  --data-format=json 
  --batch-size=100000 \
  --batch-max-interval=300s
```