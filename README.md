# bend-ingest-kafka

Ingest kafka data into databend

## Installation

```shell
go get https://github.com/databendcloud/bend-ingest-kafka
```

## Usage

### Create a table according your kafka data structrue
For example, the kafka data like 

```json
{"i64": 10,"u64": 30,"f64": 20,"s": "hao","s2": "hello","a16":[1],"a8":[2],"d": "2011-03-06","t": "2016-04-04 11:30:00"}
```

you should create a table using 

``` SQL
CREATE TABLE test_ingest (
			i64 Int64,
			u64 UInt64,
			f64 Float64,
			s   String,
			s2  String,
			a16 Array(Int16),
			a8  Array(UInt8),
			d   Date,
			t   DateTime);
```
      

```shell
bend-ingest-kafka
  --kafka-bootstrap-servers="127.0.0.1:9092,127.0.0.2:9092"\
  --kafka-topic="Your Topic"\
  --kafka-consumer-group= "consumer group"\
  --databend-dsn="http://root:root@127.0.0.1:8000"\
  --databend-table="db1.tbl" \
  --data-format=json \
  --batch-size=100000 \
  --batch-max-interval=300s
```
