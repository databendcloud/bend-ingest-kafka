# bend-ingest-kafka

Ingest kafka data into databend

# Installation

```shell
go install  github.com/databendcloud/bend-ingest-kafka@latest
```

Or download the binary from the [release page](https://github.com/databendcloud/bend-ingest-kafka/releases).

```go
bend-ingest-kafka --version
```

# Usage

## Json transform mode

The json transform mode is the default mode which will transform the kafka data into databend table, you can use it by setting the `--is-json-transform` to `true`.
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
      
### execute bend-ingest-kafka

#### command line mode
```shell
bend-ingest-kafka
  --kafka-bootstrap-servers="127.0.0.1:9092,127.0.0.2:9092"\
  --kafka-topic="Your Topic"\
  --kafka-consumer-group= "Consumer Group"\
  --databend-dsn="databend://user:password@localhost:8000/default?sslmode=disable"\
  --databend-table="db1.tbl" \
  --data-format="json" \
  --batch-size=100000 \
  --batch-max-interval=300
```

#### config file mode
Config the config file `config/conf.json`
```json
{
  "kafkaBootstrapServers": "localhost:9092",
  "kafkaTopic": "ingest_test",
  "KafkaConsumerGroup": "test",
  "isJsonTransform": true,
  "databendDSN": "databend://user:password@localhost:8000/default?sslmode=disable",
  "databendTable": "default.kfk_test",
  "batchSize": 1,
  "batchMaxInterval": 5,
  "dataFormat": "json",
  "workers": 1
}
```

and execute the command
```shell
./bend-ingest-kafka 
```

## Raw mode
The raw mode is used to ingest the raw data into databend table, you can use it by setting the `isJsonTransform` to `false`.
In this mode, we will create a table with the name `databendTable` which columns are `(uuid, koffset,kpartition, raw_data, record_metadata, add_time)` and ingest the raw data into this table.
The `record_metadata` is the metadata of the kafka record which contains the `topic`, `partition`, `offset`, `create_time`, `key`, and the `add_time` is the time when the record is added into databend.

### Example
If the kafka json data is:
```json
{"i64": 10,"u64": 30,"f64": 20,"s": "hao","s2": "hello","a16":[1],"a8":[2],"d": "2011-03-06","t": "2016-04-04 11:30:00"}
```
run the command
```shell
./bend-ingest-kafka 
```

with `config/conf.json` and the table `default.kfk_test` will be created and the data will be ingested into this table.

![](https://files.mdnice.com/user/4760/2e8b0267-5694-43b5-9992-316280b4594f.png)


## Parameter References
| Parameter             | Description               | Default           | example                         |
|-----------------------|---------------------------|-------------------|---------------------------------|
| kafkaBootstrapServers | kafka bootstrap servers   | "127.0.0.1:64103" | "127.0.0.1:9092,127.0.0.2:9092" |
| kafkaTopic            | kafka topic               | "test"            | "test"                          |
| KafkaConsumerGroup    | kafka consumer group      | "kafka-bend-ingest" | "test"                          |
|isSASL                 | is sasl                   | false             | true                            |
|saslUser               | sasl user                 | ""                | "user"                          |
|saslPassword           | sasl password             | ""                | "password"                      |
|disableTLS             | disable TLS encryption    | false             | true                            |
| mockData              | mock data                 | ""                | ""                              |
| isJsonTransform       | is json transform         | true              | true                            |
| databendDSN           | databend dsn              | no                | "databend://user:password@localhost:8000/default?sslmode=disable"         |
| databendTable         | databend table            | no                | "db1.tbl"                       |
| batchSize             | batch size                | 1000              | 1000                            |
| batchMaxInterval      | batch max interval (seconds)       | 30                  | 30                              |
| dataFormat            | data format               | json              | "json"                          |
| workers               | workers thread number     | 1                 | 1                               |
| copyPurge             | copy purge                | false             | false                           |
| copyForce             | copy force                | false             | false                           |
| DisableVariantCheck   | disable variant check     | false             | false                           |
| MinBytes              | min bytes                 | 1024              | 1024                            |
| MaxBytes              | max bytes                 | 1048576           | 1048576                         |
| MaxWait               | max wait time (seconds)   | 10                | 10                              |
| useReplaceMode       | use replace mode          | false             | false                           |
| userStage             | user external stage name  | ~                 | ~                               |
| maxRetryDelay         | max retry delay (seconds) | 1800              | 1800                            |

## Kafka Security Protocols

### SASL_SSL (Recommended for Production)
- Set `isSASL: true` and `disableTLS: false` (or omit disableTLS)
- Credentials and data encrypted in transit
- Default configuration

**Example:**
```json
{
  "isSASL": true,
  "saslUser": "username",
  "saslPassword": "password",
  "disableTLS": false
}
```

### SASL_PLAINTEXT (Internal Networks Only)
- Set `isSASL: true` and `disableTLS: true`
- **WARNING**: Credentials transmitted without encryption
- Only use in trusted internal networks

**Example:**
```json
{
  "isSASL": true,
  "saslUser": "username",
  "saslPassword": "password",
  "disableTLS": true
}
```

**NOTE:**
- The `copyPurge and copyForce` are used to delete the data in the target table before ingesting the data. More details please refer to [copy](https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table#copy-options).
- The `useReplaceMode` is used to replace the data in the table, if the data already exists in the table, the new data will replace the old data. But the `useReplaceMode` is only supported when `isJsonTransform` false because it needs to add `koffset` and `kpartition` field in the target table.