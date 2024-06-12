package message

import (
	"context"
	"time"
)

type MessagesBatch struct {
	Messages           []MessageData
	CommitFunc         func(context.Context) error
	FirstMessageOffset int64
	LastMessageOffset  int64
}

type MessageData struct {
	Data       string
	DataOffset int64
	Partition  int
	Key        string
	CreateTime time.Time
}

func (b *MessagesBatch) Empty() bool {
	return b == nil || len(b.Messages) == 0
}

func (b *MessagesBatch) ExtractMessageData() []string {
	data := make([]string, 0, len(b.Messages))
	for _, m := range b.Messages {
		data = append(data, m.Data)
	}
	return data
}

type RecordForParquet struct {
	UUID           string `json:"uuid" parquet:"name=uuid, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	KOffset        int64  `json:"koffset" parquet:"name=koffset, type=INT64"`
	KPartition     int32  `json:"kpartition" parquet:"name=kpartition, type=INT32"`
	RawData        string `json:"raw_data" parquet:"name=raw_data, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RecordMetadata string `json:"record_metadata" parquet:"name=record_metadata, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AddTime        string `json:"add_time" parquet:"name=add_time, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}
