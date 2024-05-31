package message

import (
	"context"
	"time"
)

type MessagesBatch struct {
	Messages           []MessageData
	Partition          int
	CommitFunc         func(context.Context) error
	FirstMessageOffset int64
	LastMessageOffset  int64
	Key                string
	CreateTime         time.Time
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
