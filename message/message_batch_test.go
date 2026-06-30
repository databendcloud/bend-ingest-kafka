package message

import (
	"context"
	"testing"
	"time"

	"github.com/test-go/testify/assert"
)

func TestMessagesBatch_Empty(t *testing.T) {
	var nilBatch *MessagesBatch
	assert.True(t, nilBatch.Empty())

	emptyBatch := &MessagesBatch{}
	assert.True(t, emptyBatch.Empty())

	nonEmptyBatch := &MessagesBatch{
		Messages: []MessageData{{Data: "test"}},
	}
	assert.False(t, nonEmptyBatch.Empty())
}

func TestMessagesBatch_ExtractMessageData(t *testing.T) {
	batch := &MessagesBatch{
		Messages: []MessageData{
			{Data: `{"id":1}`},
			{Data: `{"id":2}`},
			{Data: `{"id":3}`},
		},
	}

	data := batch.ExtractMessageData()
	assert.Equal(t, 3, len(data))
	assert.Equal(t, `{"id":1}`, data[0])
	assert.Equal(t, `{"id":2}`, data[1])
	assert.Equal(t, `{"id":3}`, data[2])
}

func TestMessageData_Fields(t *testing.T) {
	now := time.Now()
	msg := MessageData{
		Data:       `{"name":"test"}`,
		DataOffset: 42,
		Partition:  3,
		Key:        "mykey",
		CreateTime: now,
	}

	assert.Equal(t, `{"name":"test"}`, msg.Data)
	assert.Equal(t, int64(42), msg.DataOffset)
	assert.Equal(t, 3, msg.Partition)
	assert.Equal(t, "mykey", msg.Key)
	assert.Equal(t, now, msg.CreateTime)
}

func TestMessagesBatch_CommitFunc(t *testing.T) {
	called := false
	batch := &MessagesBatch{
		Messages: []MessageData{{Data: "x"}},
		CommitFunc: func(ctx context.Context) error {
			called = true
			return nil
		},
		FirstMessageOffset: 10,
		LastMessageOffset:  20,
	}

	err := batch.CommitFunc(context.Background())
	assert.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, int64(10), batch.FirstMessageOffset)
	assert.Equal(t, int64(20), batch.LastMessageOffset)
}
