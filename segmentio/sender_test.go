package segmentio

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-kafka-example/event"
	"testing"
)

const Topic = "test_topic"

func TestKafkaSender(t *testing.T) {
	sender, err := NewKafkaSender([]string{"localhost:9092"})
	assert.Nil(t, err)

	for i := 100; i < 150; i++ {
		assert.Nil(t, send(sender, i))
	}

	err = sender.Close()
	assert.Nil(t, err)
}

func send(sender event.Sender, num int) error {
	msg := event.NewMessage(Topic, []byte(fmt.Sprintf("[%d]hello world", num)))
	err := sender.Send(context.Background(), Topic, msg)
	if err != nil {
		return err
	}
	fmt.Printf("key:%s, value:%s\n", msg.GetKey(), msg.GetValue())
	return nil
}
