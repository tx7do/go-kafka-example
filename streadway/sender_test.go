package streadway

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-kafka-example/event"
	"testing"
)

const Topic = "test_topic"

func TestAmqpSender(t *testing.T) {
	sender, err := NewAmqpSender([]string{"amqp://user:bitnami@127.0.0.1:5672"})
	assert.Nil(t, err)

	for i := 0; i < 50; i++ {
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
