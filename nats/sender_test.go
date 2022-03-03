package nats

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-kafka-example/event"
	"testing"
)

const Topic = "test_topic"

func TestNatsSender(t *testing.T) {
	sender, err := NewNatsSender([]string{"nats://127.0.0.1:4222"})
	assert.Nil(t, err)

	for i := 0; i < 50; i++ {
		assert.Nil(t, send(sender))
	}

	err = sender.Close()
	assert.Nil(t, err)
}

func send(sender event.Sender) error {
	msg := event.NewMessage("", []byte("hello world\n"))
	err := sender.Send(context.Background(), "test_topic", msg)
	if err != nil {
		return err
	}
	fmt.Printf("key:%s, value:%s\n", msg.GetKey(), msg.GetValue())
	return nil
}
