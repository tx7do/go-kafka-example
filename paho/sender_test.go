package paho

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-kafka-example/event"
	"testing"
)

const Topic = "topic/bobo/#"

func TestKafkaSender(t *testing.T) {
	sender, err := NewKafkaSender([]string{"tcp://emqx:public@broker.emqx.io:1883"})
	assert.Nil(t, err)

	for i := 0; i < 50; i++ {
		assert.Nil(t, send(sender))
	}

	err = sender.Close()
	assert.Nil(t, err)
}

func send(sender event.Sender) error {
	msg := event.NewMessage("", []byte("hello world\n"))
	err := sender.Send(context.Background(), "topic/bobo/1", msg)
	if err != nil {
		return err
	}
	fmt.Printf("key:%s, value:%s\n", msg.GetKey(), msg.GetValue())
	return nil
}
