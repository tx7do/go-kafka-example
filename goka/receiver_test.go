package goka

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-kafka-example/event"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

func TestKafkaReceiver(t *testing.T) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	receiver, err := NewKafkaReceiver([]string{"localhost:9092"}, "a-group", []string{Topic})
	assert.Nil(t, err)

	assert.Nil(t, receive(receiver))

	<-sigs
	err = receiver.Close()
	assert.Nil(t, err)
}

func receive(receiver event.Receiver) error {
	fmt.Println("start receiver")
	err := receiver.Receive(context.Background(), func(ctx context.Context, msg event.Event) error {
		fmt.Printf("key:%s, value:%s\n", msg.GetKey(), msg.GetValue())
		return nil
	})
	return err
}
