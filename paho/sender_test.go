package paho

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-kafka-example/event"
	"testing"
)

const (
	EmqxBroker        = "tcp://broker.emqx.io:1883"
	EmqxCnBroker      = "tcp://broker-cn.emqx.io:1883"
	EclipseBroker     = "tcp://mqtt.eclipseprojects.io:1883"
	MosquittoBroker   = "tcp://test.mosquitto.org:1883"
	HiveMQBroker      = "tcp://broker.hivemq.com:1883"
	LocalEmxqBroker   = "tcp://127.0.0.1:1883"
	LocalRabbitBroker = "tcp://user:bitnami@127.0.0.1:1883"
)

const Topic = "topic/bobo/#"

func TestMqttSender(t *testing.T) {
	sender, err := NewMqttSender([]string{LocalRabbitBroker})
	assert.Nil(t, err)

	for i := 0; i < 50; i++ {
		assert.Nil(t, send(sender, i))
	}

	err = sender.Close()
	assert.Nil(t, err)
}

func send(sender event.Sender, num int) error {
	topic := "topic/bobo/1"
	msg := event.NewMessage(topic, []byte(fmt.Sprintf("[%d]hello world", num)))
	err := sender.Send(context.Background(), topic, msg)
	if err != nil {
		return err
	}
	fmt.Printf("key:%s, value:%s\n", msg.GetKey(), msg.GetValue())
	return nil
}
