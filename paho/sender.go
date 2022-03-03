package paho

import (
	"context"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/tx7do/go-kafka-example/event"
	"time"
)

type mqttSender struct {
	writer mqtt.Client
}

func (s *mqttSender) Send(_ context.Context, topic string, message event.Event) error {
	token := s.writer.Publish(topic, 0, false, message.GetValue())
	token.Wait()
	return nil
}

func (s *mqttSender) Close() error {
	s.writer.Disconnect(250)
	return nil
}

func NewMqttSender(address []string) (event.Sender, error) {
	opts := mqtt.NewClientOptions().AddBroker(address[0])

	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(1 * time.Second)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	return &mqttSender{writer: c}, nil
}
