package paho

import (
	"context"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/tx7do/go-kafka-example/event"
	"time"
)

type kafkaSender struct {
	writer mqtt.Client
}

func (s *kafkaSender) Send(_ context.Context, topic string, message event.Event) error {
	token := s.writer.Publish(topic, 0, false, message.GetValue())
	token.Wait()
	return nil
}

func (s *kafkaSender) Close() error {
	s.writer.Disconnect(250)
	return nil
}

func NewKafkaSender(address []string) (event.Sender, error) {
	opts := mqtt.NewClientOptions().AddBroker(address[0])

	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(1 * time.Second)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	return &kafkaSender{writer: c}, nil
}
