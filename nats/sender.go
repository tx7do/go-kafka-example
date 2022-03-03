package nats

import (
	"context"
	"errors"
	nats "github.com/nats-io/nats.go"
	"github.com/tx7do/go-kafka-example/event"
)

type natsSender struct {
	writer *nats.Conn
}

func (s *natsSender) Send(_ context.Context, topic string, message event.Event) error {
	if s.writer == nil {
		return errors.New("not connected")
	}

	return s.writer.Publish(topic, message.GetValue())
}

func (s *natsSender) Close() error {
	s.writer.Close()
	return nil
}

func NewNatsSender(address []string) (event.Sender, error) {
	opts := nats.GetDefaultOptions()

	opts.Url = address[0]

	c, err := opts.Connect()
	if err != nil {
		return nil, err
	}

	return &natsSender{writer: c}, nil
}
