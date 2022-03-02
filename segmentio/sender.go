package segmentio

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/tx7do/go-kafka-example/event"
)

type kafkaSender struct {
	writer *kafka.Writer
}

func (s *kafkaSender) Send(ctx context.Context, topic string, message event.Event) error {
	err := s.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(message.GetKey()),
		Value: message.GetValue(),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *kafkaSender) Close() error {
	err := s.writer.Close()
	if err != nil {
		return err
	}
	return nil
}

func NewKafkaSender(address []string) (event.Sender, error) {
	w := &kafka.Writer{
		Addr:     kafka.TCP(address...),
		Balancer: &kafka.LeastBytes{},
	}
	return &kafkaSender{writer: w}, nil
}
