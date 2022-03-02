package streadway

import (
	"context"
	"github.com/streadway/amqp"
	"github.com/tx7do/go-kafka-example/event"
)

type kafkaSender struct {
	writer *amqp.Channel
}

func (s *kafkaSender) Send(_ context.Context, topic string, message event.Event) error {
	q, err := s.writer.QueueDeclare(
		topic,
		true, //durable
		false,
		false,
		false,
		nil,
	)

	err = s.writer.Publish("", q.Name, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message.GetValue(),
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
	conn, err := amqp.Dial(address[0])
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &kafkaSender{writer: channel}, nil
}
