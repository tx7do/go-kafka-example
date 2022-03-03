package streadway

import (
	"context"
	"github.com/streadway/amqp"
	"github.com/tx7do/go-kafka-example/event"
)

type amqpSender struct {
	writer *amqp.Channel
}

func (s *amqpSender) Send(_ context.Context, topic string, message event.Event) error {
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

func (s *amqpSender) Close() error {
	err := s.writer.Close()
	if err != nil {
		return err
	}
	return nil
}

func NewAmqpSender(address []string) (event.Sender, error) {
	conn, err := amqp.Dial(address[0])
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &amqpSender{writer: channel}, nil
}
