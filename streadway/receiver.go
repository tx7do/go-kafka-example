package streadway

import (
	"context"
	"github.com/streadway/amqp"
	"github.com/tx7do/go-kafka-example/event"
	"log"
)

type amqpReceiver struct {
	reader *amqp.Channel
	topic  string
}

func (k *amqpReceiver) Receive(ctx context.Context, handler event.Handler) error {
	delivery, err := k.reader.Consume(k.topic, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for m := range delivery {
			err = handler(ctx, event.NewMessage(k.topic, m.Body))
			if err != nil {
				log.Fatal("message handling exception:", err)
			}
		}
	}()
	return nil
}

func (k *amqpReceiver) Close() error {
	err := k.reader.Close()
	if err != nil {
		return err
	}
	return nil
}

func NewAmqpReceiver(address []string, _ string, topics []string) (event.Receiver, error) {
	conn, err := amqp.Dial(address[0])
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := channel.QueueDeclare(
		topics[0],
		true, //durable
		false,
		false,
		false,
		nil,
	)

	return &amqpReceiver{reader: channel, topic: q.Name}, nil
}
