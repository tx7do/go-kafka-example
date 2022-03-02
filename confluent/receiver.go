package confluent

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tx7do/go-kafka-example/event"
	"log"
)

const (
	INT32_MAX = 2147483647 - 1000
)

type kafkaReceiver struct {
	reader *kafka.Consumer
}

func (k *kafkaReceiver) Receive(ctx context.Context, handler event.Handler) error {
	go func() {
		for {
			msg, err := k.reader.ReadMessage(-1)
			if err != nil {
				break
			}

			err = handler(ctx, event.NewMessage(string(msg.Key), msg.Value))
			if err != nil {
				log.Fatal("message handling exception:", err)
			}
		}
	}()
	return nil
}

func (k *kafkaReceiver) Close() error {
	err := k.reader.Close()
	if err != nil {
		return err
	}
	return nil
}

func NewKafkaReceiver(address []string, groupID string, topics []string) (event.Receiver, error) {
	kafkaConf := &kafka.ConfigMap{
		"api.version.request":       "true",
		"auto.offset.reset":         "latest",
		"heartbeat.interval.ms":     3000,
		"session.timeout.ms":        30000,
		"max.poll.interval.ms":      120000,
		"fetch.max.bytes":           1024000,
		"max.partition.fetch.bytes": 256000,
	}
	err := kafkaConf.SetKey("bootstrap.servers", address)
	if err != nil {
		return nil, err
	}
	err = kafkaConf.SetKey("security.protocol", "plaintext")
	if err != nil {
		return nil, err
	}
	err = kafkaConf.SetKey("group.id", groupID)
	if err != nil {
		return nil, err
	}

	c, err := kafka.NewConsumer(kafkaConf)
	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}

	return &kafkaReceiver{reader: c}, nil
}
