package segmentio

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/tx7do/go-kafka-example/event"
	"log"
)

type kafkaReceiver struct {
	reader *kafka.Reader
}

func (k *kafkaReceiver) Receive(ctx context.Context, handler event.Handler) error {
	go func() {
		for {
			m, err := k.reader.FetchMessage(context.Background())
			if err != nil {
				break
			}
			err = handler(ctx, event.NewMessage(string(m.Key), m.Value))
			if err != nil {
				log.Fatal("message handling exception:", err)
			}
			if err := k.reader.CommitMessages(ctx, m); err != nil {
				log.Fatal("failed to commit messages:", err)
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
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     address,
		GroupID:     groupID,
		GroupTopics: topics,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
	})
	return &kafkaReceiver{reader: r}, nil
}
