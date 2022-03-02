package confluent

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tx7do/go-kafka-example/event"
)

type kafkaSender struct {
	writer *kafka.Producer
}

func (s *kafkaSender) Send(_ context.Context, topic string, message event.Event) error {

	err := s.writer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(message.GetKey()),
		Value:          message.GetValue(),
	}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *kafkaSender) Close() error {
	s.writer.Close()
	return nil
}

func NewKafkaSender(address []string) (event.Sender, error) {
	kafkaConf := &kafka.ConfigMap{
		"api.version.request":           "true",
		"message.max.bytes":             1000000,
		"linger.ms":                     500,
		"sticky.partitioning.linger.ms": 1000,
		"retries":                       INT32_MAX,
		"retry.backoff.ms":              1000,
		"acks":                          "1",
	}
	err := kafkaConf.SetKey("bootstrap.servers", address)
	if err != nil {
		return nil, err
	}
	err = kafkaConf.SetKey("security.protocol", "plaintext")
	if err != nil {
		return nil, err
	}

	p, err := kafka.NewProducer(kafkaConf)
	if err != nil {
		panic(err)
	}

	return &kafkaSender{writer: p}, nil
}
