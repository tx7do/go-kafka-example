package sarama

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/tx7do/go-kafka-example/event"
	"time"
)

type kafkaSender struct {
	writer sarama.SyncProducer
}

func (s *kafkaSender) Send(_ context.Context, topic string, message event.Event) error {

	_, _, err := s.writer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Key:       sarama.StringEncoder(message.GetKey()),
		Value:     sarama.ByteEncoder(message.GetValue()),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *kafkaSender) Close() error {
	err := s.writer.Close()
	return err
}

func NewKafkaSender(address []string) (event.Sender, error) {
	kafkaConf := sarama.NewConfig()
	kafkaConf.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConf.Producer.Partitioner = sarama.NewRandomPartitioner
	kafkaConf.Producer.Return.Successes = true
	kafkaConf.Producer.Return.Errors = true
	kafkaConf.Producer.Timeout = 5 * time.Second
	kafkaConf.Version = sarama.V3_0_0_0

	p, err := sarama.NewSyncProducer(address, kafkaConf)
	if err != nil {
		return nil, err
	}

	return &kafkaSender{writer: p}, nil
}
