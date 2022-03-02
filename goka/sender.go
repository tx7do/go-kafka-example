package goka

import (
	"context"
	"log"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/tx7do/go-kafka-example/event"
)

type EmitterMap map[string]*goka.Emitter

type kafkaSender struct {
	writers EmitterMap
	address []string
}

func (s *kafkaSender) Send(_ context.Context, topic string, message event.Event) error {
	writer, ok := s.writers[topic]
	if !ok {
		writer, err := goka.NewEmitter(s.address, goka.Stream(topic), new(codec.Bytes))
		if err != nil {
			log.Fatalf("error creating emitter: %v", err)
		}
		s.writers[topic] = writer
	}

	err := writer.EmitSync(message.GetKey(), message.GetValue())
	if err != nil {
		log.Fatalf("error emitting message: %v", err)
	}

	return nil
}

func (s *kafkaSender) Close() error {
	for _, writer := range s.writers {
		err := writer.Finish()
		return err
	}
	s.writers = EmitterMap{}
	return nil
}

func NewKafkaSender(address []string) (event.Sender, error) {
	return &kafkaSender{writers: EmitterMap{}, address: address}, nil
}
