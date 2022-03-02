package goka

import (
	"context"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/tx7do/go-kafka-example/event"
	"log"
)

type kafkaReceiver struct {
	reader  *goka.Processor
	handler event.Handler
}

func (k *kafkaReceiver) Receive(ctx context.Context, handler event.Handler) error {
	k.handler = handler
	go func() {
		if err := k.reader.Run(ctx); err != nil {
			log.Fatalf("error running processor: %v", err)
		} else {
			log.Printf("Processor shutdown cleanly")
		}
	}()
	return nil
}

func (k *kafkaReceiver) Close() error {
	return nil
}

func NewKafkaReceiver(address []string, groupID string, topics []string) (event.Receiver, error) {
	r := &kafkaReceiver{reader: nil}

	g := goka.DefineGroup(goka.Group(groupID),
		goka.Input(goka.Stream(topics[0]), new(codec.String), r.receive),
		goka.Persist(new(codec.Int64)),
	)

	p, err := goka.NewProcessor(address, g)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}

	r.reader = p

	return r, nil
}

func (k *kafkaReceiver) receive(ctx goka.Context, msg interface{}) {
	var counter int64
	if val := ctx.Value(); val != nil {
		counter = val.(int64)
	}
	counter++
	ctx.SetValue(counter)

	log.Printf("key = %s, counter = %v, msg = %v", ctx.Key(), counter, msg)
	err := k.handler(context.Background(), event.NewMessage(ctx.Key(), msg.([]byte)))
	if err != nil {
		log.Fatal("message handling exception:", err)
	}
}
