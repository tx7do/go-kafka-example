package paho

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/tx7do/go-kafka-example/event"
	"log"
	"os"
	"time"
)

type kafkaReceiver struct {
	reader  mqtt.Client
	topic   string
	qos     byte
	handler event.Handler
}

func (k *kafkaReceiver) Receive(_ context.Context, handler event.Handler) error {
	k.handler = handler
	if token := k.reader.Subscribe(k.topic, k.qos, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	return nil
}

func (k *kafkaReceiver) Close() error {
	k.reader.Disconnect(250)
	return nil
}

func NewKafkaReceiver(address []string, _ string, topics []string) (event.Receiver, error) {
	r := &kafkaReceiver{reader: nil, qos: 0, topic: topics[0]}

	opts := mqtt.NewClientOptions().AddBroker(address[0])

	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(1 * time.Second)

	opts.SetDefaultPublishHandler(r.receive)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	r.reader = c

	return r, nil
}

func (k *kafkaReceiver) receive(_ mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())

	err := k.handler(nil, event.NewMessage(msg.Topic(), msg.Payload()))
	if err != nil {
		log.Fatal("message handling exception:", err)
	}
}
