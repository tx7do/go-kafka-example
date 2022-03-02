package sarama

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tx7do/go-kafka-example/event"
	"log"
)

const (
	assignor          = "range"
	channelBufferSize = 1000
	version           = "3.0.0"
)

type TopicPartitionMap map[string][]int32

type kafkaReceiver struct {
	reader  sarama.ConsumerGroup
	topics  []string
	handler event.Handler
}

func (k *kafkaReceiver) Receive(ctx context.Context, handler event.Handler) error {
	k.handler = handler
	go func() {
		for {
			if err := k.reader.Consume(ctx, k.topics, k); err != nil {
				// 当setup失败的时候，error会返回到这里
				log.Printf("Error from consumer: %v \n", err)
				return
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				log.Println(ctx.Err())
				return
			}
		}
	}()
	log.Println("Sarama consumer up and running!...")

	return nil
}

func (k *kafkaReceiver) Close() error {
	err := k.reader.Close()
	if err != nil {
		return err
	}
	return nil
}

func (k *kafkaReceiver) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("setup")

	fmt.Println(session.Claims())

	//for _, topic := range k.topics {
	//	session.ResetOffset(topic, 0, 0, "")
	//}

	return nil
}

func (k *kafkaReceiver) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("cleanup")
	return nil
}

func (k *kafkaReceiver) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("ConsumeClaim\n")
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	// 具体消费消息
	for msg := range claim.Messages() {
		fmt.Printf("[topic:%s] [partiton:%d] [offset:%d] [value:%s] [time:%v]\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value), msg.Timestamp)

		err := k.handler(context.Background(), event.NewMessage(string(msg.Key), msg.Value))
		if err != nil {
			log.Fatal("message handling exception:", err)
		}

		// 更新位移
		session.MarkMessage(msg, "")
	}
	return nil
}

func NewKafkaReceiver(address []string, groupId string, topics []string) (event.Receiver, error) {
	kafkaConf := sarama.NewConfig()
	kafkaConf.Version = sarama.V3_0_0_0
	kafkaConf.Consumer.Offsets.Initial = sarama.OffsetNewest
	kafkaConf.ChannelBufferSize = channelBufferSize // channel长度

	// 分区分配策略
	switch assignor {
	case "sticky":
		kafkaConf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		kafkaConf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		kafkaConf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	// 创建client
	newClient, err := sarama.NewClient(address, kafkaConf)
	if err != nil {
		log.Fatal(err)
	}

	// 根据client创建consumerGroup
	c, err := sarama.NewConsumerGroupFromClient(groupId, newClient)
	if err != nil {
		log.Fatalf("Error creating consumer group client: %v", err)
	}

	return &kafkaReceiver{reader: c, topics: topics}, nil
}
