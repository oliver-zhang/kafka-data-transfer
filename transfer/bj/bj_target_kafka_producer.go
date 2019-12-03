package bj

import (
	"fmt"
	"github.com/Shopify/sarama"
)

var config = sarama.NewConfig()
var producer *sarama.SyncProducer
var err error

func InitKafkaProducer(maxRetry int, brokenList []string) (producer sarama.SyncProducer) {
	if producer != nil {
		return producer
	}
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true
	producer, err = sarama.NewSyncProducer(brokenList, config)
	if err != nil {
		panic(err)
	}
	return producer
}

func BjProductEnvKafkaProducer(topic string, originMsg string, producer sarama.SyncProducer) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(originMsg),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Message is store in topic(%s)/partition(%d)/offset(%d)\n ", topic, partition, offset)
}
