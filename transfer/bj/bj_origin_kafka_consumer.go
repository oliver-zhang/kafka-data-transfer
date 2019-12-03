package bj

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/oliver-zhang/kafka-data-transfer/transfer"
	"os"
	"os/signal"
	"strings"
	"sync"
)

var wgTopic sync.WaitGroup
var log4xConfig *transfer.Log4xKafkaSyncConfig

func BjTestenvKafkaConsumer(brokerList []string, producer sarama.SyncProducer, log4xConfig transfer.Log4xKafkaSyncConfig) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	topics, err := consumer.Topics()
	if err != nil {
		panic(err)
	}
	for _, topic := range topics {
		// fitter wang to sync topic
		if !filterTopics(topic, log4xConfig) {
			continue
		}

		partitions, err := consumer.Partitions(topic)
		if err != nil {
			panic(err)
		}

		for partition := range partitions {
			partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
			if err != nil {
				fmt.Println("topic: "+topic, "partition:"+string(partition))
				panic(err)
			}
			wgTopic.Add(1)
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)
			defer partitionConsumer.AsyncClose()
			go func(sarama.PartitionConsumer) {
				defer wgTopic.Done()
				for {
					select {
					case err := <-partitionConsumer.Errors():
						fmt.Println(err)
					case msg := <-partitionConsumer.Messages():
						fmt.Println("Received messages", string(msg.Key), string(msg.Value))
						BjProductEnvKafkaProducer(topic, string(msg.Value), producer)
					case <-signals:
						fmt.Println("Interrupt is detected")
						return

					}
				}
			}(partitionConsumer)
		}
	}
	wgTopic.Wait()
}

func filterTopics(topic string, log4xConfig transfer.Log4xKafkaSyncConfig) bool {
	includeTopics := log4xConfig.Kafka.KafkaIncludeTopics
	excludeTopics := log4xConfig.Kafka.KafkaExcludeTopics
	if includeTopics != "" && len(includeTopics) > 0 {
		includeTopicList := strings.Split(includeTopics, ",")
		for _, item := range includeTopicList {
			if strings.HasPrefix(topic, item) {
				return true
			}
		}
		return false
	}

	if excludeTopics != "" && len(excludeTopics) > 0 {
		excludeTopicList := strings.Split(includeTopics, ",")
		for _, item := range excludeTopicList {
			if strings.HasPrefix(topic, item) {
				return false
			}
		}
		return true
	}
	return true
}
