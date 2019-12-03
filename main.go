package main

import (
	"github.com/oliver-zhang/kafka-data-transfer/transfer"
	"github.com/oliver-zhang/kafka-data-transfer/transfer/bj"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	configFile = kingpin.Flag("configFile", "config file").File()
	maxRetry   = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
)

func main() {
	kingpin.Parse()
	config, err := transfer.LoadLog4xKafkaSyncConfig(*configFile)
	if err != nil {
		panic(err)
	}
	producerBrokenList := []string{config.Kafka.Target}
	consumerBrokenList := []string{config.Kafka.Origin}
	producer := bj.InitKafkaProducer(*maxRetry, producerBrokenList)
	bj.BjTestenvKafkaConsumer(consumerBrokenList, producer, config)
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}

	}()
}
