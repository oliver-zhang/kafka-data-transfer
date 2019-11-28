package transfer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
)

var messageCountStart int

func SyncMsg(brokerList []string, topic string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	master, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()
	partitions, err := master.Partitions(topic)
	if err != nil {
		panic(err)
	}
	for partition := range partitions {
		consumer, err := master.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		doneCh := make(chan struct{})
		defer consumer.AsyncClose()
		go func(sarama.PartitionConsumer) {
			for {
				select {
				case err := <-consumer.Errors():
					fmt.Println(err)
				case msg := <-consumer.Messages():
					SendMsgToKafka(topic, string(msg.Value))
					fmt.Println("Received messages", string(msg.Key), string(msg.Value))
					messageCountStart++
				case <-signals:
					fmt.Println("Interrupt is detected")
					doneCh <- struct{}{}

				}
			}
		}(consumer)
		<-doneCh
		fmt.Println("Processed", messageCountStart, "messages")
	}

}
