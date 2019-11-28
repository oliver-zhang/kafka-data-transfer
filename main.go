package main

import (
	"github.com/kafka_data_transfer/transfer"
	"gopkg.in/alecthomas/kingpin.v2"
	"golang.org/x/net/proxy"
)

var (
	brokenList = kingpin.Flag("brokenList","List of brokers to connect").Strings()
	topic = kingpin.Flag("topic","the name of transfer topic").String()
	maxRetry   = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
)


func main() {
	kingpin.Parse()
	producer := transfer.InitKafkaProducer(*maxRetry,*brokenList)
	transfer.SyncMsg(*brokenList,*topic);
	defer func() {
		if err := producer.Close(); err!=nil {
			panic(err)
		}
	}()
}












