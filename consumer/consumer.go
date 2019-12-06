package main

import (
	"fmt"
	"strings"
	"sync"
	"xdg"

	"github.com/Shopify/sarama"
	"github.com/xdg/scram"
)

var (
	//change into your config
	kafkaConn = "140.92.27.17:31390"               // kafka host
	topic     = "bid"                              // which topic to write
	user      = "iid"                              // user id
	password  = "4bdfead6021c51c7b270a37e60564b1f" // user pws

	wg sync.WaitGroup
)

func getConfig() *sarama.Config {
	// create config
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Net.SASL.Enable = true //important
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdg.XDGSCRAMClient{HashGeneratorFcn: scram.SHA256} }
	config.Net.SASL.Mechanism = "SCRAM-SHA-256"
	config.Net.SASL.User = user
	config.Net.SASL.Password = password
	config.Version = sarama.V2_2_0_0 // not a must
	config.Net.SASL.Handshake = true // not a must
	return config
}

func main() {
	//craete consumer
	consumer, err := sarama.NewConsumer(strings.Split(kafkaConn, ","), getConfig())
	if err != nil {
		fmt.Printf("Failed to start consumer: %s", err)
		return
	}
	//Partitions returns the sorted list of all partition IDs for the given topic.
	//here, get all partitions of the topic
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Println("Failed to get the list of partitions: ", err)
		return
	}
	fmt.Println("partitionList:", partitionList)
	//loop all partitions
	for partition := range partitionList {
		// ConsumePartition creates a PartitionConsumer on the given topic/partition with
		// the given offset. It will return an error if this Consumer is already consuming
		// on the given topic/partition. Offset can be a literal offset, or OffsetNewest
		// or OffsetOldest
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("Failed to start consumer for partition %d: %s\n", partition, err)
			return
		}
		defer pc.AsyncClose()
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d, Offset:%d, Key:%s, Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				fmt.Println()
			}

		}(pc)
	}
	//time.Sleep(time.Hour)
	wg.Wait()
	consumer.Close()
}
