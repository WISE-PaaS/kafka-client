package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"xdg"

	"github.com/Shopify/sarama"
	"github.com/xdg/scram"
)

const (
	//change into your config
	kafkaConn = "140.92.27.17:31390"               // kafka host
	topic     = "bid"                              // which topic to write
	user      = "iid"                              // user id
	password  = "4bdfead6021c51c7b270a37e60564b1f" // user pws
)

func main() {
	// create producer
	producer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	// read command line input
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter msg: ")
		msg, _ := reader.ReadString('\n')

		// publish without goroutene
		publish(msg, producer)

		// publish with go routene
		// go publish(msg, producer)
	}
}

func getConfig() *sarama.Config {
	// set config
	config := sarama.NewConfig()
	config.ClientID = "go-kafka-producer"
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Net.SASL.Enable = true //important
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdg.XDGSCRAMClient{HashGeneratorFcn: scram.SHA256} }
	config.Net.SASL.Mechanism = "SCRAM-SHA-256"
	config.Net.SASL.User = user
	config.Net.SASL.Password = password
	// config.Version = sarama.V2_2_0_0 // not a must
	// config.Net.SASL.Handshake = true // not a must
	return config
}

// set producer config, attention to credential
func initProducer() (sarama.SyncProducer, error) {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	// getConfig
	config := getConfig()

	// async producer
	//prd, err := sarama.NewAsyncProducer([]string{kafkaConn}, config)

	// sync producer
	prd, err := sarama.NewSyncProducer([]string{kafkaConn}, config)
	return prd, err
}

// push message
func publish(message string, producer sarama.SyncProducer) {
	// publish sync
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}
	// publish async
	// producer.Input() <- &sarama.ProducerMessage{}
	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)
}
