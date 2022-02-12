package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
	"github.com/stevecshanks/db-updates-to-domain-events.git/stock-notifier/consumer"
	"github.com/stevecshanks/db-updates-to-domain-events.git/stock-notifier/producer"
	"github.com/stevecshanks/db-updates-to-domain-events.git/stock-notifier/stock"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   "dbserver1.inventory.products_on_hand",
	})
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Printf("Error closing reader: %v", err)
		}
	}()

	writer := &kafka.Writer{
		Addr:  kafka.TCP("kafka:9092"),
		Topic: "stock-notifications",
	}
	defer func() {
		err := writer.Close()
		if err != nil {
			log.Printf("Error closing writer: %v", err)
		}
	}()

	consumer := consumer.New(reader)
	producer := producer.New(writer)
	notifier := stock.NewNotifier(consumer, producer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-shutdown
		log.Println("Gracefully shutting down...")
		cancel()
	}()

	err := notifier.Run(ctx)
	if err != nil {
		log.Printf("Error from notifier: %v", err)
	}

	log.Println("Done!")
}
