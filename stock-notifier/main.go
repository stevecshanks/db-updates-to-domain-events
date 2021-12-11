package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	kafka "github.com/segmentio/kafka-go"
)

func processMessages(r *kafka.Reader) {
	ctx := context.Background()

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Println("Could not read message: " + err.Error())

		}
		log.Println("Received message: " + string(msg.Value))
	}
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   "dbserver1.inventory.products_on_hand",
	})

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	go func() {
		<-shutdown
		log.Println("Gracefully shutting down...")
		if err := r.Close(); err != nil {
			log.Println("Failed to close reader: ", err)
		}
		log.Println("Closed reader")
		done <- true
	}()

	processMessages(r)

	<-done
	log.Println("Done!")
}
