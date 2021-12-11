package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	kafka "github.com/segmentio/kafka-go"
)

type stockStatus struct {
	ProductID int `json:"product_id"`
	Quantity  int
}

type stockPayload struct {
	Before *stockStatus
	After  *stockStatus
}

type stockUpdateMessage struct {
	Payload stockPayload
}

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

		var stockUpdate stockUpdateMessage
		err = json.Unmarshal(msg.Value, &stockUpdate)
		if err != nil {
			log.Println("Could not parse message: " + err.Error())
		}

		if stockUpdate.Payload.Before == nil || stockUpdate.Payload.After == nil {
			log.Println("Message was for create/delete, ignoring...")
			continue
		}

		if stockUpdate.Payload.Before.Quantity == stockUpdate.Payload.After.Quantity {
			log.Println("Message was not for quantity change, ignoring...")
			continue
		}

		log.Printf("Product with ID %d had quantity changed from %d to %d", stockUpdate.Payload.Before.ProductID, stockUpdate.Payload.Before.Quantity, stockUpdate.Payload.After.Quantity)
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
