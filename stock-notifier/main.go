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

type stockNotification struct {
	Type      string
	ProductID int `json:"product_id"`
	Quantity  int
}

func processMessages(consumer *ProductsOnHandConsumer, w *kafka.Writer) {
	ctx := context.Background()

	for {
		stockUpdate, err := consumer.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Println("Error reading stock update: " + err.Error())
			continue
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

		if stockUpdate.Payload.After.Quantity == 0 {
			notification := stockNotification{Type: "OutOfStock", ProductID: stockUpdate.Payload.After.ProductID, Quantity: 0}
			b, err := json.Marshal(notification)
			if err != nil {
				log.Println("Error encoding notification: " + err.Error())
				continue
			}

			err = w.WriteMessages(ctx, kafka.Message{Value: b})
			if err != nil {
				log.Println("Error writing notification: " + err.Error())
			}
		}
	}
}

func main() {
	consumer := NewProductsOnHandConsumer()

	w := &kafka.Writer{
		Addr:  kafka.TCP("kafka:9092"),
		Topic: "stock-notifications",
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	go func() {
		<-shutdown

		log.Println("Gracefully shutting down...")

		if err := consumer.Close(); err != nil {
			log.Println("Failed to close consumer: ", err)
		}
		if err := w.Close(); err != nil {
			log.Println("Failed to close writer: ", err)
		}

		done <- true
	}()

	processMessages(consumer, w)

	<-done

	log.Println("Done!")
}
