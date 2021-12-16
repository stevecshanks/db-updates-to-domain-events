package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type stockNotification struct {
	Type      string `json:"type"`
	ProductID int    `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

func processMessages(consumer *ProductsOnHandConsumer, producer *StockNotificationProducer) {
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

			err = producer.WriteMessage(ctx, notification)
			if err != nil {
				log.Println("Error writing notification: " + err.Error())
			}
		}
	}
}

func main() {
	consumer := NewProductsOnHandConsumer()
	producer := NewStockNotificationProducer()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	go func() {
		<-shutdown

		log.Println("Gracefully shutting down...")

		if err := consumer.Close(); err != nil {
			log.Println("Failed to close consumer: ", err)
		}
		if err := producer.Close(); err != nil {
			log.Println("Failed to close producer: ", err)
		}

		done <- true
	}()

	processMessages(consumer, producer)

	<-done

	log.Println("Done!")
}
