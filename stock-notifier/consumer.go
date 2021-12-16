package main

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
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

type kafkaReader interface {
	Close() error
	ReadMessage(context.Context) (kafka.Message, error)
}

type ProductsOnHandConsumer struct {
	reader kafkaReader
}

func (c *ProductsOnHandConsumer) ReadMessage(ctx context.Context) (*stockUpdateMessage, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	var stockUpdate stockUpdateMessage
	err = json.Unmarshal(msg.Value, &stockUpdate)
	if err != nil {
		return nil, err
	}

	return &stockUpdate, nil
}

func (c *ProductsOnHandConsumer) Close() error {
	return c.reader.Close()
}

func NewProductsOnHandConsumer() *ProductsOnHandConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   "dbserver1.inventory.products_on_hand",
	})

	return &ProductsOnHandConsumer{reader: r}
}
