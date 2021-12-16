package main

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type kafkaWriter interface {
	Close() error
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type StockNotificationProducer struct {
	writer kafkaWriter
}

func (p *StockNotificationProducer) Close() error {
	return p.writer.Close()
}

func (p *StockNotificationProducer) WriteMessage(ctx context.Context, notification stockNotification) error {
	b, err := json.Marshal(notification)
	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{Value: b})
	if err != nil {
		return err
	}

	return nil
}

func NewStockNotificationProducer() *StockNotificationProducer {
	writer := &kafka.Writer{
		Addr:  kafka.TCP("kafka:9092"),
		Topic: "stock-notifications",
	}

	return &StockNotificationProducer{writer}
}
