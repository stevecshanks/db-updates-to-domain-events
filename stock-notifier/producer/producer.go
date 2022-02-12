package producer

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"github.com/stevecshanks/db-updates-to-domain-events.git/stock-notifier/stock"
)

type notificationMessage struct {
	Type      string `json:"type"`
	ProductID int    `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type kafkaWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type producer struct {
	writer kafkaWriter
}

// WriteNotification writes a single notification to the Kafka writer
func (p producer) WriteNotification(ctx context.Context, notification stock.Notification) error {
	msg := notificationMessage{
		Type:      notification.Type.String(),
		ProductID: notification.ProductID,
		Quantity:  notification.Quantity,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{Value: b})
	if err != nil {
		return err
	}

	return nil
}

// New creates a new producer using the provided Kafka writer
func New(writer kafkaWriter) producer {
	return producer{writer}
}
