package producer

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stevecshanks/db-updates-to-domain-events.git/stock-notifier/stock"
)

type fakeWriter struct {
	Written []kafka.Message
}

func (fw *fakeWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	fw.Written = append(fw.Written, msgs...)
	return nil
}

func TestWriteNotificationWritesToKafkaWriter(t *testing.T) {
	writer := fakeWriter{}
	producer := New(&writer)

	notification := stock.Notification{Type: stock.OutOfStock, ProductID: 123, Quantity: 5}

	err := producer.WriteNotification(context.Background(), notification)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(writer.Written) != 1 {
		t.Fatalf("Unexpected number of messages written")
	}
	lastMessage := string(writer.Written[0].Value)
	if lastMessage != `{"type":"OutOfStock","product_id":123,"quantity":5}` {
		t.Fatalf("Unexpected message written: " + lastMessage)
	}
}
