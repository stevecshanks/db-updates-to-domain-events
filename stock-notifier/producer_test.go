package main

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

type testWriter struct {
	CloseCalled bool
	LastWritten []kafka.Message
}

func (s *testWriter) Close() error {
	s.CloseCalled = true
	return nil
}

func (w *testWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.LastWritten = msgs
	return nil
}

func TestStockNotificationsAreWrittenToWriter(t *testing.T) {
	writer := &testWriter{}
	producer := StockNotificationProducer{writer}

	notification := stockNotification{Type: "OutOfStock", ProductID: 123, Quantity: 5}

	err := producer.WriteMessage(context.Background(), notification)

	if err != nil {
		t.Fatalf("Unexpected error: " + err.Error())
	}

	if len(writer.LastWritten) != 1 {
		t.Fatalf("Unexpected number of messages written")
	}
	lastMessage := string(writer.LastWritten[0].Value)
	if lastMessage != `{"type":"OutOfStock","product_id":123,"quantity":5}` {
		t.Fatalf("Unexpected message written: " + lastMessage)
	}
}

func TestCloseCallsCloseOnTheWriter(t *testing.T) {
	writer := &testWriter{}
	producer := StockNotificationProducer{writer}

	err := producer.Close()

	if err != nil {
		t.Fatalf("Unexpected error: " + err.Error())
	}

	if !writer.CloseCalled {
		t.Fatalf("Writer was not closed")
	}
}
