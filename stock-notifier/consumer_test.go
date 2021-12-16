package main

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

type testReader struct {
	CloseCalled bool
	Message     kafka.Message
}

func (s *testReader) Close() error {
	s.CloseCalled = true
	return nil
}

func (s *testReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	return s.Message, nil
}

func TestReadMessageReturnsStockUpdates(t *testing.T) {
	consumer := ProductsOnHandConsumer{&testReader{
		Message: kafka.Message{Value: []byte(`{
			"payload": {
				"before": {
					"product_id": 123,
					"quantity": 5
				},
				"after" : {
					"product_id": 456,
					"quantity": 10
				}
			}
		}`)},
	}}

	msg, err := consumer.ReadMessage(context.Background())

	if err != nil {
		t.Fatalf("Unexpected error: " + err.Error())
	}

	expected := stockUpdateMessage{
		Payload: stockPayload{
			Before: &stockStatus{
				ProductID: 123,
				Quantity:  5,
			},
			After: &stockStatus{
				ProductID: 456,
				Quantity:  10,
			},
		},
	}

	if msg.Payload.Before.ProductID != expected.Payload.Before.ProductID ||
		msg.Payload.Before.Quantity != expected.Payload.Before.Quantity ||
		msg.Payload.After.ProductID != expected.Payload.After.ProductID ||
		msg.Payload.After.Quantity != expected.Payload.After.Quantity {
		t.Fatalf("Unexpected message")
	}
}

func TestCloseCallsCloseOnTheReader(t *testing.T) {
	reader := &testReader{}
	consumer := ProductsOnHandConsumer{reader}

	err := consumer.Close()

	if err != nil {
		t.Fatalf("Unexpected error: " + err.Error())
	}

	if !reader.CloseCalled {
		t.Fatalf("Reader was not closed")
	}
}
