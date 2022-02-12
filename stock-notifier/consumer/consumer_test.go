package consumer

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

type fakeReader struct {
	Message kafka.Message
}

func (fr fakeReader) ReadMessage(context.Context) (kafka.Message, error) {
	return fr.Message, nil
}

func TestReadUpdateReadsFromKafkaStream(t *testing.T) {
	consumer := New(fakeReader{
		Message: kafka.Message{Value: []byte(`{
			"payload": {
				"before": {
					"product_id": 123,
					"quantity": 5
				},
				"after" : {
					"product_id": 123,
					"quantity": 10
				}
			}
		}`)},
	})

	update, err := consumer.ReadUpdate(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if update.ProductID != 123 {
		t.Errorf("Incorrect product ID %d", update.ProductID)
	}
	if update.OldQuantity == nil {
		t.Errorf("Incorrect old quantity %d", update.OldQuantity)
	} else if *update.OldQuantity != 5 {
		t.Errorf("Incorrect old quantity %d", *update.OldQuantity)
	}
	if update.NewQuantity == nil {
		t.Errorf("Incorrect new quantity %d", update.NewQuantity)
	} else if *update.NewQuantity != 10 {
		t.Errorf("Incorrect new quantity %d", *update.NewQuantity)
	}
}

func TestReadUpdateHandlesMissingBefore(t *testing.T) {
	consumer := New(fakeReader{
		Message: kafka.Message{Value: []byte(`{
			"payload": {
				"after" : {
					"product_id": 123,
					"quantity": 10
				}
			}
		}`)},
	})

	update, err := consumer.ReadUpdate(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if update.ProductID != 123 {
		t.Errorf("Incorrect product ID %d", update.ProductID)
	}
	if update.OldQuantity != nil {
		t.Errorf("Incorrect old quantity %d", *update.OldQuantity)
	}
	if update.NewQuantity == nil {
		t.Errorf("Incorrect new quantity %d", update.NewQuantity)
	} else if *update.NewQuantity != 10 {
		t.Errorf("Incorrect new quantity %d", *update.NewQuantity)
	}
}

func TestReadUpdateHandlesMissingAfter(t *testing.T) {
	consumer := New(fakeReader{
		Message: kafka.Message{Value: []byte(`{
			"payload": {
				"before": {
					"product_id": 123,
					"quantity": 5
				}
			}
		}`)},
	})

	update, err := consumer.ReadUpdate(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if update.ProductID != 123 {
		t.Errorf("Incorrect product ID %d", update.ProductID)
	}
	if update.OldQuantity == nil {
		t.Errorf("Incorrect old quantity %d", update.OldQuantity)
	} else if *update.OldQuantity != 5 {
		t.Errorf("Incorrect old quantity %d", *update.OldQuantity)
	}
	if update.NewQuantity != nil {
		t.Errorf("Incorrect new quantity %d", *update.NewQuantity)
	}
}

func TestReadUpdateReturnsErrorForBlankPayload(t *testing.T) {
	consumer := New(fakeReader{
		Message: kafka.Message{Value: []byte(`{"payload": {}}`)},
	})

	_, err := consumer.ReadUpdate(context.Background())
	if err == nil {
		t.Fatalf("Expected error but was nil")
	}
}

func TestReadUpdateReturnsErrorIfProductIDChanges(t *testing.T) {
	consumer := New(fakeReader{
		Message: kafka.Message{Value: []byte(`{
			"payload": {
				"before": {
					"product_id": 123,
					"quantity": 1
				},
				"after" : {
					"product_id": 456,
					"quantity": 1
				}
			}
		}`)},
	})

	_, err := consumer.ReadUpdate(context.Background())
	if err == nil {
		t.Fatalf("Expected error but was nil")
	}
}

func TestReadUpdateReturnsNilForTombstoneMessages(t *testing.T) {
	consumer := New(fakeReader{
		Message: kafka.Message{},
	})

	update, err := consumer.ReadUpdate(context.Background())
	if err != nil {
		t.Errorf("Expected no error but was %v", err)
	}
	if update != nil {
		t.Errorf("Expected no update but was %v", update)
	}
}
