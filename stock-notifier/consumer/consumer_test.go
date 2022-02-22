package consumer

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/segmentio/kafka-go"
	"github.com/stevecshanks/db-updates-to-domain-events.git/stock-notifier/stock"
)

type fakeReader struct {
	Message kafka.Message
}

func (fr fakeReader) ReadMessage(context.Context) (kafka.Message, error) {
	return fr.Message, nil
}

func intPtr(i int) *int {
	return &i
}

func TestReadUpdateReadsFromKafkaStream(t *testing.T) {
	consumer := New(fakeReader{
		kafka.Message{
			Key: []byte(`{"payload":{"product_id":123}}`),
			Value: []byte(`{
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
			}`),
		},
	})

	update, err := consumer.ReadUpdate(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expected := &stock.Update{ProductID: 123, OldQuantity: intPtr(5), NewQuantity: intPtr(10)}
	if diff := cmp.Diff(expected, update); diff != "" {
		t.Errorf("Unexpcted update: %s", diff)
	}
}

func TestReadUpdateHandlesMissingBefore(t *testing.T) {
	consumer := New(fakeReader{
		kafka.Message{
			Key: []byte(`{"payload":{"product_id":123}}`),
			Value: []byte(`{
				"payload": {
					"after" : {
						"product_id": 123,
						"quantity": 10
					}
				}
			}`),
		},
	})

	update, err := consumer.ReadUpdate(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expected := &stock.Update{ProductID: 123, NewQuantity: intPtr(10)}
	if diff := cmp.Diff(expected, update); diff != "" {
		t.Errorf("Unexpcted update: %s", diff)
	}
}

func TestReadUpdateHandlesMissingAfter(t *testing.T) {
	consumer := New(fakeReader{
		kafka.Message{
			Key: []byte(`{"payload":{"product_id":123}}`),
			Value: []byte(`{
				"payload": {
					"before": {
						"product_id": 123,
						"quantity": 5
					}
				}
			}`),
		},
	})

	update, err := consumer.ReadUpdate(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expected := &stock.Update{ProductID: 123, OldQuantity: intPtr(5)}
	if diff := cmp.Diff(expected, update); diff != "" {
		t.Errorf("Unexpcted update: %s", diff)
	}
}

func TestReadUpdateHandlesTombstoneMessages(t *testing.T) {
	consumer := New(fakeReader{
		kafka.Message{
			Key: []byte(`{"payload":{"product_id":123}}`),
		},
	})

	update, err := consumer.ReadUpdate(context.Background())
	if err != nil {
		t.Errorf("Expected no error but was %v", err)
	}
	expected := &stock.Update{ProductID: 123}
	if diff := cmp.Diff(expected, update); diff != "" {
		t.Errorf("Unexpcted update: %s", diff)
	}
}

func TestReadUpdateReturnsErrorForBlankPayload(t *testing.T) {
	consumer := New(fakeReader{
		kafka.Message{
			Key:   []byte(`{"payload":{"product_id":123}}`),
			Value: []byte(`{"payload": {}}`),
		},
	})

	_, err := consumer.ReadUpdate(context.Background())
	if err == nil {
		t.Fatalf("Expected error but was nil")
	}
}

func TestReadUpdateReturnsErrorIfProductIDChanges(t *testing.T) {
	consumer := New(fakeReader{
		kafka.Message{
			Key: []byte(`{"payload":{"product_id":456}}`),
			Value: []byte(`{
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
			}`),
		},
	})

	_, err := consumer.ReadUpdate(context.Background())
	if err == nil {
		t.Fatalf("Expected error but was nil")
	}
}
