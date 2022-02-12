package stock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

type fakeConsumer struct {
	queue    []interface{}
	queuePos int
}

func (fc *fakeConsumer) AddUpdate(update Update) {
	fc.queue = append(fc.queue, update)
}

func (fc *fakeConsumer) AddTombstone() {
	fc.queue = append(fc.queue, nil)
}

func (fc *fakeConsumer) AddError(err error) {
	fc.queue = append(fc.queue, err)
}

func (fc *fakeConsumer) ReadUpdate(context.Context) (*Update, error) {
	if fc.queuePos >= len(fc.queue) {
		return nil, io.EOF
	}
	next := fc.queue[fc.queuePos]
	fc.queuePos++

	switch t := next.(type) {
	case Update:
		return &t, nil
	case nil:
		return nil, nil
	case error:
		return nil, t
	default:
		return nil, fmt.Errorf("unknown type in queue: %v", t)
	}
}

type fakeProducer struct {
	NextError error
	Written   []Notification
}

func (fp *fakeProducer) WriteNotification(ctx context.Context, notification Notification) error {
	if err := fp.NextError; err != nil {
		fp.NextError = nil
		return err
	}
	fp.Written = append(fp.Written, notification)
	return nil
}

func intPtr(i int) *int {
	return &i
}

func TestNotifierWritesNotificationWhenProductGoesOutOfStock(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(10),
		NewQuantity: intPtr(0),
	})

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 1 {
		t.Fatalf("Expected 1 notification, got %d", len(producer.Written))
	}
	if producer.Written[0].Type != OutOfStock {
		t.Errorf("Incorrect type in notification: %s", producer.Written[0].Type)
	}
	if producer.Written[0].ProductID != 123 {
		t.Errorf("Incorrect product ID in notification: %d", producer.Written[0].ProductID)
	}
	if producer.Written[0].Quantity != 0 {
		t.Errorf("Incorrect quantityin notification: %d", producer.Written[0].Quantity)
	}
}

func TestNotifierWritesNotificationWhenProductIsBackInStock(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(0),
		NewQuantity: intPtr(10),
	})

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 1 {
		t.Fatalf("Expected 1 notification, got %d", len(producer.Written))
	}
	if producer.Written[0].Type != BackInStock {
		t.Errorf("Incorrect type in notification: %s", producer.Written[0].Type)
	}
	if producer.Written[0].ProductID != 123 {
		t.Errorf("Incorrect product ID in notification: %d", producer.Written[0].ProductID)
	}
	if producer.Written[0].Quantity != 10 {
		t.Errorf("Incorrect quantityin notification: %d", producer.Written[0].Quantity)
	}
}

func TestNotifierDoesNotWriteNotificationForStillInStockProduct(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(10),
		NewQuantity: intPtr(5),
	})

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 0 {
		t.Fatalf("Expected 0 notifications, got %d", len(producer.Written))
	}
}

func TestNotifierDoesNotWriteNotificationForNewProduct(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	consumer.AddUpdate(Update{
		ProductID:   123,
		NewQuantity: intPtr(0),
	})

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 0 {
		t.Fatalf("Expected 0 notifications, got %d", len(producer.Written))
	}
}

func TestNotifierDoesNotWriteNotificationForDeletedProduct(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(0),
	})
	consumer.AddTombstone()

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 0 {
		t.Fatalf("Expected 0 notifications, got %d", len(producer.Written))
	}
}

func TestNotifierDoesNotWriteNotificationForUnchangedProduct(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(0),
		NewQuantity: intPtr(0),
	})

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 0 {
		t.Fatalf("Expected 0 notifications, got %d", len(producer.Written))
	}
}

func TestNotifierCanWriteMultipleNotifications(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(10),
		NewQuantity: intPtr(0),
	})
	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(10),
		NewQuantity: intPtr(0),
	})

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 2 {
		t.Fatalf("Expected 2 notifications, got %d", len(producer.Written))
	}
}

func TestNotifierContinuesAfterConsumerError(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	consumer.AddError(errors.New("something bad happened"))
	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(10),
		NewQuantity: intPtr(0),
	})

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 1 {
		t.Fatalf("Expected 1 notification, got %d", len(producer.Written))
	}
}

func TestNotifierContinuesAfterProducerError(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{NextError: errors.New("something bad happened")}
	notifier := NewNotifier(consumer, producer)

	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(10),
		NewQuantity: intPtr(0),
	})
	consumer.AddUpdate(Update{
		ProductID:   123,
		OldQuantity: intPtr(10),
		NewQuantity: intPtr(0),
	})

	err := notifier.Run(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(producer.Written) != 1 {
		t.Fatalf("Expected 1 notification, got %d", len(producer.Written))
	}
}

func TestNotifierExitsAfterContextCancellation(t *testing.T) {
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}
	notifier := NewNotifier(consumer, producer)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := notifier.Run(ctx)

	if err != context.Canceled {
		t.Fatalf("Expected cancellation error, got %v", err)
	}
}
