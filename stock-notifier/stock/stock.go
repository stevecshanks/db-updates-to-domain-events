package stock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
)

// UpdateType is an enumeration representing the different types of updates you might receive from a consumer
type UpdateType int

const (
	Uncategorized UpdateType = iota
	BackInStock
	OutOfStock
	Tombstone
)

// String converts the UpdateType to a human-readable string
func (ut UpdateType) String() string {
	switch ut {
	case BackInStock:
		return "BackInStock"
	case OutOfStock:
		return "OutOfStock"
	case Tombstone:
		return "Tombstone"
	default:
		return "Uncategorized"
	}
}

// Update represents a change in a product's quantity - the old/new quantities may be nil if the product is new/deleted
type Update struct {
	ProductID   int
	OldQuantity *int
	NewQuantity *int
}

// Type returns the UpdateType that this Update represents
func (u *Update) Type() UpdateType {
	if u == nil {
		return Tombstone
	}
	if u.OldQuantity != nil && u.NewQuantity != nil {
		if *u.OldQuantity == 0 && *u.NewQuantity > 0 {
			return BackInStock
		}
		if *u.OldQuantity > 0 && *u.NewQuantity == 0 {
			return OutOfStock
		}
	}
	return Uncategorized
}

// Notification represents a notification of something interesting happening to a product - e.g. going out of stock
type Notification struct {
	Type      UpdateType
	ProductID int
	Quantity  int
}

type updateConsumer interface {
	ReadUpdate(context.Context) (*Update, error)
}

type notificationProducer interface {
	WriteNotification(context.Context, Notification) error
}

type notifier struct {
	consumer updateConsumer
	producer notificationProducer
}

// Run will read messages from the consumer and write interesting notifications to the producer until the context is cancelled
func (n notifier) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := n.processNextUpdate(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			log.Printf("Error: %s\n", err)
		}
	}
}

func (n notifier) processNextUpdate(ctx context.Context) error {
	update, err := n.consumer.ReadUpdate(ctx)
	if err != nil {
		return fmt.Errorf("error from consumer: %w", err)
	}

	switch update.Type() {
	case BackInStock, OutOfStock:
		err = n.producer.WriteNotification(ctx, Notification{
			ProductID: update.ProductID,
			Quantity:  *update.NewQuantity,
			Type:      update.Type(),
		})
		if err != nil {
			return fmt.Errorf("error from producer: %w", err)
		}
	default:
		log.Printf("Ignored update of type: %s\n", update.Type().String())
	}

	return nil
}

// NewNotifier creates a new Notifier to connect the provided consumer and producer
func NewNotifier(consumer updateConsumer, producer notificationProducer) notifier {
	return notifier{consumer, producer}
}
