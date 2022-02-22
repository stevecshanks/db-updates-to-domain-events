package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/stevecshanks/db-updates-to-domain-events.git/stock-notifier/stock"
)

type state struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

type messagePayload struct {
	Before *state `json:"before"`
	After  *state `json:"after"`
}

type message struct {
	Payload messagePayload `json:"payload"`
}

type keyPayload struct {
	ProductID int `json:"product_id"`
}

type key struct {
	Payload keyPayload
}

func (m message) Validate() error {
	if m.Payload.Before == nil && m.Payload.After == nil {
		return errors.New("payload is empty")
	}
	if m.Payload.Before != nil && m.Payload.After != nil && m.Payload.Before.ProductID != m.Payload.After.ProductID {
		return errors.New("product ids do not match")
	}

	return nil
}

type kafkaReader interface {
	ReadMessage(context.Context) (kafka.Message, error)
}

type consumer struct {
	reader kafkaReader
}

// ReadUpdate reads a single update from the Kafka reader. Note that the return value can be nil for Tombstone records
func (c consumer) ReadUpdate(ctx context.Context) (*stock.Update, error) {
	kafkaMessage, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	var k key
	err = json.Unmarshal(kafkaMessage.Key, &k)
	if err != nil {
		return nil, err
	}

	if isTombstone(kafkaMessage) {
		return &stock.Update{ProductID: k.Payload.ProductID}, nil
	}

	var m message
	err = json.Unmarshal(kafkaMessage.Value, &m)
	if err != nil {
		return nil, err
	}

	err = m.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid message: %w", err)
	}

	return createUpdate(k, m), nil
}

func isTombstone(kafkaMessage kafka.Message) bool {
	return len(kafkaMessage.Value) == 0
}

func createUpdate(k key, m message) *stock.Update {
	update := stock.Update{
		ProductID: k.Payload.ProductID,
	}
	if m.Payload.Before != nil {
		update.OldQuantity = &m.Payload.Before.Quantity
	}
	if m.Payload.After != nil {
		update.NewQuantity = &m.Payload.After.Quantity
	}

	return &update
}

// New creates a new consumer using the provided Kafka reader
func New(reader kafkaReader) consumer {
	return consumer{reader}
}
