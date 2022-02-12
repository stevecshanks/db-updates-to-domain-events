package consumer

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/segmentio/kafka-go"
	"github.com/stevecshanks/db-updates-to-domain-events.git/stock-notifier/stock"
)

type updateState struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

type updatePayload struct {
	Before *updateState `json:"before"`
	After  *updateState `json:"after"`
}

type updateMessage struct {
	Payload updatePayload `json:"payload"`
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

	if isTombstone(kafkaMessage) {
		return nil, nil
	}

	var message updateMessage
	err = json.Unmarshal(kafkaMessage.Value, &message)
	if err != nil {
		return nil, err
	}

	update, err := createUpdate(message)
	if err != nil {
		return nil, err
	}

	return update, nil
}

func isTombstone(kafkaMessage kafka.Message) bool {
	return len(kafkaMessage.Value) == 0
}

func createUpdate(message updateMessage) (*stock.Update, error) {
	if message.Payload.Before == nil && message.Payload.After == nil {
		return nil, errors.New("invalid message: payload is empty")
	}
	if message.Payload.Before != nil && message.Payload.After != nil && message.Payload.Before.ProductID != message.Payload.After.ProductID {
		return nil, errors.New("invalid message: product ids do not match")
	}

	update := stock.Update{}
	if message.Payload.Before != nil {
		update.ProductID = message.Payload.Before.ProductID
		update.OldQuantity = &message.Payload.Before.Quantity
	}
	if message.Payload.After != nil {
		update.ProductID = message.Payload.After.ProductID
		update.NewQuantity = &message.Payload.After.Quantity
	}

	return &update, nil
}

// New creates a new consumer using the provided Kafka reader
func New(reader kafkaReader) consumer {
	return consumer{reader}
}
