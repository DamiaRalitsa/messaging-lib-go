package common

import (
	"context"
	"encoding/json"
	"time"
)

type Message struct {
	ID         string
	Topic      string
	Key        string
	Payload    json.RawMessage
	Status     string
	RetryCount int
	CreatedAt  time.Time
}

type MessageHandler interface {
	Dispatch(ctx context.Context, message Message) error
}
