package kafka

import (
	"context"
)

type Base interface {
	// Start initialize and start the producer or consumer
	Start(context context.Context) error

	// Stop stops the producer or consumer
	Stop(context context.Context) error
}

type Producer interface {
	Base

	//Write writes a `message` to `topic`'s `partition`
	Write(ctx context.Context, topic string, partitionKey []byte, value []byte) error

}

type Consumer interface {
	Base

	//Read reads the message
	Read(ctx context.Context) (<- chan *Message, error)
}
