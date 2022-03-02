package event

import "context"

// copy from https://github.com/go-kratos/kratos/blob/main/examples/event/event/event.go

type Event interface {
	GetKey() string
	GetValue() []byte
}

type Handler func(context.Context, Event) error

type Sender interface {
	Send(ctx context.Context, topic string, msg Event) error
	Close() error
}

type Receiver interface {
	Receive(ctx context.Context, handler Handler) error
	Close() error
}
