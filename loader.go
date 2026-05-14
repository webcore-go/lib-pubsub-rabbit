package rabbitmq

import (
	"context"

	"github.com/webcore-go/webcore/port"
)

type RabbitMQLoader struct {
	name string
}

func (a *RabbitMQLoader) SetName(name string) {
	a.name = name
}

func (a *RabbitMQLoader) Name() string {
	return a.name
}

func (l *RabbitMQLoader) Init(args ...any) (port.Library, error) {
	ctx := args[0].(context.Context)
	cfg := args[1].(RabbitMQConfig)

	rmq, err := NewRabbitMQ(ctx, cfg)
	if err != nil {
		return nil, err
	}

	err = rmq.Install(args...)
	if err != nil {
		return nil, err
	}

	return rmq, nil
}
