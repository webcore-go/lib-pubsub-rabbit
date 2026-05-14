package rabbitmq

import (
	"context"

	"github.com/webcore-go/webcore/port"
)

type RabbitMLoader struct {
	name string
}

func (a *RabbitMLoader) SetName(name string) {
	a.name = name
}

func (a *RabbitMLoader) Name() string {
	return a.name
}

func (l *RabbitMLoader) Init(args ...any) (port.Library, error) {
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
