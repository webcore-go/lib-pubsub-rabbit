package rabbitmq

import "github.com/webcore-go/webcore/infra/config"

type RabbitMQConfig struct {
	// config.PubSubConfig

	// Exchange      string `mapstructure:"exchange"` // Sama dengan Topic
	// Queue         string `mapstructure:"queue"` // sama dengan Subscription

	Subscription  string                `mapstructure:"subscription"`
	Topic         string                `mapstructure:"topic"`
	Uri           string                `mapstructure:"uri"`           // amqp://user:pass@host:port/vhost
	ExchangeType  string                `mapstructure:"exchange_type"` // direct, fanout, topic, headers, none (tanpa exchage = message queue biasa)
	Durable       bool                  `mapstructure:"durable"`
	AutoDelete    bool                  `mapstructure:"auto_delete"`
	Exclusive     bool                  `mapstructure:"exclusive"`
	PrefetchCount int                   `mapstructure:"prefetch_count"`
	Producer      config.ProducerConfig `mapstructure:"producer"`
}

func (c *RabbitMQConfig) SetEnvBindings() map[string]string {
	return map[string]string{
		"rabbitmq.topic":               "RABBITMQ_TOPIC",
		"rabbitmq.subscription":        "RABBITMQ_SUBSCRIPTION",
		"rabbitmq.uri":                 "RABBITMQ_URI",
		"rabbitmq.exchange_type":       "RABBITMQ_EXCHANGE_TYPE",
		"rabbitmq.durable":             "RABBITMQ_DURABLE",
		"rabbitmq.auto_delete":         "RABBITMQ_AUTO_DELETE",
		"rabbitmq.exclusive":           "RABBITMQ_EXCLUSIVE",
		"rabbitmq.prefetch_count":      "RABBITMQ_PREFETCH_COUNT",
		"rabbitmq.producer.attributes": "PUBSUB_PRODUCER_ATTRIBUTES",
	}
}

func (c *RabbitMQConfig) SetDefaults() map[string]any {
	return map[string]any{
		"rabbitmq.topic":               "",
		"rabbitmq.subscription":        "",
		"rabbitmq.uri":                 "",
		"rabbitmq.exchange_type":       "fanout",
		"rabbitmq.durable":             true,
		"rabbitmq.auto_delete":         false,
		"rabbitmq.exclusive":           false,
		"rabbitmq.prefetch_count":      0,
		"rabbitmq.producer.attributes": make(map[string]string),
	}
}
