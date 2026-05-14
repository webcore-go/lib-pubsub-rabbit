package rabbitmq

import "github.com/webcore-go/webcore/infra/config"

type RabbitMQConfig struct {
	config.PubSubConfig

	// Exchange      string `mapstructure:"exchange"` // Sama dengan Topic
	// Queue         string `mapstructure:"queue"` // sama dengan Subscription

	Uri           string `mapstructure:"uri"`           // amqp://user:pass@host:port/vhost
	ExchangeType  string `mapstructure:"exchange_type"` // direct, fanout, topic, headers, none (tanpa exchage = message queue biasa)
	Durable       bool   `mapstructure:"durable"`
	AutoDelete    bool   `mapstructure:"auto_delete"`
	Exclusive     bool   `mapstructure:"exclusive"`
	PrefetchCount int    `mapstructure:"prefetch_count"`
}

func (c *RabbitMQConfig) SetEnvBindings() map[string]string {
	return map[string]string{
		"pubsub.topic":          "PUBSUB_TOPIC",
		"pubsub.subscription":   "PUBSUB_SUBSCRIPTION",
		"pubsub.uri":            "PUBSUB_URI",
		"pubsub.exchange_type":  "PUBSUB_EXCHANGE_TYPE",
		"pubsub.durable":        "PUBSUB_DURABLE",
		"pubsub.auto_delete":    "PUBSUB_AUTO_DELETE",
		"pubsub.exclusive":      "PUBSUB_EXCLUSIVE",
		"pubsub.prefetch_count": "PUBSUB_PREFETCH_COUNT",
	}
}

func (c *RabbitMQConfig) SetDefaults() map[string]any {
	return map[string]any{
		"pubsub.topic":          "",
		"pubsub.subscription":   "",
		"pubsub.uri":            "",
		"pubsub.exchange_type":  "fanout",
		"pubsub.durable":        true,
		"pubsub.auto_delete":    false,
		"pubsub.exclusive":      false,
		"pubsub.prefetch_count": 0,
	}
}
