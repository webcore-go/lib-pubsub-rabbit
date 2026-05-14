package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/webcore-go/webcore/app/helper"
	"github.com/webcore-go/webcore/infra/logger"
	"github.com/webcore-go/webcore/port"
)

/*
RabbitMQ Exchange Types Configuration:

Based on PubSubConfig fields (inherited from config.PubSubConfig):
- Topic: Used as exchange name (for exchange types other than "none") or queue name (for "none")
- Subscription: Used as queue name (for exchange types other than "none") or routing key pattern
- Producer.MessageAttributes: Used for headers matching in "headers" exchange type

Exchange Types:

1. "none" (Message Queue mode):
   - Producer: Publish directly to queue (exchange="", routingKey=Topic)
   - Consumer: Consume from queue named Topic
   - Queue Declaration: Topic

2. "fanout":
   - Producer: Publish to exchange (Topic), routingKey=""
   - Consumer: Bind queue (Subscription) to exchange (Topic) with empty routing key
   - Exchange Declaration: Topic (type: fanout)
   - Queue Binding: QueueBind(Subscription, "", Topic)

3. "direct":
   - Producer: Publish to exchange (Topic) with routingKey=Subscription
   - Consumer: Bind queue (Subscription) to exchange (Topic) with routingKey=Subscription
   - Exchange Declaration: Topic (type: direct)
   - Queue Binding: QueueBind(Subscription, Subscription, Topic)

4. "topic":
   - Producer: Publish to exchange (Topic) with routingKey=Subscription
   - Consumer: Bind queue (Subscription) to exchange (Topic) with routingKey pattern=Subscription
   - Exchange Declaration: Topic (type: topic)
   - Queue Binding: QueueBind(Subscription, Subscription, Topic)
   - Note: Subscription supports wildcards: * (single word) and # (zero or more words)

5. "headers":
   - Producer: Publish to exchange (Topic) with headers, routingKey=""
   - Consumer: Bind queue (Subscription) to exchange (Topic) with headers matching
   - Exchange Declaration: Topic (type: headers)
   - Queue Binding: QueueBind(Subscription, "", Topic, args={x-match: "all", ...Producer.MessageAttributes})
   - Note: Headers are taken from Config.Producer.MessageAttributes
*/

type RabbitMQMessage struct {
	ID          string
	Data        []byte
	PublishTime time.Time
	Attributes  map[string]string
}

func (m *RabbitMQMessage) GetID() string {
	return m.ID
}

func (m *RabbitMQMessage) GetData() []byte {
	return m.Data
}

func (m *RabbitMQMessage) GetPublishTime() time.Time {
	return m.PublishTime
}

func (m *RabbitMQMessage) GetAttributes() map[string]string {
	return m.Attributes
}

type RabbitMQ struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Config     RabbitMQConfig
	Receivers  []port.PubSubReceiver
}

func NewRabbitMQ(ctx context.Context, cfg RabbitMQConfig) (*RabbitMQ, error) {
	if cfg.Uri == "" {
		return nil, fmt.Errorf("RabbitMQ config uri cannot be empty")
	}

	conn, err := amqp.Dial(cfg.Uri)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %v", err)
	}

	return &RabbitMQ{
		Connection: conn,
		Channel:    ch,
		Config:     cfg,
		Receivers:  []port.PubSubReceiver{},
	}, nil
}

func (r *RabbitMQ) Install(args ...any) error {
	return r.declareTopology()
}

func (r *RabbitMQ) Connect() error {
	if r.Connection == nil || r.Connection.IsClosed() {
		conn, err := amqp.Dial(r.Config.Uri)
		if err != nil {
			return fmt.Errorf("failed to reconnect to RabbitMQ: %v", err)
		}
		r.Connection = conn

		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to reopen RabbitMQ channel: %v", err)
		}
		r.Channel = ch

		return r.declareTopology()
	}
	return nil
}

func (r *RabbitMQ) Disconnect() error {
	if r.Channel != nil {
		r.Channel.Close()
	}
	if r.Connection != nil {
		r.Connection.Close()
	}
	return nil
}

func (r *RabbitMQ) Uninstall() error {
	return nil
}

func (r *RabbitMQ) Publish(ctx context.Context, message any, attributes map[string]string) (string, error) {
	var body []byte

	switch v := message.(type) {
	case string:
		body = []byte(v)
	case []byte:
		body = v
	default:
		str, err := helper.ToJSON(message)
		if err != nil {
			return "", err
		}
		body = []byte(str)
	}

	headers := amqp.Table{}
	for k, v := range attributes {
		headers[k] = v
	}

	exchange := ""
	if r.Config.ExchangeType != "none" {
		exchange = r.Config.Topic
	}

	routingKey := r.Config.Subscription
	switch r.Config.ExchangeType {
	case "none":
		routingKey = r.Config.Topic
	case "fanout", "headers":
		routingKey = ""
	}

	msgID := fmt.Sprintf("%d", time.Now().UnixNano())

	err := r.Channel.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		MessageId:    msgID,
		ContentType:  "application/json",
		Body:         body,
		Headers:      headers,
		Timestamp:    time.Now(),
		DeliveryMode: amqp.Persistent,
	})
	if err != nil {
		return "", fmt.Errorf("failed to publish RabbitMQ message: %v", err)
	}

	logger.Debug("RabbitMQ Publish: message", "msgID", msgID)
	return msgID, nil
}

func (r *RabbitMQ) RegisterReceiver(receiver port.PubSubReceiver) {
	r.Receivers = append(r.Receivers, receiver)
}

func (r *RabbitMQ) StartReceiving(ctx context.Context) {
	if len(r.Receivers) == 0 {
		logger.Error("RabbitMQ has no Receiver to process incoming message")
		return
	}

	var queueName string

	if r.Config.ExchangeType != "none" {
		queueName = r.Config.Subscription
		_, err := r.Channel.QueueDeclare(
			queueName,
			r.Config.Durable,
			r.Config.AutoDelete,
			r.Config.Exclusive,
			false,
			amqp.Table{},
		)
		if err != nil {
			logger.Error("Failed to declare RabbitMQ queue", "queue", queueName, "error", err)
			return
		}

		exchangeName := r.Config.Topic

		switch r.Config.ExchangeType {
		case "fanout":
			err = r.Channel.QueueBind(queueName, "", exchangeName, false, nil)
			if err != nil {
				logger.Error("Failed to bind queue to fanout exchange", "queue", queueName, "exchange", exchangeName, "error", err)
				return
			}

		case "direct":
			err = r.Channel.QueueBind(queueName, r.Config.Subscription, exchangeName, false, nil)
			if err != nil {
				logger.Error("Failed to bind queue to direct exchange", "queue", queueName, "exchange", exchangeName, "routingKey", r.Config.Subscription, "error", err)
				return
			}

		case "topic":
			err = r.Channel.QueueBind(queueName, r.Config.Subscription, exchangeName, false, nil)
			if err != nil {
				logger.Error("Failed to bind queue to topic exchange", "queue", queueName, "exchange", exchangeName, "routingKey", r.Config.Subscription, "error", err)
				return
			}

		case "headers":
			args := amqp.Table{}
			if len(r.Config.Producer.MessageAttributes) > 0 {
				args["x-match"] = "all"
				for k, v := range r.Config.Producer.MessageAttributes {
					args[k] = v
				}
			}
			err = r.Channel.QueueBind(queueName, "", exchangeName, false, args)
			if err != nil {
				logger.Error("Failed to bind queue to headers exchange", "queue", queueName, "exchange", exchangeName, "error", err)
				return
			}
		}
	}

	msgs, err := r.Channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		logger.Error("Failed to start consuming RabbitMQ queue", "error", err)
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("RabbitMQ consumer stopped")
				return
			case msg, ok := <-msgs:
				if !ok {
					logger.Info("RabbitMQ channel closed")
					return
				}

				attributes := make(map[string]string)
				for k, v := range msg.Headers {
					if s, ok := v.(string); ok {
						attributes[k] = s
					}
				}

				m := &RabbitMQMessage{
					ID:          msg.MessageId,
					Data:        msg.Body,
					PublishTime: msg.Timestamp,
					Attributes:  attributes,
				}

				if m.ID == "" {
					m.ID = fmt.Sprintf("%d-%d", msg.DeliveryTag, time.Now().UnixNano())
				}

				ackDone := false
				for _, c := range r.Receivers {
					ack, err := c.Consume(ctx, []port.IPubSubMessage{m})
					if !ackDone && err == nil && len(ack) > 0 {
						if val, ok := ack[m.ID]; ok && val {
							ackDone = true
							msg.Ack(false)
							logger.Debug("Message processed and acknowledged", "messageID", m.ID)
						}
					}
				}

				if !ackDone {
					msg.Nack(false, true)
					logger.Debug("Message not processed and not acknowledged", "messageID", m.ID)
				}
			}
		}
	}()
}

func (r *RabbitMQ) declareTopology() error {
	if r.Config.ExchangeType == "none" {
		// r.Config.Subscription tidak berlaku di mode message queue r.Config.Topic dijadikan nama queue
		_, err := r.Channel.QueueDeclare(
			r.Config.Topic,
			r.Config.Durable,
			r.Config.AutoDelete,
			r.Config.Exclusive,
			false,
			amqp.Table{},
		)
		if err != nil {
			return fmt.Errorf("failed to declare RabbitMQ queue: %v", err)
		}
	} else {
		err := r.Channel.ExchangeDeclare(
			r.Config.Topic,
			r.Config.ExchangeType,
			r.Config.Durable,
			r.Config.AutoDelete,
			false,
			false,
			amqp.Table{},
		)
		if err != nil {
			return fmt.Errorf("failed to declare RabbitMQ exchange: %v", err)
		}
	}

	if r.Config.PrefetchCount > 0 {
		err := r.Channel.Qos(r.Config.PrefetchCount, 0, false)
		if err != nil {
			return fmt.Errorf("failed to set RabbitMQ QoS: %v", err)
		}
	}

	return nil
}
