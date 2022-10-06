package RabbitMQ

import (
	"fmt"
	"github.com/YProblemka/Tester/Utils"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
)

type Consumer struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	Deliveries <-chan amqp.Delivery
	done       chan error
}

func NewConsumer(conn *amqp.Connection, exchange, exchangeType, exchangeKey, queueName, tag string) (*Consumer, error) {
	c := &Consumer{
		conn:    conn,
		channel: nil,
		done:    make(chan error),
	}

	var err error

	Utils.Log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	Utils.Log.Printf("got Channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("exchange Declare: %s", err)
	}

	Utils.Log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	Utils.Log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, exchangeKey)

	if err = c.channel.QueueBind(
		queue.Name,
		exchangeKey,
		exchange,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	Utils.Log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", tag)
	c.Deliveries, err = c.channel.Consume(
		queue.Name,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	c.SetupCloseHandler()

	return c, nil
}

func (cons *Consumer) SetupCloseHandler() {
	go func() {
		<-cons.done
		Utils.Log.Printf("shutting down")
		if err := cons.Shutdown(); err != nil {
			Utils.ErrLog.Fatalf("error during shutdown: %s", err)
		}
		os.Exit(0)
	}()
}

func (cons *Consumer) Shutdown() error {
	if err := cons.channel.Close(); err != nil {
		return fmt.Errorf("consumer close failed: %s", err)
	}

	defer Utils.Log.Printf("AMQP shutdown OK")

	return nil
}
