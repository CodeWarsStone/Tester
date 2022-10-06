package RabbitMQ

import (
	"fmt"
	"github.com/YProblemka/Tester/Utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn     *amqp.Connection
	Consumer *Consumer
	Producer *Producer
	tag      string
}

func NewClient(
	amqpURI,
	exchangeConsumer,
	exchangeTypeConsumer,
	routingKeyConsumer,
	queueNameConsumer,
	exchangeProducer,
	exchangeTypeProducer,
	ctag string) (*Client, error) {

	c := &Client{
		conn: nil,
		tag:  ctag,
	}

	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName(c.tag)

	Utils.Log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.DialConfig(amqpURI, config)
	if err != nil {
		return nil, fmt.Errorf("dial: %s", err)
	}

	go func() {
		Utils.Log.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	c.Consumer, err = NewConsumer(c.conn, exchangeConsumer, exchangeTypeConsumer, routingKeyConsumer, queueNameConsumer, ctag)
	if err != nil {
		return nil, err
	}
	c.Producer, _ = NewProducer(c.conn, exchangeProducer, exchangeTypeProducer)

	c.Producer.confirmHandler()

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) Shutdown() error {
	c.Consumer.done <- nil
	c.Producer.done <- nil

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	return nil
}
