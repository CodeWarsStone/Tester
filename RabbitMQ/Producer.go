package RabbitMQ

import (
	"Tester/Utils"
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	exchange  string
	done      chan error
	publishes chan uint64
	confirms  chan amqp.Confirmation
}

func NewProducer(conn *amqp.Connection, exchange, exchangeType string) (*Producer, error) {
	p := &Producer{
		conn:      conn,
		channel:   nil,
		exchange:  exchange,
		done:      make(chan error),
		publishes: make(chan uint64, 8),
	}

	var err error

	Utils.Log.Printf("got Connection, getting Channel")
	p.channel, err = p.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %s", err)
	}

	Utils.Log.Printf("got Channel, declaring %q Exchange (%q)", exchangeType, p.exchange)
	if err := p.channel.ExchangeDeclare(
		p.exchange,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("exchange Declare: %s", err)
	}

	p.confirms = p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	return p, nil
}

func (p *Producer) Publish(routingKey string, body []byte, headers amqp.Table) error {

	Utils.Log.Println("declared Exchange, publishing messages")

	seqNo := p.channel.GetNextPublishSeqNo()
	Utils.Log.Printf("publishing %dB body (%q)", len(body), body)

	if err := p.channel.PublishWithContext(
		context.TODO(),
		p.exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers:         headers,
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Transient,
			Priority:        0,
		},
	); err != nil {
		return fmt.Errorf("exchange Publish: %s", err)
	}

	Utils.Log.Printf("published %dB OK", len(body))

	p.publishes <- seqNo

	return nil
}

func (p *Producer) confirmHandler() {
	go func() {
		m := make(map[uint64]bool)
		for {
			select {
			case <-p.done:
				if err := p.Shutdown(); err != nil {
					Utils.ErrLog.Fatalf("error during shutdown: %s", err)
				}
				Utils.Log.Println("confirmHandler is stopping")
				return
			case publishSeqNo := <-p.publishes:
				Utils.Log.Printf("waiting for confirmation of %d", publishSeqNo)
				m[publishSeqNo] = false
			case confirmed := <-p.confirms:
				if confirmed.DeliveryTag > 0 {
					if confirmed.Ack {
						Utils.Log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
					} else {
						Utils.ErrLog.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
					}
					delete(m, confirmed.DeliveryTag)
				}
			}
			if len(m) > 1 {
				Utils.Log.Printf("outstanding confirmations: %d", len(m))
			}
		}
	}()

}

func (p *Producer) Shutdown() error {
	if err := p.channel.Close(); err != nil {
		return fmt.Errorf("producer close failed: %s", err)
	}

	defer Utils.Log.Printf("AMQP shutdown OK")

	return nil
}
