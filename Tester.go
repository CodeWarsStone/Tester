package Tester

import (
	"encoding/json"
	"github.com/YProblemka/Tester/RabbitMQ"
	"github.com/YProblemka/Tester/Utils"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"strconv"
	"time"
)

type Worker interface {
	Testing(body []byte, directoryName string) (*ResultTester, error)
}

type Tester struct {
	keyProducer  string
	testerWorker Worker
	Client       *RabbitMQ.Client
}

func NewTester(
	amqpURI,
	exchangeConsumer,
	exchangeTypeConsumer,
	routingKeyConsumer,
	queueNameConsumer,
	exchangeProducer,
	exchangeTypeProducer,
	ctag,
	keyProducer string,
	testerWorker Worker,
	workerCount int) *Tester {

	c, err := RabbitMQ.NewClient(
		amqpURI,
		exchangeConsumer,
		exchangeTypeConsumer,
		routingKeyConsumer,
		queueNameConsumer,
		exchangeProducer,
		exchangeTypeProducer,
		ctag)

	t := &Tester{
		testerWorker: testerWorker,
		Client:       c,
		keyProducer:  keyProducer,
	}

	if err != nil {
		Utils.ErrLog.Fatalf("%s", err)
	}

	for w := 1; w <= workerCount; w++ {
		go t.worker(w, c.Consumer.Deliveries, c.Producer)
	}

	return t
}

func (t *Tester) Testing(body []byte, key string) (*ResultTester, bool) {

	directoryName := strconv.FormatInt(time.Now().UnixNano(), 10) + key

	t.makeDir(directoryName)

	defer func() {
		t.delDir(directoryName)
	}()

	resultTester, err := t.testerWorker.Testing(body, directoryName)

	if err != nil {
		return nil, false
	}

	return resultTester, true
}

func (t *Tester) makeDir(directoryName string) {
	_ = os.MkdirAll(directoryName, os.ModePerm)
}

func (t *Tester) delDir(directoryName string) {
	_ = os.RemoveAll(directoryName)
}

func (t *Tester) worker(id int, jobs <-chan amqp.Delivery, producer *RabbitMQ.Producer) {
	for delivery := range jobs {

		if resultTester, success := t.Testing(delivery.Body, strconv.Itoa(id)); success {

			_ = delivery.Ack(false)

			resultTesterJson, _ := json.Marshal(resultTester)
			_ = producer.Publish(t.keyProducer, resultTesterJson, delivery.Headers)
		}
	}
}
