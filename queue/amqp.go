package queue

import (
	"github.com/streadway/amqp"
	"time"
)

type MqMessage struct {
	TimeNow        time.Time
	SequenceNumber int
	Payload        string
}

func MakeQueue(c *amqp.Channel) amqp.Queue {
	q, err2 := c.QueueDeclare("stress-test-queue", true, false, false, false, nil)
	if err2 != nil {
		panic(err2)
	}
	return q
}

func GetConnection(uri string) *amqp.Connection {
	connection, err := amqp.Dial(uri)
	if err != nil {
		println(err.Error())
		panic(err.Error())
	}
	return connection
}
