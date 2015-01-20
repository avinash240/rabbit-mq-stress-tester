package consumer

import (
	"encoding/json"
	"github.com/avinash240/rabbit-mq-stress-tester/queue"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func Consume(number int, uri string) {
	log.Printf("Consumer %d, connecting.\n", number)
	connection, err := amqp.Dial(uri)
	if err != nil {
		println(err.Error())
		panic(err.Error())
	}
	defer connection.Close()

	channel, err1 := connection.Channel()
	if err1 != nil {
		println(err1.Error())
		panic(err1.Error())
	}
	defer channel.Close()

	q := queue.MakeQueue(channel)

	msgs, err3 := channel.Consume(q.Name, "", true, false, false, false, nil)
	if err3 != nil {
		panic(err3)
	}

	for d := range msgs {
		var thisMessage queue.MqMessage
		err4 := json.Unmarshal(d.Body, &thisMessage)
		if err4 != nil {
			log.Printf("Error unmarshalling! %s", err.Error())
		}
		log.Printf("Consumer(%d) - Receiver message, age: %s", number, time.Since(thisMessage.TimeNow))

	}

	log.Println("done recieving")

}
