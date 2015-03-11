package consumer

import (
	"encoding/json"
	"github.com/avinash240/rabbit-mq-stress-tester/queue"
	"log"
	"time"
)

func Consume(number int, uri string) {
	for {
		log.Printf("Consumer %d, connecting.\n", number)
		connection := queue.GetConnection(uri)
		defer connection.Close()
		channel, err := connection.Channel()
		if err != nil {
			println(err.Error())
			panic(err.Error())
		}
		// defer channel.Close()

		q := queue.MakeQueue(channel)

		msgs, err := channel.Consume(q.Name, "", true, false, false, false, nil)
		if err != nil {
			panic(err)
		}

		d := <-msgs
		var thisMessage queue.MqMessage
		err = json.Unmarshal(d.Body, &thisMessage)
		if err != nil {
			log.Printf("Error unmarshalling! %s", err.Error())
		}
		log.Printf("Consumer(%d) - Receiver message, age: %s", number, time.Since(thisMessage.TimeNow))
		connection.Close()
	}
	log.Println("done recieving")

}
