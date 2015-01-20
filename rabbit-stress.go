package main

import (
	"github.com/avinash240/rabbit-mq-stress-tester/consumer"
	"github.com/avinash240/rabbit-mq-stress-tester/producer"
	"github.com/codegangsta/cli"
	"log"
	"os"
	"os/signal"
	"time"
)

var totalTime int64 = 0
var totalCount int64 = 0

func main() {
	app := cli.NewApp()
	app.Name = "rabbit-stress"
	app.Usage = "RabbitMQ stress test tool"
	app.Flags = []cli.Flag{
		cli.StringFlag{"server, s", "localhost", "Hostname for RabbitMQ server", ""},
		cli.IntFlag{"ramp, r", 0, "Numberof milli-seconds to wait between generation of consumer/producer pair.", ""},
		cli.IntFlag{"producer, p", 0, "Number of messages to produce, -1 to produce forever", ""},
		cli.IntFlag{"wait, w", 0, "Number of milliseconds to wait between publish events", ""},
		cli.IntFlag{"bytes, b", 0, "number of extra bytes to add to the RabbitMQ message payload. About 50K max", ""},
		cli.IntFlag{"concurrency, n", 50, "number of consumers AND publishers.  50 == 50 publishers AND 50 consumers.", ""},
		cli.BoolFlag{"quiet, q", "Print only errors to stdout", ""},
		cli.BoolFlag{"wait-for-ack, a", "Wait for an ack or nack after enqueueing a message", ""},
	}
	app.Action = func(c *cli.Context) {
		runApp(c)
	}
	app.Run(os.Args)
}

func runApp(c *cli.Context) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	uri := "amqp://guest:guest@" + c.String("server") + ":5672"
	// Timer for  generating consumer/producer pair
	var ramp int
	consumerGen := make(chan bool)
	producerGen := make(chan bool)
	if c.Int("ramp") > 0 {
		ramp = c.Int("ramp")
		generatePair := time.NewTicker(time.Duration(ramp) * time.Millisecond)
		doneBuilding := c.Int("concurrency")
		// For consumers
		go func() {
			i := 0
			for {
				consumerGen <- true
				if c.Int("producer") == -1 || c.Int("producer") > 0 {
					producerGen <- true
				}
				<-generatePair.C
				i += 1
				if i >= doneBuilding {
					generatePair.Stop()
					log.Println("Done generating consumer/publishers.")
					return
				}
			}
		}()
	} else {
		close(consumerGen)
		close(producerGen)
	}
	//
	if c.Int("consumer") > -1 {
		go makeConsumers(uri, consumerGen, c.Int("concurrency"))
	}
	//
	if c.Int("producer") != 0 {
		config := producer.ProducerConfig{uri, c.Int("bytes"), c.Bool("quiet"), c.Bool("wait-for-ack")}
		go makeProducers(c.Int("producer"), producerGen, c.Int("wait"), c.Int("concurrency"), config)
	}
	//
	<-signalChan
}

func makeProducers(n int, ramp chan bool, wait, concurrency int, config producer.ProducerConfig) {
	log.Printf("Generating %d Producers.\n", concurrency)

	taskChan := make(chan int)
	for i := 0; i < concurrency; i++ {
		go producer.Produce(i, config, taskChan, n)
		<-ramp
	}

	start := time.Now()
	if n == -1 {
		log.Printf("Producing messages til killed.\n")
		i := 0
		for {
			i++
			taskChan <- i
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
	} else {
		log.Printf("Producing %d messages.\n", n)
		for i := 0; i < n; i++ {
			taskChan <- i
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
	}

	time.Sleep(time.Duration(1) * time.Second)
	close(taskChan)
	log.Printf("Finished: %s", time.Since(start))
}

func makeConsumers(uri string, ramp chan bool, concurrency int) {
	log.Printf("Generating %d Consumers.\n", concurrency)

	for i := 0; i < concurrency; i++ {
		go consumer.Consume(i, uri)
		<-ramp
	}
}
