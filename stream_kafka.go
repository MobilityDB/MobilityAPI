// The Kafka result topic of a continuous query, a MobilityAPI extension beside
// the Server-Sent Events #streamQuery writes and the WebSocket
// #websocketQuery writes: a query registered with a topic publishes each of its
// results to that topic of the brokers MFAPI_KAFKA_BROKERS names, one record
// per result, keyed by the query's id, its value the JSON object an SSE event
// carries. The topic is created on first use where the brokers allow it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaTopicName is the name Kafka accepts for a topic.
var kafkaTopicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

var (
	kafkaMu      sync.Mutex
	kafkaClients = map[string]*kgo.Client{}
)

// kafkaProducer answers the client of the brokers MFAPI_KAFKA_BROKERS names, a
// comma-separated list of host:port, built once per list.
func kafkaProducer() (*kgo.Client, error) {
	brokers := strings.TrimSpace(os.Getenv("MFAPI_KAFKA_BROKERS"))
	if brokers == "" {
		return nil, errors.New("a Kafka topic needs the brokers MFAPI_KAFKA_BROKERS names, and it names none")
	}
	kafkaMu.Lock()
	defer kafkaMu.Unlock()
	if cl, ok := kafkaClients[brokers]; ok {
		return cl, nil
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.AllowAutoTopicCreation())
	if err != nil {
		return nil, err
	}
	kafkaClients[brokers] = cl
	return cl, nil
}

// checkKafkaTopic refuses a topic Kafka does not accept, or one the service has
// no brokers to publish to; no topic is no Kafka channel.
func checkKafkaTopic(topic string) error {
	if topic == "" {
		return nil
	}
	if !kafkaTopicName.MatchString(topic) {
		return errors.New("topic " + topic + " is no Kafka topic name: 1 to 249 letters, digits, '.', '_' or '-'")
	}
	_, err := kafkaProducer()
	return err
}

// publishToKafka publishes each result read from ch, a subscription of the
// query cq, to its topic, and flushes what is in flight once the query stops.
func publishToKafka(cq *contQuery, ch chan Event) {
	cl, err := kafkaProducer()
	if err != nil {
		log.Printf("query %s publishes to no topic: %v", cq.id, err)
		cq.unsubscribe(ch)
		return
	}
	for ev := range ch {
		b, _ := json.Marshal(ev)
		cl.Produce(context.Background(), &kgo.Record{Topic: cq.spec.Topic, Key: []byte(cq.id),
			Value: []byte(rfc3339In(string(b)))}, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Printf("query %s publishing to topic %s: %v", cq.id, cq.spec.Topic, err)
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cl.Flush(ctx)
}
