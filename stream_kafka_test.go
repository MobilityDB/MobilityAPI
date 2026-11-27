package main

// Mirrors #TestWebSocketDeliversResultsAndClosesWithTheQuery of
// stream_websocket_test.go for the Kafka result topic (#publishToKafka): a
// query on the fake engine publishing to an in-process broker (kfake), so no
// Kafka, no database and no MEOS is needed.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaTopicReceivesEveryResult(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "mf-results"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	t.Setenv("MFAPI_KAFKA_BROKERS", strings.Join(cluster.ListenAddrs(), ","))

	src := make(chan Instant, 4)
	ctx, cancel := context.WithCancel(context.Background())
	spec := QuerySpec{CID: "ships", FID: 1, Pname: "speed", Op: "add", Arg: 1, Topic: "mf-results"}
	cq, err := streamReg.submit(ctx, cancel, fakeEngine{}, spec, src)
	if err != nil {
		t.Fatal(err)
	}
	defer streamReg.remove(cq.id)
	src <- Instant{T: "2026-01-01T00:00:00Z", V: 10}
	src <- Instant{T: "2026-01-01T00:00:01Z", V: 20}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics("mf-results"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	var got []float64
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	for len(got) < 2 {
		fetches := consumer.PollFetches(deadline)
		if deadline.Err() != nil {
			t.Fatalf("%d of 2 results read from the topic in 10 s", len(got))
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			var ev map[string]any
			if string(rec.Key) != cq.id || json.Unmarshal(rec.Value, &ev) != nil {
				t.Errorf("record key %q value %s, want key %s and a JSON result", rec.Key, rec.Value, cq.id)
				return
			}
			got = append(got, ev["value"].(float64))
		})
	}
	if got[0] != 11 || got[1] != 21 {
		t.Errorf("the topic holds %v, want [11 21], in order", got)
	}
	cq.stop()
}

func TestKafkaTopicRefusals(t *testing.T) {
	t.Setenv("MFAPI_KAFKA_BROKERS", "")
	for topic, says := range map[string]string{
		"mf-results": "names none",
		"no spaces":  "no Kafka topic name",
		"":           "",
	} {
		err := checkKafkaTopic(topic)
		if says == "" {
			if err != nil {
				t.Errorf("no topic is refused: %v", err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), says) {
			t.Errorf("topic %q = %v, want a refusal saying %q", topic, err, says)
		}
	}
}
