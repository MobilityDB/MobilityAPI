package main

import (
	"context"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// A record published to an MQTT topic reaches the live hub through the ingest
// binding. Verified against an embedded MQTT broker (no external broker needed).
func TestMQTTIngestToHub(t *testing.T) {
	const addr = "127.0.0.1:18831"
	broker := mqttserver.New(&mqttserver.Options{InlineClient: false})
	if err := broker.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	if err := broker.AddListener(listeners.NewTCP(listeners.Config{ID: "t1", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go func() { _ = broker.Serve() }()
	defer broker.Close()
	time.Sleep(300 * time.Millisecond) // let the broker bind

	c, err := startMQTTIngest("tcp://"+addr, "mfapi-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := live.subscribe(ctx, liveKey("ships", 7, "speed"))

	pub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + addr).SetClientID("producer"))
	if tok := pub.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	defer pub.Disconnect(100)
	pub.Publish("mfapi/ships/7/speed", 0, false, `{"value":42}`).Wait()

	select {
	case in := <-ch:
		if in.V != 42 {
			t.Errorf("delivered value = %v, want 42", in.V)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no record delivered from MQTT to the live hub")
	}
}
