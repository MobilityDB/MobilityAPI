// MQTT ingestion: a broker binding for the live-ingestion hub. A producer
// publishes moving-feature updates to an MQTT topic and the tier republishes
// them to the live queries — the EDR Pub/Sub source realised over a real broker,
// feeding the same hub as the HTTP …/ingest endpoint.
//
// Topic scheme: mfapi/<cid>/<fid>/<pname>. Payload is a JSON object
// {"value": <number>, "datetime"?: <string>} or a bare number.
package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// startMQTTIngest connects to the broker and forwards messages on
// mfapi/<cid>/<fid>/<pname> to the live hub. It returns once subscribed.
func startMQTTIngest(broker, clientID string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().AddBroker(broker).SetClientID(clientID).
		SetAutoReconnect(true).SetConnectTimeout(5 * time.Second)
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		return nil, tok.Error()
	}
	if tok := c.Subscribe("mfapi/+/+/+", 0, func(_ mqtt.Client, m mqtt.Message) {
		handleMQTT(m.Topic(), m.Payload())
	}); tok.Wait() && tok.Error() != nil {
		c.Disconnect(100)
		return nil, tok.Error()
	}
	return c, nil
}

// handleMQTT routes one message (topic mfapi/<cid>/<fid>/<pname>) to the hub.
func handleMQTT(topic string, payload []byte) {
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "mfapi" {
		return
	}
	fid, err := strconv.Atoi(parts[2])
	if err != nil {
		return
	}
	in, ok := parseIngestPayload(payload)
	if !ok {
		return
	}
	live.publish(liveKey(parts[1], fid, parts[3]), in)
}

// parseIngestPayload reads {value[,datetime]} or a bare number into an Instant.
func parseIngestPayload(payload []byte) (Instant, bool) {
	var obj struct {
		Value    *float64 `json:"value"`
		Datetime string   `json:"datetime"`
	}
	if err := json.Unmarshal(payload, &obj); err == nil && obj.Value != nil {
		return instantFrom(*obj.Value, obj.Datetime), true
	}
	if v, err := strconv.ParseFloat(strings.TrimSpace(string(payload)), 64); err == nil {
		return instantFrom(v, ""), true
	}
	return Instant{}, false
}

func instantFrom(v float64, ts string) Instant {
	ts = strings.TrimSpace(ts)
	if ts == "" {
		ts = time.Now().UTC().Format("2006-01-02 15:04:05+00")
	}
	return Instant{T: ts, V: v}
}
