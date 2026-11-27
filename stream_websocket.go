// The WebSocket channel of a continuous query, a MobilityAPI extension beside
// the Server-Sent Events #streamQuery writes: each result of the query is one
// text message, the JSON object an event of the SSE channel carries, and the
// connection is closed with a normal closure when the query stops. A client
// reads the same results either way; the WebSocket serves the clients and the
// gateways that hold no EventSource.
package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// wsUpgrader accepts a connection from any origin, as the SSE channel answers
// any client: the results are what GET on the query reads.
var wsUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// wsWriteWait bounds the write of one message, so a client that stops reading
// releases its subscription rather than holding the query's fan-out.
const wsWriteWait = 10 * time.Second

// websocketQuery delivers a continuous query's results over a WebSocket, as
// #streamQuery delivers them over Server-Sent Events.
func websocketQuery(w http.ResponseWriter, r *http.Request) {
	cq := streamReg.get(r.PathValue("qid"))
	if cq == nil {
		httpErr(w, 404, "unknown query: "+r.PathValue("qid"))
		return
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		// the upgrader has answered the request with its refusal
		return
	}
	defer conn.Close()
	ch := cq.subscribe()
	defer cq.unsubscribe(ch)

	// The client sends nothing; reading notices its close and answers its pings
	gone := make(chan struct{})
	go func() {
		defer close(gone)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-gone:
			return
		case <-ping.C:
			if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)) != nil {
				return
			}
		case ev, ok := <-ch:
			if !ok {
				conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "the query has stopped"),
					time.Now().Add(wsWriteWait))
				return
			}
			b, _ := json.Marshal(ev)
			conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if conn.WriteMessage(websocket.TextMessage, []byte(rfc3339In(string(b)))) != nil {
				return
			}
		}
	}
}
