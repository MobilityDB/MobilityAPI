// Streaming tier: the engine-neutral control plane for OGC API – Moving
// Features – Part 4 (Continuous Query / Stream Extension). It is the streaming
// analogue of the request-response Backend seam: the control plane holds no
// MEOS and no engine specifics — it defines a continuous query, hands it to a
// StreamEngine, and delivers the engine's result instants over Server-Sent
// Events. A continuous transform applies a lifted scalar operation (the
// temporal counterpart of a float operation: ln, exp, ×, +, …) to a streaming
// tfloat, exactly as the float operation applies to a scalar.
//
// The seam sits at continuous-query submission, not per record, so it extends
// to the streaming engines the same way Backend extends to the DB engines: the
// local in-process MEOS engine (cgo, default) spawns a goroutine; a Flink /
// Kafka / Spark engine deploys a job and bridges its result topic into the same
// SSE delivery. MEOS runs inside the engine (cgo or JMEOS UDFs) — never as SQL
// issued to a database in the streaming path.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Instant is one stream record: a temporal-property value at a timestamp. The
// timestamp is carried verbatim as its ISO-8601 string so it round-trips
// through MEOS without a parse/format step. V holds a numeric value (TReal /
// TInt); S holds a text value (TText) or "t"/"f" (TBool).
type Instant struct {
	T string  `json:"datetime"`
	V float64 `json:"value"`
	S string  `json:"-"`
}

// QuerySpec is an engine-neutral continuous-query definition. A property query
// is either a transform (Op set) or a windowed aggregate (Agg + Window set); a
// geometry query streams positions.
type QuerySpec struct {
	Kind     string // "transform" (a temporal property) or "geometry" (the moving point)
	CID      string
	FID      int
	Pname    string
	Op       string        // a lifted scalar operation, e.g. "ln" or "mul"
	Arg      float64       // operand for scalar ops (add/sub/mul/div); ignored otherwise
	Agg      string        // a window aggregation, e.g. "AVG" or "MAX"
	Window   Window        // the window over which Agg is computed
	Ptype    string        // the property's OGC type (TReal | TInt | TText | TBool)
	Interval time.Duration // pacing between emitted records
}

// Window is a continuous-query window (OGC MF Part 4). COUNT groups a fixed
// number of records; TUMBLING groups a fixed, non-overlapping time span;
// HOPPING groups a fixed time span that advances by a shorter hop (overlapping).
type Window struct {
	Type    string // "COUNT", "TUMBLING" or "HOPPING"
	Size    int    // record count (COUNT) or duration (TUMBLING/HOPPING)
	Unit    string // time unit for TUMBLING/HOPPING (SECONDS, MINUTES, HOURS)
	Hop     int    // advance for HOPPING
	HopUnit string // time unit for the hop
}

// aggByType is the catalogue of window aggregations valid per property type
// (OGC MF Part 4 Table 6). Numeric aggregations use MEOS scalar accessors over
// the window's values; text and boolean aggregations reduce the MEOS value
// arrays.
var aggByType = map[string]map[string]bool{
	"TReal": {"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true},
	"TInt":  {"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true},
	"TText": {"COUNT": true, "COUNT_DISTINCT": true},
	"TBool": {"COUNT": true, "ANY": true, "ALL": true, "COUNT_TRUE": true, "COUNT_FALSE": true},
}

// aggregations is the union of all valid aggregation names (the engine validates
// the name; the control plane validates it against the property type).
var aggregations = func() map[string]bool {
	all := map[string]bool{}
	for _, m := range aggByType {
		for k := range m {
			all[k] = true
		}
	}
	return all
}()

func supportedAggs(ptype string) string {
	m := aggByType[ptype]
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

// StreamEngine is the engine-neutral streaming seam — the streaming analogue of
// Backend. An implementation runs a continuous query on its platform and
// exposes the result instants through a QueryHandle.
type StreamEngine interface {
	Name() string
	Submit(ctx context.Context, spec QuerySpec, source <-chan Instant) (QueryHandle, error)
}

// QueryHandle is a running continuous query on some engine. Results emits the
// engine's output Events (a transformed instant or a window aggregate) and is
// closed when the query ends. Stop releases any engine-side resources (for
// cluster engines, cancels the deployed job); the control plane also cancels the
// context, which is how the local engine stops.
type QueryHandle interface {
	Results() <-chan Event
	Status() string
	Stop() error
}

// makeEngine builds the configured engine; it is a var so tests can inject a
// fake engine.
var makeEngine = selectEngine

// selectEngine picks the streaming engine from MFAPI_STREAM_ENGINE: "flink"
// runs queries as Flink DataStream jobs (cluster engine); anything else uses the
// in-process meos-local engine (defaultStreamEngine, defined per build tag — the
// cgo MEOS engine under -tags meos, a stub otherwise). Both sit behind the same
// StreamEngine seam.
func selectEngine() (StreamEngine, error) {
	if strings.EqualFold(os.Getenv("MFAPI_STREAM_ENGINE"), "flink") {
		return newFlinkEngine()
	}
	return defaultStreamEngine()
}

var (
	engMu   sync.Mutex
	engInst StreamEngine
	engOK   bool
)

// streamEngine returns the process-wide engine, building it once.
func streamEngine() (StreamEngine, error) {
	engMu.Lock()
	defer engMu.Unlock()
	if engOK {
		return engInst, nil
	}
	e, err := makeEngine()
	if err != nil {
		return nil, err
	}
	engInst, engOK = e, true
	return e, nil
}

// opInfo describes a lifted scalar operation exposed by the streaming tier. The
// engine maps the name to the MEOS function; the control plane only validates.
type opInfo struct {
	needsArg bool
	desc     string
}

// liftedOps is the catalogue of scalar operations a continuous transform can
// apply to a tfloat stream. Every entry is a MEOS lifted temporal function that
// exists today, so the POC needs no kernel change. (sin/cos/tan join this set
// once MEOS adds them; arithmetic and the listed math functions are present.)
var liftedOps = map[string]opInfo{
	"ln":      {false, "natural logarithm"},
	"exp":     {false, "exponential"},
	"log10":   {false, "base-10 logarithm"},
	"ceil":    {false, "ceiling"},
	"floor":   {false, "floor"},
	"abs":     {false, "absolute value"},
	"degrees": {false, "radians to degrees"},
	"radians": {false, "degrees to radians"},
	"add":     {true, "add a scalar"},
	"sub":     {true, "subtract a scalar"},
	"mul":     {true, "multiply by a scalar"},
	"div":     {true, "divide by a scalar"},
}

func supportedOps() string {
	keys := make([]string, 0, len(liftedOps))
	for k := range liftedOps {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

// Event is the JSON payload of one streamed record — a transformed instant, a
// moving-feature position, or (later) a window aggregate. The control plane fans
// Events out to SSE subscribers without interpreting their shape.
type Event = map[string]any

// contQuery is one registered continuous query: its spec, the unified Event
// stream it fans out, and the set of SSE subscribers a pump goroutine serves.
// handle is the engine handle for transform/aggregate queries; it is nil for a
// geometry query, which streams positions directly from its source.
type contQuery struct {
	id      string
	spec    QuerySpec
	handle  QueryHandle
	events  <-chan Event
	created time.Time
	cancel  context.CancelFunc

	mu      sync.Mutex
	subs    map[chan Event]struct{}
	stopped bool
}

// pump fans each Event out to every subscriber. A slow subscriber is skipped
// (never blocks the others); when the event stream ends, all subscribers close.
func (cq *contQuery) pump() {
	for ev := range cq.events {
		cq.mu.Lock()
		for ch := range cq.subs {
			select {
			case ch <- ev:
			default:
			}
		}
		cq.mu.Unlock()
	}
	cq.mu.Lock()
	cq.stopped = true
	for ch := range cq.subs {
		close(ch)
		delete(cq.subs, ch)
	}
	cq.mu.Unlock()
}

func (cq *contQuery) subscribe() chan Event {
	ch := make(chan Event, 32)
	cq.mu.Lock()
	if cq.stopped {
		close(ch)
	} else {
		cq.subs[ch] = struct{}{}
	}
	cq.mu.Unlock()
	return ch
}

func (cq *contQuery) unsubscribe(ch chan Event) {
	cq.mu.Lock()
	if _, ok := cq.subs[ch]; ok {
		delete(cq.subs, ch)
		close(ch)
	}
	cq.mu.Unlock()
}

func (cq *contQuery) stop() {
	cq.cancel()
	if cq.handle != nil {
		cq.handle.Stop()
	}
}

func (cq *contQuery) status() string {
	cq.mu.Lock()
	st := cq.stopped
	cq.mu.Unlock()
	if st {
		return "stopped"
	}
	if cq.handle != nil {
		return cq.handle.Status()
	}
	return "running"
}

func (cq *contQuery) basePath() string {
	if cq.spec.Kind == "geometry" {
		return fmt.Sprintf("/collections/%s/items/%d/tgsequence/queries", cq.spec.CID, cq.spec.FID)
	}
	return fmt.Sprintf("/collections/%s/items/%d/tproperties/%s/queries", cq.spec.CID, cq.spec.FID, cq.spec.Pname)
}

// streamRegistry is the in-memory set of running continuous queries.
type streamRegistry struct {
	mu      sync.Mutex
	queries map[string]*contQuery
	nextID  int
}

var streamReg = &streamRegistry{queries: map[string]*contQuery{}}

// register stores a query built around its Event stream and starts fanning out.
// The caller owns the cancellable context (the query outlives the HTTP request
// that created it); cancel is stored so stop() can end it.
func (r *streamRegistry) register(spec QuerySpec, handle QueryHandle, events <-chan Event, cancel context.CancelFunc) *contQuery {
	r.mu.Lock()
	r.nextID++
	id := "q" + strconv.Itoa(r.nextID)
	cq := &contQuery{id: id, spec: spec, handle: handle, events: events, created: time.Now().UTC(), cancel: cancel, subs: map[chan Event]struct{}{}}
	r.queries[id] = cq
	r.mu.Unlock()
	go cq.pump()
	return cq
}

// submit runs a transform or aggregate query on the engine and fans its output
// Events out.
func (r *streamRegistry) submit(ctx context.Context, cancel context.CancelFunc, eng StreamEngine, spec QuerySpec, source <-chan Instant) (*contQuery, error) {
	h, err := eng.Submit(ctx, spec, source)
	if err != nil {
		cancel()
		return nil, err
	}
	return r.register(spec, h, h.Results(), cancel), nil
}

func (r *streamRegistry) get(id string) *contQuery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.queries[id]
}

func (r *streamRegistry) remove(id string) {
	r.mu.Lock()
	delete(r.queries, id)
	r.mu.Unlock()
}

func (r *streamRegistry) list(cid string, fid int, name string) []*contQuery {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*contQuery, 0)
	for _, cq := range r.queries {
		if cq.spec.CID == cid && cq.spec.FID == fid && cq.spec.Pname == name {
			out = append(out, cq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// cqueryLink renders the OGC 'cquery' link object for a registered query.
func cqueryLink(r *http.Request, cq *contQuery) map[string]any {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	self := scheme + "://" + r.Host + cq.basePath() + "/" + cq.id
	stream := self + "/stream"
	return map[string]any{
		"rel":       "cquery",
		"queryId":   cq.id,
		"href":      stream,
		"channel":   cq.basePath() + "/" + cq.id + "/stream",
		"status":    cq.status(),
		"type":      "text/event-stream",
		"operation": cq.spec.Op,
		"links": []map[string]string{
			{"rel": "self", "href": self},
			{"rel": "stream", "href": stream, "type": "text/event-stream"},
		},
	}
}

// parseInstantsMFJSON extracts the (timestamp, value) instants from a temporal
// MF-JSON document, handling both the flat form and a sequence set. A numeric
// value lands in V; a text value in S; a boolean in S as "t"/"f".
func parseInstantsMFJSON(b []byte) ([]Instant, error) {
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	var out []Instant
	collect := func(m map[string]any) {
		dts, _ := m["datetimes"].([]any)
		vals, _ := m["values"].([]any)
		n := len(dts)
		if len(vals) < n {
			n = len(vals)
		}
		for i := 0; i < n; i++ {
			ts, _ := dts[i].(string)
			in := Instant{T: ts}
			switch v := vals[i].(type) {
			case float64:
				in.V = v
			case string:
				in.S = v
			case bool:
				if v {
					in.S = "t"
				} else {
					in.S = "f"
				}
			}
			out = append(out, in)
		}
	}
	if seqs, ok := doc["sequences"].([]any); ok {
		for _, s := range seqs {
			if m, ok := s.(map[string]any); ok {
				collect(m)
			}
		}
	} else {
		collect(doc)
	}
	if len(out) == 0 {
		return nil, errors.New("temporal property has no instants to stream")
	}
	return out, nil
}

// replaySource loads the stored property's instants once (through the DB seam —
// data loading, not per-record work) from the column for its type and replays
// them as a paced, looping stream, giving the continuous query an unbounded
// source to process.
func replaySource(ctx context.Context, cid string, fid int, name, col string, interval time.Duration) (<-chan Instant, error) {
	var mf *string
	err := db.QueryRow(ctx,
		"SELECT asMFJSON("+col+") FROM mf_tproperty WHERE cid=$1 AND fid=$2 AND name=$3",
		cid, fid, name).Scan(&mf)
	if err != nil {
		return nil, err
	}
	if mf == nil {
		return nil, errors.New("temporal property value is null")
	}
	insts, err := parseInstantsMFJSON([]byte(*mf))
	if err != nil {
		return nil, err
	}
	out := make(chan Instant)
	go func() {
		defer close(out)
		t := time.NewTicker(interval)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select {
				case out <- insts[i]:
				case <-ctx.Done():
					return
				}
				i = (i + 1) % len(insts) // loop for an unbounded feel
			}
		}
	}()
	return out, nil
}

// postQuery registers a continuous transform on a tfloat temporal property.
func postQuery(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	name := r.PathValue("pname")
	if _, _, ok := collectionMeta(r.Context(), cid); !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	var ptype string
	err = db.QueryRow(r.Context(),
		"SELECT ptype FROM mf_tproperty WHERE cid=$1 AND fid=$2 AND name=$3", cid, fid, name).Scan(&ptype)
	if errors.Is(err, ErrNoRows) {
		httpErr(w, 404, "unknown temporal property: "+name)
		return
	}
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	tt, _ := tPropType(ptype)
	ogc := tt.ogc

	var body struct {
		Operation   string   `json:"operation"`
		Arg         *float64 `json:"arg"`
		Aggregation string   `json:"aggregation"`
		Window      struct {
			Type    string `json:"type"`
			Size    int    `json:"size"`
			Unit    string `json:"unit"`
			Hop     int    `json:"hop"`
			HopUnit string `json:"hopUnit"`
		} `json:"window"`
		Live       bool `json:"live"`
		IntervalMs int  `json:"intervalMs"`
	}
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&body)
	}
	interval := time.Duration(body.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	spec := QuerySpec{CID: cid, FID: fid, Pname: name, Ptype: ogc, Interval: interval}
	if agg := strings.ToUpper(strings.TrimSpace(body.Aggregation)); agg != "" {
		// windowed aggregation, valid per property type
		if !aggByType[ogc][agg] {
			httpErr(w, 400, "aggregation "+body.Aggregation+" is not defined for "+ogc+" (supported: "+supportedAggs(ogc)+")")
			return
		}
		wtype := strings.ToUpper(strings.TrimSpace(body.Window.Type))
		if wtype != "COUNT" && wtype != "TUMBLING" && wtype != "HOPPING" {
			httpErr(w, 400, "window type must be COUNT, TUMBLING or HOPPING")
			return
		}
		if body.Window.Size <= 0 {
			httpErr(w, 400, "window size must be a positive integer")
			return
		}
		if wtype == "HOPPING" && body.Window.Hop <= 0 {
			httpErr(w, 400, "a HOPPING window requires a positive hop")
			return
		}
		spec.Agg = agg
		spec.Window = Window{
			Type: wtype, Size: body.Window.Size, Unit: strings.ToUpper(body.Window.Unit),
			Hop: body.Window.Hop, HopUnit: strings.ToUpper(body.Window.HopUnit),
		}
	} else {
		// lifted transform — defined for TReal (tfloat) properties
		if ogc != "TReal" {
			httpErr(w, 422, "continuous transforms are defined for TReal (tfloat) properties; "+name+" is "+ptype)
			return
		}
		op := strings.ToLower(strings.TrimSpace(body.Operation))
		info, ok := liftedOps[op]
		if !ok {
			httpErr(w, 400, "unknown operation: "+body.Operation+" (supported: "+supportedOps()+")")
			return
		}
		if info.needsArg && body.Arg == nil {
			httpErr(w, 400, "operation "+op+" requires a scalar \"arg\"")
			return
		}
		spec.Op = op
		if body.Arg != nil {
			spec.Arg = *body.Arg
		}
	}

	eng, err := streamEngine()
	if err != nil {
		httpErr(w, 501, err.Error())
		return
	}

	qctx, qcancel := context.WithCancel(context.Background())
	var src <-chan Instant
	if body.Live {
		// the producer pushes records to …/ingest; this query processes them live
		src = live.subscribe(qctx, liveKey(cid, fid, name))
	} else {
		src, err = replaySource(qctx, cid, fid, name, tt.col, interval)
		if err != nil {
			qcancel()
			httpErr(w, 400, err.Error())
			return
		}
	}
	cq, err := streamReg.submit(qctx, qcancel, eng, spec, src)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, cqueryLink(r, cq))
}

// listQueries lists the continuous queries registered on a temporal property.
func listQueries(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	name := r.PathValue("pname")
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	list := streamReg.list(cid, fid, name)
	out := make([]any, 0, len(list))
	for _, cq := range list {
		out = append(out, cqueryLink(r, cq))
	}
	writeJSON(w, 200, map[string]any{"queries": out, "numberReturned": len(out)})
}

// getQuery returns the cquery link object and current status of one query.
func getQuery(w http.ResponseWriter, r *http.Request) {
	cq := streamReg.get(r.PathValue("qid"))
	if cq == nil {
		httpErr(w, 404, "unknown query: "+r.PathValue("qid"))
		return
	}
	writeJSON(w, 200, cqueryLink(r, cq))
}

// deleteQuery stops a running continuous query and removes it.
func deleteQuery(w http.ResponseWriter, r *http.Request) {
	cq := streamReg.get(r.PathValue("qid"))
	if cq == nil {
		httpErr(w, 404, "unknown query: "+r.PathValue("qid"))
		return
	}
	cq.stop()
	streamReg.remove(cq.id)
	w.WriteHeader(204)
}

// streamQuery delivers a continuous query's result instants as Server-Sent
// Events — the broker-free channel binding Part 4 allows.
func streamQuery(w http.ResponseWriter, r *http.Request) {
	cq := streamReg.get(r.PathValue("qid"))
	if cq == nil {
		httpErr(w, 404, "unknown query: "+r.PathValue("qid"))
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		httpErr(w, 500, "streaming unsupported by this server")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)
	fl.Flush()

	ch := cq.subscribe()
	defer cq.unsubscribe(ch)
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": keep-alive\n\n")
			fl.Flush()
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: instant\ndata: %s\n\n", rfc3339Tz(string(b)))
			fl.Flush()
		}
	}
}
