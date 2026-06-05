# MF Stream — animated map

A browser front-end for the streaming tutorial: it registers a **geometry
continuous query** on the MobilityAPI tier, subscribes to its Server-Sent Events
position stream, and animates the moving feature over a [MapLibre](https://maplibre.org/)
basemap with [DeckGL](https://deck.gl/). All temporal logic runs in the browser
via [MEOS.js](https://github.com/MobilityDB/MEOS.js) (WebAssembly): the streamed
vertices are assembled into a `TGeomPoint`, and `valueAtTimestamp` interpolates
the position at the animation clock, so the dot moves smoothly between the
vertices the stream delivers.

## Requirements

- A browser with **WebAssembly Memory64** (Chrome/Chromium 133+ or recent
  Firefox) — MEOS.js is compiled with `MEMORY64=1`.
- **Node 22+** to run the dev server.
- The MobilityAPI tier running with a moving feature loaded (the
  `stream_demo` collection, feature `1`, by default). Build and run it per the
  repository README.

## Run

```bash
npm install
npm run dev   # proxies /collections to http://localhost:8088 (override with MFAPI_ORIGIN)
```

Open the printed URL. The page registers the query, opens the stream, and
animates the feature. Point it at another feature with query parameters:

```
?cid=<collection>&fid=<feature id>&intervalMs=<pacing>
```

## How it fits

```
MobilityAPI  POST …/tgsequence/queries        → cquery link object
             GET  …/queries/{id}/stream (SSE)  → {datetime, coordinates}
   │  EventSource
   ▼
browser:  positions → TGeomPoint (MEOS.js/WASM) → valueAtTimestamp(clock)
                    → DeckGL PathLayer (trail) + ScatterplotLayer (dot) over MapLibre
```

The tier holds no MEOS and streams only the stored positions; the browser does
the temporal interpolation.

## Windowed-aggregate overlay

The page also registers a windowed-aggregate query on a property (`speed` by
default, `AVG` over a `COUNT` window) and shows the live value in the HUD,
colouring the dot by it. The overlay degrades silently if the property is absent
or the tier was built without the in-process aggregate engine (`-tags meos`) —
the position animation still runs. Configure it with query parameters:

```
?aggProp=<property>&agg=AVG|SUM|MIN|MAX|COUNT&window=<count>
```
