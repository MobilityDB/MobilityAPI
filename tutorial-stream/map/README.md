# MF Stream — animated map

Browser front-ends for the streaming tutorial. Two pages, both registering a
**geometry continuous query** on the MobilityAPI tier, subscribing to its
Server-Sent Events position stream, and animating moving features over a
[MapLibre](https://maplibre.org/) basemap with [DeckGL](https://deck.gl/). All
temporal logic runs in the browser via [MEOS.js](https://github.com/MobilityDB/MEOS.js)
(WebAssembly): the streamed vertices are assembled into a `TGeomPoint`, and
`valueAtTimestamp` interpolates the position at the animation clock, so a dot
moves smoothly between the vertices the stream delivers.

- **`fleet.html`** — the **animated counterpart of the DB tutorial**, over the
  same `ships` collection (the one day of Danish AIS loaded by
  `tutorial/setup/load_ships.sql`). A fleet of vessels animates over the map,
  each coloured by its speed, with a live speed chart that grows as the stream
  delivers values. Speed is the vessel's reported speed over ground (AIS SOG),
  stored as a temporal property and delivered by a windowed-average continuous
  query, so the chart accumulates the running average of the last observations.
- **`index.html`** — a minimal single-feature map.

## Common base with the static tutorial

The animated tutorial uses the **same data and setup script** as the static
notebook tutorial: load the `ships` collection once with
`tutorial/setup/load_ships.sql`, then either notebook (request–response) or
`fleet.html` (animated) runs over it. Point the fleet at another collection with
`?cid=<collection>&vessels=<n>`.

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
npm run build && npm run preview   # serves the production build on http://localhost:4173
```

`preview` serves the bundled MEOS.js wasm as a hashed asset, so it loads
reliably; both `dev` and `preview` proxy `/collections` to the tier at
`http://localhost:8088` (override with `MFAPI_ORIGIN`).

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

## Windowed-aggregate speed

Alongside the position stream, `fleet.html` registers a windowed-aggregate query
on each vessel's `speed` property (`AVG` over a `COUNT` window of the last
observations). Each delivered value feeds the live chart, scales the colour ramp,
and colours the vessel's dot. The speed feed degrades silently if the property is
absent or the tier was built without the in-process aggregate engine
(`-tags meos`) — the position animation still runs.
