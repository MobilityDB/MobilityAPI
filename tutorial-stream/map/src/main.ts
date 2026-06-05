/**
 * MF Stream — animated map.
 *
 * Registers a geometry continuous query on the MobilityAPI tier, subscribes to
 * its Server-Sent Events position stream, and animates the moving feature over a
 * MapLibre basemap with DeckGL. The temporal logic runs in the browser via
 * MEOS.js (WebAssembly): the streamed vertices are assembled into a TGeomPoint,
 * and `valueAtTimestamp` interpolates the position at the animation clock, so the
 * dot moves smoothly between the vertices the stream delivers.
 */

import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { MapboxOverlay } from '@deck.gl/mapbox';
import { ScatterplotLayer, PathLayer } from '@deck.gl/layers';
import { initMeos, TGeomPoint } from 'meos.js';

const statusEl = document.getElementById('status')!;
const posEl = document.getElementById('pos')!;
const countEl = document.getElementById('count')!;

const params = new URLSearchParams(location.search);
const CID = params.get('cid') ?? 'stream_demo';
const FID = params.get('fid') ?? '1';
const INTERVAL_MS = Number(params.get('intervalMs') ?? 500);

const EPOCH_2000_MS = Date.UTC(2000, 0, 1);
const toTstz = (ms: number): number => Math.round((ms - EPOCH_2000_MS) * 1000);

// "YYYY-MM-DD HH:MM:SS+00" — the timestamp form MEOS' text parser accepts.
const toMeosTs = (ms: number): string =>
	new Date(ms).toISOString().replace('T', ' ').replace(/\.\d+Z$/, '+00');

function parsePoint(wkt: string): [number, number] | null {
	const m = wkt.match(/\(\s*([-\d.eE+]+)\s+([-\d.eE+]+)/);
	return m ? [parseFloat(m[1]), parseFloat(m[2])] : null;
}

// Accumulated trajectory vertices, keyed by their source timestamp so the looped
// replay does not grow the path. Each vertex carries an ordered clock in ms.
const seen = new Set<string>();
const verts: { ms: number; lon: number; lat: number }[] = [];
let traj: TGeomPoint | null = null;
let startMs = 0;
let endMs = 0;

function rebuild(): void {
	if (verts.length < 2) return;
	const body = verts
		.map(v => `POINT(${v.lon} ${v.lat})@${toMeosTs(v.ms)}`)
		.join(', ');
	traj = TGeomPoint.fromString(`[${body}]`);
	startMs = verts[0].ms;
	endMs = verts[verts.length - 1].ms;
}

async function main(): Promise<void> {
	statusEl.textContent = 'initialising MEOS (WebAssembly)…';
	await initMeos();

	const map = new maplibregl.Map({
		container: 'map',
		style: 'https://demotiles.maplibre.org/style.json',
		center: [12.5, 55.7],
		zoom: 11,
	});
	const overlay = new MapboxOverlay({ interleaved: true, layers: [] });
	map.addControl(overlay as unknown as maplibregl.IControl);

	statusEl.textContent = 'registering geometry query…';
	const base = `/collections/${CID}/items/${FID}/tgsequence/queries`;
	const res = await fetch(base, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ intervalMs: INTERVAL_MS }),
	});
	if (!res.ok) {
		statusEl.textContent = `query registration failed (${res.status})`;
		return;
	}
	const link = (await res.json()) as { queryId: string; channel: string };
	statusEl.textContent = `streaming ${CID}/${FID} (query ${link.queryId})`;

	let centred = false;
	const es = new EventSource(link.channel);
	es.addEventListener('instant', ev => {
		const data = JSON.parse((ev as MessageEvent).data) as {
			datetime: string;
			coordinates: [number, number];
		};
		if (seen.has(data.datetime)) return;
		seen.add(data.datetime);
		const [lon, lat] = data.coordinates;
		// A parseable timestamp keeps the real spacing; otherwise space the
		// vertices evenly so the animation still flows.
		const parsed = Date.parse(data.datetime);
		const ms = Number.isNaN(parsed) ? EPOCH_2000_MS + verts.length * 1000 : parsed;
		verts.push({ ms, lon, lat });
		countEl.textContent = String(verts.length);
		rebuild();
		if (!centred) {
			map.setCenter([lon, lat]);
			centred = true;
		}
	});
	es.onerror = () => {
		statusEl.textContent = 'stream error (is the tier running?)';
	};

	// A clock loops over the trajectory's time span; MEOS interpolates the
	// position at the clock, so the dot moves smoothly between the vertices.
	const PERIOD_MS = 8000;
	function frame(t: number): void {
		if (traj && endMs > startMs) {
			const phase = (t % PERIOD_MS) / PERIOD_MS;
			const clock = startMs + phase * (endMs - startMs);
			const wkt = traj.valueAtTimestamp(toTstz(clock));
			const pt = wkt ? parsePoint(wkt) : null;
			if (pt) {
				posEl.textContent = `${pt[0].toFixed(4)}, ${pt[1].toFixed(4)}`;
				overlay.setProps({
					layers: [
						new PathLayer({
							id: 'trail',
							data: [{ path: verts.map(v => [v.lon, v.lat] as [number, number]) }],
							getPath: (d: { path: [number, number][] }) => d.path,
							getColor: [44, 123, 182],
							getWidth: 3,
							widthUnits: 'pixels',
						}),
						new ScatterplotLayer({
							id: 'dot',
							data: [{ position: pt }],
							getPosition: (d: { position: [number, number] }) => d.position,
							getRadius: 7,
							radiusUnits: 'pixels',
							getFillColor: [215, 25, 28],
						}),
					],
				});
			}
		}
		requestAnimationFrame(frame);
	}
	requestAnimationFrame(frame);
}

main().catch(e => {
	statusEl.textContent = 'error: ' + (e as Error).message;
});
