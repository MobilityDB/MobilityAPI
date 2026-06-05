/**
 * MF Stream — animated fleet: the production-grade animated counterpart of the
 * DB tutorial. Its distinctive feature over a plain DeckGL demo is that **MEOS
 * runs in the browser (WebAssembly)**: the tier ships every vessel's trajectory
 * once (MEOS-sampled to a viz interval, decoupled into WGS84 path + epoch
 * timestamps — efficient transport, not render geometry), and MEOS.js builds a
 * TGeomPoint per ship. Every animation frame, MEOS.js computes the live position
 * of the whole fleet with valueAtTimestamp on one global clock — massive
 * real-time temporal processing client-side — and DeckGL only draws the result.
 * A QGIS-style controller (play/pause, scrubber, pace) drives the clock.
 */

import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { MapboxOverlay } from '@deck.gl/mapbox';
import { ScatterplotLayer, PathLayer } from '@deck.gl/layers';
import { initMeos, TGeomPoint } from 'meos.js';

const statusEl = document.getElementById('status')!;
const countEl = document.getElementById('count')!;
const scrub = document.getElementById('scrub') as HTMLInputElement;
const speedSlider = document.getElementById('speed') as HTMLInputElement;
const playBtn = document.getElementById('playpause')!;
const tnowEl = document.getElementById('tnow')!;
const tstartEl = document.getElementById('tstart')!;
const tendEl = document.getElementById('tend')!;
const chartCanvas = document.getElementById('chart') as HTMLCanvasElement;

const params = new URLSearchParams(location.search);
const CID = params.get('cid') ?? 'ships';
const SAMPLE = Number(params.get('sample') ?? 180);   // tier viz-sampling interval (seconds)
const MAX_SHIPS = Number(params.get('ships') ?? 0);   // 0 = all
const RECORD = Number(params.get('record') ?? 0);     // seconds; >0 captures fleet.webm on the GPU

const EPOCH_2000_MS = Date.UTC(2000, 0, 1);
const toTstz = (ms: number): number => Math.round((ms - EPOCH_2000_MS) * 1000); // MEOS µs since 2000
const toMeosTs = (ms: number): string =>
	new Date(ms).toISOString().replace('T', ' ').replace(/\.\d+Z$/, '+00');
const fmtClock = (ms: number): string =>
	new Date(ms).toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC');
const fmtShort = (ms: number): string =>
	new Date(ms).toISOString().slice(5, 16).replace('T', ' ');

interface Ship { id: number; traj: TGeomPoint; col: [number, number, number]; }
type Seg = { path: [number, number][]; t: number[] };

function palette(id: number): [number, number, number] {
	const h = (id * 137.508) % 360;
	const c = 0.65, x = c * (1 - Math.abs(((h / 60) % 2) - 1)), m = 0.35;
	const [r, g, b] = h < 60 ? [c, x, 0] : h < 120 ? [x, c, 0] : h < 180 ? [0, c, x]
		: h < 240 ? [0, x, c] : h < 300 ? [x, 0, c] : [c, 0, x];
	return [Math.round((r + m) * 255), Math.round((g + m) * 255), Math.round((b + m) * 255)];
}

// regex-free WKT point parse — runs once per ship per frame, so keep it cheap
function parsePoint(wkt: string): [number, number] | null {
	const o = wkt.indexOf('(');
	if (o < 0) return null;
	const sp = wkt.indexOf(' ', o);
	const cl = wkt.indexOf(')', sp);
	if (sp < 0 || cl < 0) return null;
	const lon = parseFloat(wkt.slice(o + 1, sp));
	const lat = parseFloat(wkt.slice(sp + 1, cl));
	return Number.isFinite(lon) && Number.isFinite(lat) ? [lon, lat] : null;
}

// pace slider 0..1000 -> whole span played in this many ms (1000 = fast)
const sliderToPlayback = (v: number) => 180000 - (v / 1000) * 165000;   // 180 s (slow) … 15 s (fast)
const yieldToBrowser = () => new Promise(r => setTimeout(r, 0));

// --- evolving chart: average speed over ground by ship type, swept by the clock ---
const SHIPTYPE_COLOR: Record<string, [number, number, number]> = {
	Cargo: [44, 123, 182], Tanker: [215, 25, 28], Passenger: [77, 175, 74],
	Fishing: [255, 160, 40], Tug: [152, 78, 163], Pilot: [120, 190, 210],
	HSC: [240, 110, 200], Dredging: [160, 140, 90], SAR: [230, 200, 50],
	Military: [110, 120, 130], Pleasure: [90, 200, 160], Sailing: [200, 180, 90],
	Towing: [180, 120, 70], Other: [120, 130, 140],
};
interface ChartSeries { type: string; color: [number, number, number]; pts: { ms: number; v: number }[]; }
let chartSeries: ChartSeries[] = [];
let chartMaxV = 1;

// draw the chart each frame: full curves faint, the portion up to `clockMs` bold,
// a playhead at the current time — the animated sibling of the static category chart
function drawChart(clockMs: number, tminMs: number, tmaxMs: number): void {
	const dpr = window.devicePixelRatio || 1;
	const W = chartCanvas.clientWidth, H = chartCanvas.clientHeight;
	if (chartCanvas.width !== W * dpr || chartCanvas.height !== H * dpr) {
		chartCanvas.width = W * dpr; chartCanvas.height = H * dpr;
	}
	const ctx = chartCanvas.getContext('2d')!;
	ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
	ctx.clearRect(0, 0, W, H);
	if (!chartSeries.length || tmaxMs <= tminMs) return;
	const padL = 34, padR = 8, padT = 20, padB = 14;
	const xOf = (ms: number) => padL + ((ms - tminMs) / (tmaxMs - tminMs)) * (W - padL - padR);
	const yOf = (v: number) => padT + (1 - v / chartMaxV) * (H - padT - padB);
	// gridlines + y labels
	ctx.strokeStyle = '#26303a'; ctx.fillStyle = '#5b6b78'; ctx.font = '10px system-ui'; ctx.lineWidth = 1;
	for (let k = 0; k <= 2; k++) {
		const v = (chartMaxV / 2) * k, y = yOf(v);
		ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
		ctx.fillText(v.toFixed(0), 6, y + 3);
	}
	const px = xOf(clockMs);
	for (const s of chartSeries) {
		const col = `rgb(${s.color[0]},${s.color[1]},${s.color[2]})`;
		// faint full curve
		ctx.strokeStyle = `rgba(${s.color[0]},${s.color[1]},${s.color[2]},0.25)`; ctx.lineWidth = 1;
		ctx.beginPath();
		s.pts.forEach((p, i) => (i ? ctx.lineTo(xOf(p.ms), yOf(p.v)) : ctx.moveTo(xOf(p.ms), yOf(p.v))));
		ctx.stroke();
		// bold up to the clock
		ctx.strokeStyle = col; ctx.lineWidth = 1.8; ctx.beginPath();
		let started = false;
		for (const p of s.pts) {
			if (p.ms > clockMs) break;
			const x = xOf(p.ms), y = yOf(p.v);
			started ? ctx.lineTo(x, y) : ctx.moveTo(x, y); started = true;
		}
		ctx.stroke();
	}
	// playhead
	ctx.strokeStyle = '#ffc83c'; ctx.lineWidth = 1; ctx.beginPath();
	ctx.moveTo(px, padT - 4); ctx.lineTo(px, H - padB); ctx.stroke();
	// legend (top, left-to-right)
	let lx = padL + 4; ctx.font = '11px system-ui'; ctx.textBaseline = 'middle';
	for (const s of chartSeries) {
		ctx.fillStyle = `rgb(${s.color[0]},${s.color[1]},${s.color[2]})`;
		ctx.fillRect(lx, 8, 9, 9); lx += 13;
		ctx.fillText(s.type, lx, 13); lx += ctx.measureText(s.type).width + 14;
	}
}

async function main(): Promise<void> {
	statusEl.textContent = 'initialising MEOS (WebAssembly)…';
	await initMeos();
	statusEl.textContent = `loading the "${CID}" fleet…`;
	const res = await fetch(`/collections/${CID}/trajectories?sample=${SAMPLE}`);
	if (!res.ok) {
		throw new Error(`collection "${CID}" trajectories unavailable (${res.status}); load it with tutorial/setup/load_ships.sql`);
	}
	const doc = (await res.json()) as { trips: { id: number; path: [number, number][]; timestamps: number[] }[]; tmin: number; tmax: number };
	const tminMs = doc.tmin * 1000, tmaxMs = doc.tmax * 1000;
	const spanMs = tmaxMs - tminMs;

	// group gap-free segments by vessel
	const byId = new Map<number, Seg[]>();
	for (const tr of doc.trips) {
		if (!byId.has(tr.id)) byId.set(tr.id, []);
		byId.get(tr.id)!.push({ path: tr.path, t: tr.timestamps });
	}
	let ids = [...byId.keys()];
	if (MAX_SHIPS > 0 && ids.length > MAX_SHIPS) ids = ids.slice(0, MAX_SHIPS);

	// fetch the aggregate time series (avg SOG per ship type) for the evolving chart
	fetch(`/collections/${CID}/timeseries?step=600`).then(async tr => {
		if (!tr.ok) return;
		const ts = (await tr.json()) as { buckets: number[]; series: Record<string, (number | null)[]> };
		const out: ChartSeries[] = [];
		for (const [type, vals] of Object.entries(ts.series)) {
			const pts = ts.buckets.map((b, i) => ({ ms: b * 1000, v: vals[i] })).filter(p => p.v != null) as { ms: number; v: number }[];
			if (pts.length < 2) continue;
			out.push({ type, color: SHIPTYPE_COLOR[type] ?? SHIPTYPE_COLOR.Other, pts });
		}
		// keep the most-populated types so the chart stays legible
		out.sort((a, b) => b.pts.length - a.pts.length);
		chartSeries = out.slice(0, 7);
		chartMaxV = Math.max(1, Math.ceil(Math.max(...chartSeries.flatMap(s => s.pts.map(p => p.v))) / 5) * 5);
	}).catch(() => { /* chart is optional */ });

	// --- map + basemap + controls FIRST so the view shows immediately ---
	const mapOpts: maplibregl.MapOptions = {
		container: 'map',
		style: 'https://basemaps.cartocdn.com/gl/dark-matter-nolabels-gl-style/style.json',
		center: [11, 56], zoom: 5,
	};
	// let the basemap canvas be read back for recording (runtime option)
	(mapOpts as { preserveDrawingBuffer?: boolean }).preserveDrawingBuffer = RECORD > 0;
	const map = new maplibregl.Map(mapOpts);
	// when recording, composite the basemap + DeckGL canvases into recCtx after
	// each DeckGL draw (the buffer is fresh in onAfterRender), and MediaRecorder
	// captures that — a frame-perfect, GPU-rendered video, no screen capture.
	let recCtx: CanvasRenderingContext2D | null = null;
	const overlay = new MapboxOverlay({
		interleaved: false, layers: [],
		onAfterRender: () => {
			if (!recCtx) return;
			const ml = map.getCanvas();
			recCtx.drawImage(ml, 0, 0, ml.width, ml.height);
			const dk = [...map.getContainer().querySelectorAll('canvas')].find(c => c !== ml) as HTMLCanvasElement | undefined;
			if (dk) recCtx.drawImage(dk, 0, 0, ml.width, ml.height);
		},
	});
	map.addControl(overlay as unknown as maplibregl.IControl);
	map.addControl(new maplibregl.NavigationControl({ showCompass: true }), 'top-right');

	function record(seconds: number): void {
		const ml = map.getCanvas();
		const rc = document.createElement('canvas');
		rc.width = ml.width; rc.height = ml.height;
		recCtx = rc.getContext('2d');
		const mime = MediaRecorder.isTypeSupported('video/webm;codecs=vp9') ? 'video/webm;codecs=vp9' : 'video/webm';
		const chunks: Blob[] = [];
		const mr = new MediaRecorder(rc.captureStream(60), { mimeType: mime, videoBitsPerSecond: 16_000_000 });
		mr.ondataavailable = e => { if (e.data.size) chunks.push(e.data); };
		mr.onstop = () => {
			recCtx = null;
			const a = document.createElement('a');
			a.href = URL.createObjectURL(new Blob(chunks, { type: 'video/webm' }));
			a.download = 'fleet.webm'; a.click();
			statusEl.textContent = 'recording saved → fleet.webm';
		};
		mr.start();
		statusEl.textContent = `recording ${seconds}s (GPU) → fleet.webm…`;
		setTimeout(() => mr.stop(), seconds * 1000);
	}

	// static route trails, straight from the sampled paths (no MEOS needed)
	const trails = ids.flatMap(id => byId.get(id)!.map(s => ({ path: s.path, color: palette(id) })));
	map.once('load', () => {
		let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
		for (const tr of trails) for (const [x, y] of tr.path) {
			minx = Math.min(minx, x); miny = Math.min(miny, y);
			maxx = Math.max(maxx, x); maxy = Math.max(maxy, y);
		}
		if (Number.isFinite(minx) && maxx > minx) {
			map.fitBounds([[minx, miny], [maxx, maxy]], { padding: 40, duration: 0 });
		}
	});
	// coloured route web on the dark basemap (normal blending — no over-bright blobs)
	const trailLayer = new PathLayer<{ path: [number, number][]; color: [number, number, number] }>({
		id: 'trails', data: trails,
		getPath: d => d.path, getColor: d => d.color,
		getWidth: 1, widthUnits: 'pixels', opacity: 0.4, capRounded: true, jointRounded: true,
	});

	// --- temporal controller: one global clock over the AIS span ---
	let playing = true;
	let clockMs = tminMs, lastT = 0;
	let playbackMs = sliderToPlayback(Number(speedSlider.value));
	tstartEl.textContent = fmtShort(tminMs);
	tendEl.textContent = fmtShort(tmaxMs);
	playBtn.addEventListener('click', () => {
		playing = !playing;
		playBtn.textContent = playing ? '⏸' : '▶';
	});
	scrub.addEventListener('input', () => {
		playing = false; playBtn.textContent = '▶';
		clockMs = tminMs + (Number(scrub.value) / 1000) * spanMs;
	});
	speedSlider.addEventListener('input', () => { playbackMs = sliderToPlayback(Number(speedSlider.value)); });

	// fleet fills in progressively as MEOS builds it (below); the loop renders
	// whatever exists so far, so the map is live from the first frame.
	const fleet: Ship[] = [];
	function frame(t: number): void {
		const dt = lastT ? t - lastT : 0; lastT = t;
		if (playing) {
			clockMs += dt * (spanMs / playbackMs);
			if (clockMs > tmaxMs) clockMs = tminMs;          // loop
		}
		const tstz = toTstz(clockMs);
		const dots: { position: [number, number]; color: [number, number, number] }[] = [];
		for (const s of fleet) {                              // MEOS.js: live position of every ship, this frame
			const wkt = s.traj.valueAtTimestamp(tstz);
			const pt = wkt ? parsePoint(wkt) : null;          // null when absent / in a gap
			if (pt) dots.push({ position: pt, color: s.col });
		}
		overlay.setProps({
			layers: [
				trailLayer,
				// crisp bright ship heads (the glowing trails carry the glow)
				new ScatterplotLayer<{ position: [number, number]; color: [number, number, number] }>({
					id: 'dots', data: dots,
					getPosition: d => d.position,
					getFillColor: d => [Math.min(255, d.color[0] + 70), Math.min(255, d.color[1] + 70), Math.min(255, d.color[2] + 70)],
					getRadius: 2.6, radiusUnits: 'pixels', radiusMinPixels: 2,
				}),
			],
		});
		scrub.value = String(Math.round(((clockMs - tminMs) / spanMs) * 1000));
		tnowEl.textContent = fmtClock(clockMs);
		drawChart(clockMs, tminMs, tmaxMs);
		requestAnimationFrame(frame);
	}
	requestAnimationFrame(frame);

	// build a gap-aware MEOS TGeomPoint per ship, in chunks that yield to the
	// browser so the map stays responsive and ships pop in as they are built
	let built = 0;
	for (let i = 0; i < ids.length; i += 50) {
		for (const id of ids.slice(i, i + 50)) {
			const seqs = byId.get(id)!
				.filter(s => s.path.length >= 2)
				.map(s => '[' + s.path.map((p, k) => `POINT(${p[0]} ${p[1]})@${toMeosTs(s.t[k] * 1000)}`).join(', ') + ']');
			if (!seqs.length) continue;
			const wkt = seqs.length === 1 ? seqs[0] : '{' + seqs.join(', ') + '}';
			try {
				fleet.push({ id, traj: TGeomPoint.fromString(wkt), col: palette(id) });
				built++;
			} catch { /* skip a malformed trajectory */ }
		}
		countEl.textContent = String(built);
		statusEl.textContent = `MEOS building ${built}/${ids.length} ships…`;
		await yieldToBrowser();
	}
	statusEl.textContent = `${built} ships · MEOS.js computing positions live`;

	// frame-perfect capture on the GPU: ?record=<seconds> waits for the fleet to
	// build, then records the composited canvas to fleet.webm
	if (RECORD > 0) {
		clockMs = tminMs;                          // start the clip at the first instant
		setTimeout(() => record(RECORD), 600);
	}
}

main().catch(e => { statusEl.textContent = 'error: ' + (e as Error).message; });
