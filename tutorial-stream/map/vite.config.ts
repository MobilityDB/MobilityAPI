import { defineConfig } from 'vite';

// The page calls the MobilityAPI tier same-origin; the dev server proxies those
// paths to the tier (default http://localhost:8088, override with MFAPI_ORIGIN),
// so the Server-Sent Events stream is not a cross-origin request.
const origin = process.env.MFAPI_ORIGIN ?? 'http://localhost:8088';

export default defineConfig({
	server: {
		proxy: {
			'/collections': { target: origin, changeOrigin: true },
			'/health': { target: origin, changeOrigin: true },
		},
	},
	// meos.js ships its own wasm; let it resolve at runtime, and keep a single
	// copy of the DeckGL/luma stack.
	optimizeDeps: {
		exclude: ['meos.js'],
	},
	resolve: {
		dedupe: [
			'@deck.gl/core',
			'@deck.gl/layers',
			'@deck.gl/mapbox',
			'@luma.gl/core',
			'@luma.gl/engine',
			'@luma.gl/shadertools',
			'@luma.gl/webgl',
			'@math.gl/core',
			'@math.gl/web-mercator',
		],
	},
});
