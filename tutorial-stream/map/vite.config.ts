import { defineConfig } from 'vite';
import { fileURLToPath } from 'node:url';

const page = (name: string) => fileURLToPath(new URL(name, import.meta.url));

// The page calls the MobilityAPI tier same-origin; the dev server proxies those
// paths to the tier (default http://localhost:8088, override with MFAPI_ORIGIN),
// so the Server-Sent Events stream is not a cross-origin request.
const origin = process.env.MFAPI_ORIGIN ?? 'http://localhost:8088';

export default defineConfig({
	build: {
		rollupOptions: {
			input: { main: page('index.html'), fleet: page('fleet.html') },
		},
	},
	server: {
		proxy: {
			'/collections': { target: origin, changeOrigin: true },
			'/health': { target: origin, changeOrigin: true },
		},
	},
	// `vite preview` serves the production build (whose MEOS.js wasm is emitted as
	// a hashed asset, so it loads reliably); it proxies to the tier the same way.
	preview: {
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
