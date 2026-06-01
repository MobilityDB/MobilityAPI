#!/usr/bin/env python3
"""Controlled Python tier baseline for the MobilityAPI Go-vs-Python comparison.

NOT the production PyMEOS app — a faithful proxy for the *tier architecture*:
the same SQL (asMFJSON / jsonb assembly) against the same DB (mfapi_demo), so
the only variable is the HTTP/runtime tier (Python http.server + psycopg2 pool
vs Go net/http + pgxpool). Read path needs no PyMEOS (the DB serializes), which
mirrors the real read path. Env knobs:
  PY_MODE=threading|single  (ThreadingHTTPServer vs single-threaded; the explore
                             app is single-threaded http.server)
  PY_POOL=16                (psycopg2 ThreadedConnectionPool max — match Go MaxConns)
  PY_PORT=8089
"""
import http.server, json, os, re
from urllib.parse import urlparse, parse_qs
import psycopg2, psycopg2.pool

POOL = psycopg2.pool.ThreadedConnectionPool(
    1, int(os.environ.get("PY_POOL", "16")),
    host="/tmp", port=5432, dbname="mfapi_demo", user="esteban")

def q1(sql, args=()):
    conn = POOL.getconn()
    try:
        cur = conn.cursor(); cur.execute(sql, args); row = cur.fetchone(); conn.commit()
        return row[0] if row else None
    finally:
        POOL.putconn(conn)

ITEMS = ("SELECT jsonb_build_object('type','FeatureCollection','features',"
         " coalesce(jsonb_agg(jsonb_build_object('type','Feature','id',id::text,"
         " 'properties',jsonb_build_object('mmsi',mmsi,'name',name),"
         " 'temporalGeometry',asMFJSON(trip)::jsonb)),'[]'::jsonb))::text"
         " FROM (SELECT id,mmsi,name,trip FROM ships ORDER BY id LIMIT %s) s")
ITEM = ("SELECT jsonb_build_object('type','Feature','id',id::text,"
        " 'properties',jsonb_build_object('mmsi',mmsi,'name',name),"
        " 'temporalGeometry',asMFJSON(trip)::jsonb)::text FROM ships WHERE id=%s")
COLLS = ("SELECT jsonb_build_object('collections', coalesce(jsonb_agg("
         " jsonb_build_object('id',id,'title',title,'description',description)),'[]'::jsonb))::text"
         " FROM collections")

class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def _send(self, code, body):
        b = body.encode() if isinstance(body, str) else (body or b"{}")
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        p = urlparse(self.path)
        if p.path == "/health":
            return self._send(200, '{"status":"ok"}')
        if p.path == "/collections":
            return self._send(200, q1(COLLS))
        if re.match(r"^/collections/[^/]+/items$", p.path):
            limit = int(parse_qs(p.query).get("limit", ["100"])[0])
            return self._send(200, q1(ITEMS, (limit,)))
        m = re.match(r"^/collections/[^/]+/items/(\d+)$", p.path)
        if m:
            body = q1(ITEM, (int(m.group(1)),))
            return self._send(200 if body else 404, body)
        self._send(404, "{}")

mode = os.environ.get("PY_MODE", "threading")
port = int(os.environ.get("PY_PORT", "8089"))
Server = http.server.ThreadingHTTPServer if mode == "threading" else http.server.HTTPServer
print(f"python baseline mode={mode} pool={POOL.maxconn} on :{port}", flush=True)
Server(("127.0.0.1", port), H).serve_forever()
