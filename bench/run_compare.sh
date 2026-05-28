#!/usr/bin/env bash
# Head-to-head load comparison of two MobilityAPI tiers over the SAME MobilityDB.
# Both tiers run identical SQL (asMFJSON / jsonb assembly); the only variable is
# the HTTP/runtime tier. Usage:
#   ./run_compare.sh <go_base> <py_base>
# e.g. ./run_compare.sh http://localhost:8088 http://localhost:8089
# Point py_base at the real PyMEOS server for the production-app comparison.
set -u
HEY="${HEY:-/tmp/gobin/hey}"
GO="${1:-http://localhost:8088}"
PY="${2:-http://localhost:8089}"

row() { # base path n c label
  out=$("$HEY" -n "$3" -c "$4" "$1$2" 2>&1)
  rps=$(echo "$out" | grep -i 'Requests/sec' | awk '{print $2}')
  avg=$(echo "$out" | grep -iE '^[[:space:]]*Average:' | awk '{print $2}')
  ok=$(echo "$out"  | grep -oE '\[200\][^0-9]*[0-9]+' | grep -oE '[0-9]+$')
  printf "  %-3s %-34s rps=%-11s avg=%-8s 200=%s/%s\n" "$5" "$2 (c=$4)" "${rps:-NA}" "${avg:-NA}" "${ok:-0}" "$3"
}

# endpoint  n      c     — light (tier-bound) first, heavy (DB-bound) last
SPECS=(
  "/health                          30000 200"
  "/collections                     20000 200"
  "/collections/ships/items/1        5000 100"
  "/collections/ships/items?limit=10 3000  50"
)
for spec in "${SPECS[@]}"; do
  set -- $spec; e=$1; n=$2; c=$3
  echo "== $e =="
  row "$GO" "$e" "$n" "$c" "GO"
  row "$PY" "$e" "$n" "$c" "PY"
done
