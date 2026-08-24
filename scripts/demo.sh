#!/usr/bin/env bash
#
# A complete, self contained tour of the queue. Starts a server, drives it,
# proves it survives a hard kill, and cleans up after itself.
#
#   ./scripts/demo.sh
#
# Nothing to install beyond Go. Uses a temporary log, so it never touches
# ./data and can be run repeatedly. Override the port with PORT=9000.

set -euo pipefail

PORT="${PORT:-8080}"
BASE="http://localhost:${PORT}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
SRV_PID=""

if [ -t 1 ]; then B=$(tput bold); D=$(tput dim); G=$(tput setaf 2); R=$(tput setaf 1); N=$(tput sgr0); JQC="-C"
else B=""; D=""; G=""; R=""; N=""; JQC=""; fi

cleanup() {
  if [ -n "$SRV_PID" ]; then kill "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; fi
  rm -rf "$WORK"
}
trap cleanup EXIT

step()  { printf '\n%s── %s ──%s\n' "$B" "$1" "$N"; }
note()  { printf '   %s%s%s\n' "$D" "$1" "$N"; }
cmd()   { printf '\n   %s$ %s%s\n' "$D" "$1" "$N"; }
ok()    { printf '   %s✓%s %s\n' "$G" "$N" "$1"; }
bad()   { printf '   %s✗%s %s\n' "$R" "$N" "$1"; }

# Pretty print JSON if a tool is around, otherwise pass it through.
pretty() {
  if command -v jq >/dev/null 2>&1; then jq $JQC "$@" 2>/dev/null || cat
  elif command -v python3 >/dev/null 2>&1; then python3 -m json.tool 2>/dev/null || cat
  else cat; fi
}
# Extract one field without needing jq.
field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)" 2>/dev/null || echo ""; }

start_server() {
  "$WORK/queued" -wal "$WORK/queue.wal" -addr ":$PORT" >"$WORK/server.log" 2>&1 &
  SRV_PID=$!
  for _ in $(seq 1 50); do
    if curl -sf "$BASE/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  bad "server did not come up. Log:"; cat "$WORK/server.log"; exit 1
}

# ---------------------------------------------------------------- preflight

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  bad "port $PORT is already in use. Stop whatever is on it, or run: PORT=9000 $0"
  exit 1
fi

step "Building"
cd "$ROOT"
go build -o "$WORK/queued" .
go build -o "$WORK/demo" ./cmd/demo
ok "built, zero dependencies"

step "Starting the server"
start_server
ok "listening on :$PORT, log at a temp path so ./data is untouched"

# ------------------------------------------------------------ narrated tour

step "The demo application"
note "cmd/demo is a pure HTTP client. It imports no queue code, so everything"
note "it shows is available to any consumer over the wire."
echo
"$WORK/demo" -addr "$BASE"

# --------------------------------------------------------- API walkthrough

step "The same thing by hand, one call at a time"

cmd "POST /queues            create a delayed priority LIFO queue"
curl -s -XPOST "$BASE/queues" \
  -d '{"name":"walkthrough","mode":"lifo","priority":true,"max_attempts":3}' | pretty

cmd "POST /queues/walkthrough/messages   x3"
for m in '{"body":"normal job","priority":1}' \
         '{"body":"URGENT job","priority":9}' \
         '{"body":"later job","priority":9,"delay_ms":4000}'; do
  curl -s -XPOST "$BASE/queues/walkthrough/messages" -d "$m" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print("   enqueued  %-12s priority=%d seq=%d" % (d["body"], d["priority"], d["seq"]))'
done

cmd "GET /queues/walkthrough"
STATS=$(curl -s "$BASE/queues/walkthrough")
echo "$STATS" | pretty -c '{ready,delayed,inflight}'
note "two are candidates, one is behind the delay gate"

cmd "POST /queues/walkthrough/receive"
RESP=$(curl -s -XPOST "$BASE/queues/walkthrough/receive" -d '{"max":5}')
echo "$RESP" | python3 -c '
import json,sys
for m in json.load(sys.stdin)["messages"]:
    print("   got  %-12s priority=%d" % (m["body"], m["priority"]))'
note "URGENT first because priority outranks arrival, then the normal one."
note "\"later job\" is ALSO priority 9 and is absent, because it was not"
note "eligible. Sorting never saw it. That is the whole design."

RECEIPT=$(echo "$RESP" | field '["messages"][0]["receipt"]')

cmd "POST /queues/walkthrough/ack        (same receipt twice)"
printf '   first:  '; curl -s -XPOST "$BASE/queues/walkthrough/ack" -d "{\"receipt\":\"$RECEIPT\"}"; echo
printf '   again:  '; curl -s -XPOST "$BASE/queues/walkthrough/ack" -d "{\"receipt\":\"$RECEIPT\"}"; echo
note "a receipt is good for exactly one delivery"

step "Waiting out the delay"
note "4 seconds..."
sleep 4.5
cmd "POST /queues/walkthrough/receive"
curl -s -XPOST "$BASE/queues/walkthrough/receive" -d '{"max":5}' \
  | python3 -c '
import json,sys
ms = json.load(sys.stdin)["messages"]
for m in ms: print("   got  %-12s priority=%d" % (m["body"], m["priority"]))
print("   (nothing)" if not ms else "")'
note "the gate opened and now priority applies to it"

# ---------------------------------------------------------------- durability

step "Durability, proved with SIGKILL rather than a clean shutdown"

curl -s -XPOST "$BASE/queues" -d '{"name":"survive","mode":"lifo","priority":true}' >/dev/null
for i in 1 2 3; do
  curl -s -XPOST "$BASE/queues/survive/messages" -d "{\"body\":\"msg-$i\",\"priority\":$i}" >/dev/null
done
BEFORE=$(curl -s "$BASE/queues/survive" | field '["ready"]')
note "enqueued 3 messages, ready=$BEFORE"

cmd "kill -9   (no graceful shutdown, no flush, no cleanup)"
kill -9 "$SRV_PID" 2>/dev/null || true
wait "$SRV_PID" 2>/dev/null || true
SRV_PID=""
sleep 0.5
ok "process killed"

cmd "restart against the same log"
start_server
AFTER=$(curl -s "$BASE/queues/survive" | field '["ready"]')
ORDER=$(curl -s -XPOST "$BASE/queues/survive/receive" -d '{"max":10}' \
  | python3 -c 'import json,sys; print(", ".join(m["body"] for m in json.load(sys.stdin)["messages"]))')

if [ "$BEFORE" = "$AFTER" ] && [ "$ORDER" = "msg-3, msg-2, msg-1" ]; then
  ok "ready=$AFTER, delivery order: $ORDER"
  ok "messages, priorities and the LIFO config all survived a hard kill"
else
  bad "expected ready=$BEFORE and msg-3, msg-2, msg-1; got ready=$AFTER and: $ORDER"
  exit 1
fi

step "Done"
note "Everything above ran against a temporary log, now deleted."
note "To poke at it yourself:  go run .   then curl $BASE/queues"
echo
