#!/usr/bin/env bash
#
# One command that checks everything and exits non zero if anything is wrong.
#
#   ./scripts/verify.sh
#
# Formatting, vet, build, the full suite under the race detector, and a live
# durability proof against a real server that gets SIGKILLed. Intended for a
# reviewer, human or agent, who wants a single verdict.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
WORK="$(mktemp -d)"
PORT="${PORT:-8791}"
FAILED=0

if [ -t 1 ]; then B=$(tput bold); D=$(tput dim); G=$(tput setaf 2); R=$(tput setaf 1); N=$(tput sgr0)
else B=""; D=""; G=""; R=""; N=""; fi

trap 'pkill -9 -f "$WORK/queued" 2>/dev/null; rm -rf "$WORK"' EXIT

check() {
  local name="$1"; shift
  printf '%s%-34s%s' "$B" "$name" "$N"
  local out
  if out="$("$@" 2>&1)"; then
    printf '%s PASS%s\n' "$G" "$N"
    [ -n "$out" ] && printf '%s\n' "$out" | sed 's/^/    /'
    return 0
  else
    printf '%s FAIL%s\n' "$R" "$N"
    printf '%s\n' "$out" | sed 's/^/    /'
    FAILED=1
    return 1
  fi
}

fmt_check() {
  local unformatted
  unformatted="$(gofmt -l . 2>&1)"
  if [ -n "$unformatted" ]; then echo "not gofmt'd:"; echo "$unformatted"; return 1; fi
}

count_tests() {
  echo "$(go test -list 'Test.*' ./... 2>/dev/null | grep -c '^Test') tests"
}

race_suite() {
  go test -race -count=1 ./... 2>&1 | grep -v '\[no test files\]'
}

# A real server, killed with SIGKILL so it gets no chance to flush, then
# restarted against the same log. This is the requirement the brief called
# "protected from application restarts", checked rather than asserted.
durability() {
  go build -o "$WORK/queued" . || return 1
  "$WORK/queued" -wal "$WORK/q.wal" -addr ":$PORT" >"$WORK/log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 50); do curl -sf "http://localhost:$PORT/healthz" >/dev/null 2>&1 && break; sleep 0.1; done

  curl -s -XPOST "http://localhost:$PORT/queues" \
    -d '{"name":"v","mode":"lifo","priority":true}' >/dev/null
  for i in 1 2 3; do
    curl -s -XPOST "http://localhost:$PORT/queues/v/messages" \
      -d "{\"body\":\"m$i\",\"priority\":$i}" >/dev/null
  done

  kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; sleep 0.4

  "$WORK/queued" -wal "$WORK/q.wal" -addr ":$PORT" >>"$WORK/log" 2>&1 &
  pid=$!
  for _ in $(seq 1 50); do curl -sf "http://localhost:$PORT/healthz" >/dev/null 2>&1 && break; sleep 0.1; done

  local got
  got="$(curl -s -XPOST "http://localhost:$PORT/queues/v/receive" -d '{"max":10}' \
    | python3 -c 'import json,sys; print(",".join(m["body"] for m in json.load(sys.stdin)["messages"]))')"
  kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

  if [ "$got" = "m3,m2,m1" ]; then
    echo "3 messages and the priority LIFO config survived SIGKILL"
  else
    echo "expected m3,m2,m1 after SIGKILL, got: ${got:-<nothing>}"; return 1
  fi
}

echo
printf '%sVerifying %s%s\n\n' "$B" "$ROOT" "$N"

check "go build"                 go build ./...
check "gofmt"                    fmt_check
check "go vet"                   go vet ./...
check "test count"               count_tests
check "go test -race"            race_suite
check "durability under SIGKILL" durability

echo
if [ "$FAILED" -eq 0 ]; then
  printf '%s%sAll checks passed.%s\n\n' "$B" "$G" "$N"
else
  printf '%s%sSomething failed. See above.%s\n\n' "$B" "$R" "$N"
fi
exit "$FAILED"
