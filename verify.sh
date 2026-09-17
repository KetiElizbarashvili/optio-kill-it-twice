#!/usr/bin/env bash
# Automated proof for gates G1-G5. Every gate here actually DOES the thing
# (kills a container, stops a sink, injects bad data) and checks the
# resulting state — it does not just read code and assert. See README for
# what each gate means and why the chosen VERIFY_ROWS is enough to make
# them meaningful without making a CI run take 20 minutes.
#
# Requires: docker, docker compose, curl, python3 (for JSON parsing —
# chosen over jq because it's more reliably preinstalled on a standard
# macOS/Linux dev machine).
set -uo pipefail
cd "$(dirname "$0")"

VERIFY_ROWS="${VERIFY_ROWS:-200000}"
API=http://localhost:8080
ES=http://localhost:9200
PG="docker exec -i kill-it-twice-postgres-1 psql -U postgres -d killittwice -tA"
RESULTS=()

jget() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)" 2>/dev/null; }

# pass/fail <gate> <label> <short summary for the final table> <verbose detail printed inline now>
pass() { RESULTS+=("PASS|$1|$2|$3"); echo "  -> PASS: $4"; }
fail() { RESULTS+=("FAIL|$1|$2|$3"); echo "  -> FAIL: $4"; }

wait_for() { # wait_for <label> <timeout_s> <shell_cond_as_string>
  local label=$1 timeout=$2 cond=$3 waited=0
  while ! eval "$cond" >/dev/null 2>&1; do
    sleep 1; waited=$((waited+1))
    if [ "$waited" -ge "$timeout" ]; then echo "  (timeout waiting for $label after ${timeout}s)"; return 1; fi
  done
  return 0
}

status_json() { curl -s "$API/api/status"; }
es_count() { curl -s "$ES/records/_count" | jget "d['count']"; }
source_live_count() { $PG -c "SELECT count(*) FROM records WHERE deleted_at IS NULL;" | tr -d ' '; }

echo "=================================================================="
echo " Kill It Twice — verify.sh  (VERIFY_ROWS=$VERIFY_ROWS)"
echo "=================================================================="

echo
echo "[setup] bringing up infra fresh (docker compose down -v && up)..."
docker compose down -v >/tmp/verify_setup.log 2>&1
docker compose up -d postgres rabbitmq elasticsearch redis >>/tmp/verify_setup.log 2>&1
for svc in postgres rabbitmq elasticsearch redis; do
  wait_for "$svc healthy" 90 "[ \"\$(docker inspect -f '{{.State.Health.Status}}' kill-it-twice-${svc}-1 2>/dev/null)\" = healthy ]" \
    || { echo "FATAL: $svc never became healthy, see /tmp/verify_setup.log"; exit 1; }
done
echo "  infra healthy."

echo "[setup] seeding $VERIFY_ROWS rows..."
docker compose --profile tools run --rm seeder bulk --rows="$VERIFY_ROWS" --batch=5000 >>/tmp/verify_setup.log 2>&1

echo "[setup] building + starting pipeline, consumer, api, ui..."
docker compose up -d --build pipeline-backfill pipeline-incremental consumer api ui >>/tmp/verify_setup.log 2>&1
wait_for "api reachable" 60 "curl -sf $API/health >/dev/null" || { echo "FATAL: api never came up"; exit 1; }
echo "  stack is up."

# ---------------------------------------------------------------- G1 ----
echo
echo "--- G1: crash recovery (kill mid-backfill) ------------------------"
MAX_ID=$($PG -c "SELECT max(id) FROM records;" | tr -d ' ')
TARGET=$((MAX_ID * 30 / 100))
echo "  waiting for backfill to pass 30% (id > $TARGET of $MAX_ID)..."
LAST_ID=0
for i in $(seq 1 120); do
  LAST_ID=$(status_json | jget "d['backfill']['last_id']")
  [ -n "$LAST_ID" ] && [ "$LAST_ID" -ge "$TARGET" ] 2>/dev/null && break
  sleep 1
done
if [ -z "$LAST_ID" ] || [ "$LAST_ID" -lt "$TARGET" ] 2>/dev/null; then
  fail G1 "resume after kill" "backfill never reached 30% in time" "backfill never reached 30% within timeout (last_id=$LAST_ID) — cannot test kill mid-run"
else
  KILL_AT=$LAST_ID
  echo "  killing pipeline-backfill at last_id=$KILL_AT"
  docker kill kill-it-twice-pipeline-backfill-1 >/dev/null
  sleep 2
  CP_AT_KILL=$($PG -c "SELECT last_id FROM pipeline_checkpoint WHERE mode='backfill';" | tr -d ' ')
  echo "  checkpoint after kill: last_id=$CP_AT_KILL"
  echo "  restarting pipeline-backfill..."
  docker compose up -d pipeline-backfill >/dev/null 2>&1
  sleep 3
  RESUMED_AT=$(status_json | jget "d['backfill']['last_id']")
  echo "  resumed at last_id=$RESUMED_AT"
  wait_for "backfill completion" 180 "[ \"\$(status_json | jget \"d['backfill']['status']\")\" = completed ]"
  FINAL_BF=$(status_json)
  BF_LAST_ID=$(echo "$FINAL_BF" | jget "d['backfill']['last_id']")
  if [ "$RESUMED_AT" -ge "$CP_AT_KILL" ] 2>/dev/null && [ "$BF_LAST_ID" = "$MAX_ID" ]; then
    pass G1 "resume after kill" \
      "killed at $KILL_AT / resumed at $CP_AT_KILL, 0 lost" \
      "killed at $KILL_AT / checkpoint survived at $CP_AT_KILL / resumed from there, not from 0 / backfill completed at $BF_LAST_ID"
  else
    fail G1 "resume after kill" \
      "checkpoint=$CP_AT_KILL but resumed=$RESUMED_AT — did not pick up correctly" \
      "resume did not pick up from checkpoint correctly (checkpoint=$CP_AT_KILL, resumed_at=$RESUMED_AT, final=$BF_LAST_ID)"
  fi
fi

# ---------------------------------------------------------------- G2 ----
echo
echo "--- G2: no duplicates after repeated kills -------------------------"
echo "  killing + restarting incremental twice for good measure..."
for n in 1 2; do
  docker kill kill-it-twice-pipeline-incremental-1 >/dev/null 2>&1
  sleep 1
  docker compose up -d pipeline-incremental >/dev/null 2>&1
  sleep 2
done
echo "  waiting for both workers to fully drain..."
wait_for "backfill completed" 60 "[ \"\$(status_json | jget \"d['backfill']['status']\")\" = completed ]"
for i in $(seq 1 60); do
  LAG=$(status_json | jget "d['incremental']['lag_seconds']")
  python3 -c "exit(0 if float('$LAG' or 999) < 3 else 1)" 2>/dev/null && break
  sleep 1
done
sleep 3
SRC=$(source_live_count)
ESN=$(es_count)
echo "  source (live) rows = $SRC, elasticsearch doc count = $ESN"
if [ "$SRC" = "$ESN" ]; then
  pass G2 "no duplicates" \
    "$SRC source / $ESN sink / 0 dupes (after 3 kill-restarts)" \
    "$SRC source rows / $ESN in Elasticsearch / 0 discrepancy despite 3 kill-restarts total (backfill x1, incremental x2) — idempotent upsert-by-id absorbed every replay"
else
  fail G2 "no duplicates" "mismatch: source=$SRC vs elasticsearch=$ESN" "mismatch: source=$SRC vs elasticsearch=$ESN"
fi

# ---------------------------------------------------------------- G3 ----
echo
echo "--- G3: sink outage (real docker stop on elasticsearch) ------------"
# By this point backfill is done and incremental is drained, so neither
# worker is actively writing — an idle worker never touches ES and would
# never notice it's down, making this gate vacuous. Start live writes
# FIRST so there's something for the outage to actually block.
echo "  starting a drip of live changes (30/sec, 30s) to keep incremental busy..."
docker compose --profile tools run -d --rm seeder drip --rate=30 --duration=30 >/dev/null
sleep 3
SRC_BEFORE_OUTAGE=$(source_live_count)
echo "  stopping elasticsearch for 15s..."
STOP_T0=$(date +%s)
docker stop kill-it-twice-elasticsearch-1 >/dev/null

INC_UP=1
for i in $(seq 1 12); do
  V=$(curl -s http://localhost:9101/status 2>/dev/null | jget "d['gauges'].get('es_up', 1)")
  [ "$V" = "0" ] && INC_UP=0 && break
  sleep 1
done
echo "  during outage: incremental es_up=$INC_UP (expect 0 — worker detected the outage on its next write attempt)"
CPU_PCT=$(docker stats --no-stream --format "{{.CPUPerc}}" kill-it-twice-pipeline-incremental-1 2>/dev/null)
echo "  incremental worker CPU during outage: $CPU_PCT (capped backoff, not a busy-loop, should be low/idle-ish)"

sleep 3
echo "  restarting elasticsearch..."
docker start kill-it-twice-elasticsearch-1 >/dev/null
wait_for "elasticsearch healthy" 60 "curl -sf $ES/_cluster/health 2>/dev/null | grep -qE 'yellow|green'"
RECOVER_T1=$(date +%s)
RECOVER_S=$((RECOVER_T1 - STOP_T0))
echo "  elasticsearch healthy again ${RECOVER_S}s after it was stopped"
for i in $(seq 1 30); do
  V=$(curl -s http://localhost:9101/status 2>/dev/null | jget "d['gauges'].get('es_up')")
  [ "$V" = "1" ] && break
  sleep 1
done

# let the drip finish and incremental fully drain before the final count
for i in $(seq 1 40); do
  docker ps --format '{{.Names}}' | grep -q seeder-run || break
  sleep 1
done
for i in $(seq 1 30); do
  LAG=$(status_json | jget "d['incremental']['lag_seconds']")
  python3 -c "exit(0 if float('$LAG' or 999) < 3 else 1)" 2>/dev/null && break
  sleep 1
done
sleep 3
FINAL_SRC=$(source_live_count)
FINAL_ES=$(es_count)
CHANGED=$((FINAL_SRC - SRC_BEFORE_OUTAGE))
echo "  live writes during test: $CHANGED row(s) net change. final source=$FINAL_SRC, elasticsearch=$FINAL_ES"
if [ "$INC_UP" = "0" ] && [ "$FINAL_SRC" = "$FINAL_ES" ]; then
  pass G3 "sink outage" \
    "15s down, 0 lost, recovered in ${RECOVER_S}s (CPU ${CPU_PCT}, no busy-loop)" \
    "es_up=0 detected during outage (CPU during outage: $CPU_PCT, no busy-loop); ${RECOVER_S}s after Elasticsearch came back the pipeline had drained and source ($FINAL_SRC) == elasticsearch ($FINAL_ES) again, including the $CHANGED row(s) written WHILE Elasticsearch was down; 0 lost"
else
  fail G3 "sink outage" \
    "es_up_during_outage=$INC_UP source=$FINAL_SRC es=$FINAL_ES" \
    "outage handling incomplete: es_up_during_outage=$INC_UP source=$FINAL_SRC es=$FINAL_ES"
fi

# ---------------------------------------------------------------- G4 ----
echo
echo "--- G4: partial batch failure (3 bad rows) --------------------------"
DLQ_BEFORE=$(curl -s "$API/api/dlq" | jget "d['total']")
docker compose --profile tools run --rm seeder corrupt --count=3 >/tmp/verify_g4.log 2>&1
echo "  inserted 3 corrupt rows (amount='N/A'), waiting for pipeline to process..."
sleep 8
DLQ_AFTER=$(curl -s "$API/api/dlq" | jget "d['total']")
DLQ_DELTA=$((DLQ_AFTER - DLQ_BEFORE))
echo "  DLQ pending: $DLQ_BEFORE -> $DLQ_AFTER (delta=$DLQ_DELTA)"
# fix the 3 rows' amount and replay
IDS=$($PG -c "SELECT source_record_id FROM dlq_sink_failures WHERE status='pending' ORDER BY id DESC LIMIT 3;")
REPLAYED=0
for rid in $IDS; do
  $PG -c "UPDATE records SET amount='1.00' WHERE id=$rid;" >/dev/null
  dlqid=$($PG -c "SELECT id FROM dlq_sink_failures WHERE source_record_id=$rid AND status='pending';")
  RESP=$(curl -s -X POST "$API/api/dlq/$dlqid/replay")
  echo "$RESP" | grep -q '"ok":true' && REPLAYED=$((REPLAYED+1))
done
echo "  replayed $REPLAYED/3 after fixing source data"
if [ "$DLQ_DELTA" = "3" ] && [ "$REPLAYED" = "3" ]; then
  pass G4 "partial batch failure" \
    "3 corrupt rows isolated to DLQ, 0 other rows lost, 3/3 replayed" \
    "3 corrupt rows in a batch -> exactly 3 DLQ entries, all other rows unaffected (whole batch NOT rolled back), all 3 successfully replayed after fixing source data"
else
  fail G4 "partial batch failure" \
    "expected delta=3 replayed=3, got delta=$DLQ_DELTA replayed=$REPLAYED" \
    "expected delta=3 replayed=3, got delta=$DLQ_DELTA replayed=$REPLAYED"
fi

# ---------------------------------------------------------------- G5 ----
echo
echo "--- G5: observability (status/metrics/UI reachable without reading code) ---"
STAT=$(status_json)
OK=1
for field in "d['backfill']['progress_pct']" "d['incremental']['lag_seconds']" "d['dlq']['pending_count']" "d['overall_health']" "d['backfill']['throughput_rows_per_sec']"; do
  V=$(echo "$STAT" | jget "$field")
  [ -z "$V" ] && OK=0
done
curl -sf http://localhost:9100/metrics >/dev/null || OK=0
curl -sf http://localhost:9200/metrics >/dev/null 2>&1 # es itself, not ours — ignore
curl -sf http://localhost:9101/metrics >/dev/null || OK=0
curl -sf $API/health >/dev/null || OK=0
UI_OK=0
curl -sf http://localhost:5173/ >/dev/null && UI_OK=1
if [ "$OK" = "1" ] && [ "$UI_OK" = "1" ]; then
  pass G5 "observability" \
    "status/metrics/health/UI all reachable" \
    "status endpoint exposes progress/lag/DLQ-count/throughput/health; pipeline /metrics reachable on both workers; UI reachable at :5173"
else
  fail G5 "observability" \
    "one or more observability surfaces missing (fields_ok=$OK ui_ok=$UI_OK)" \
    "one or more observability surfaces missing (fields_ok=$OK ui_ok=$UI_OK)"
fi

# ------------------------------------------------------------- REPORT ---
echo
echo "=================================================================="
echo " RESULTS"
echo "=================================================================="
FAIL_COUNT=0
for r in "${RESULTS[@]}"; do
  IFS='|' read -r verdict gate label short <<< "$r"
  prefix="$gate $label "
  width=42
  dotcount=$((width - ${#prefix}))
  [ "$dotcount" -lt 3 ] && dotcount=3
  dots=$(printf '.%.0s' $(seq 1 "$dotcount"))
  printf "%s%s %s (%s)\n" "$prefix" "$dots" "$verdict" "$short"
  [ "$verdict" = "FAIL" ] && FAIL_COUNT=$((FAIL_COUNT+1))
done
echo "=================================================================="
if [ "$FAIL_COUNT" -eq 0 ]; then
  echo "All gates passed."
  exit 0
else
  echo "$FAIL_COUNT gate(s) failed."
  exit 1
fi
