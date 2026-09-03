#!/usr/bin/env bash
# Full TB check: login, device list (PG), latest (latest_cf), history (ts_kv_cf).
set -euo pipefail
TB_URL="${TB_URL:-http://localhost:8080}"
TB_USER="${TB_USER:?set TB_USER}"
TB_PASS="${TB_PASS:?set TB_PASS}"
DEVICE_ID="${DEVICE_ID:?set DEVICE_ID to a device with migrated telemetry}"
START_TS="${START_TS:?set START_TS ms (newest partition min)}"
END_TS="${END_TS:-$(date +%s)000}"

TOKEN=$(curl -sf -X POST "$TB_URL/api/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$TB_USER\",\"password\":\"$TB_PASS\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
echo "login OK"
curl -sf -H "X-Authorization: Bearer $TOKEN" "$TB_URL/api/device/$DEVICE_ID" > /dev/null
echo "device OK (PG)"
curl -sf -H "X-Authorization: Bearer $TOKEN" \
  "$TB_URL/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries?keys=temperature" | python3 -c 'import sys,json;d=json.load(sys.stdin);assert d.get("temperature"),"latest empty";print("latest OK",d["temperature"][0]["ts"])'
curl -sf -H "X-Authorization: Bearer $TOKEN" \
  "$TB_URL/api/plugins/telemetry/DEVICE/$DEVICE_ID/values/timeseries?keys=temperature&startTs=$START_TS&endTs=$END_TS&limit=10" | python3 -c 'import sys,json;d=json.load(sys.stdin);assert d.get("temperature"),"history empty";print("history OK rows=",len(d["temperature"]))'
echo ALL_CHECKS_PASSED
