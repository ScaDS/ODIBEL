#!/usr/bin/env bash
# Continue RDF-path generation after the Airport property_filter job finishes.
set -euo pipefail

ROOT=/u/hadena/marvin/code/ODIBEL
DATA=/local/d1/datasets/kgi-bench-text2kgbench
LOG=$DATA/logs
export JAVA_HOME=/u/hadena/.sdkman/candidates/java/17.0.16-tem
export PATH="$JAVA_HOME/bin:$PATH"
export MASTER='local[16]'
export SPARK_LOCAL_DIR=$DATA/spark-local
mkdir -p "$LOG" "$SPARK_LOCAL_DIR"

FILTER_PID=${1:-71615}

echo "[$(date -Is)] waiting for Airport filter pid=$FILTER_PID"
while kill -0 "$FILTER_PID" 2>/dev/null; do
  sleep 30
done
echo "[$(date -Is)] Airport filter finished; log tail:"
tail -40 "$LOG/ont3_generate.log" | tr '\r' '\n' | tail -40

cd "$ROOT"
echo "[$(date -Is)] Airport: subgraph → splits → sources (no pre-sample; splits select root entities)"
uv run resources/text2kgbench/run_domain.py airport --size 100 \
  --from-stage subgraph --skip-sample --engine spark \
  >"$LOG/airport_100_pipeline.log" 2>&1

echo "[$(date -Is)] CelestialBody: filter → subgraph → splits → sources"
uv run resources/text2kgbench/run_domain.py celestialbody --size 100 \
  --from-stage filter --skip-sample --engine spark \
  >"$LOG/celestialbody_100_pipeline.log" 2>&1

echo "[$(date -Is)] all done"
ls -la "$DATA/datasets" 2>/dev/null || true
python3 - <<'PY' || true
import json
from pathlib import Path
reg = Path("/local/d1/datasets/kgi-bench-text2kgbench/registry.json")
if reg.exists():
    data = json.loads(reg.read_text())
    for dataset_id in sorted(data.get("datasets", {})):
        print(dataset_id)
PY
