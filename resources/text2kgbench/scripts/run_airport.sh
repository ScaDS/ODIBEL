set -euo pipefail

ROOT=/u/hadena/marvin/code/ODIBEL
DATA=/local/d1/datasets/kgi-bench-text2kgbench
LOG=$DATA/logs
export JAVA_HOME=/u/hadena/.sdkman/candidates/java/17.0.16-tem
export PATH="$JAVA_HOME/bin:$PATH"
export MASTER='local[16]'
export SPARK_LOCAL_DIR=$DATA/spark-local
mkdir -p "$LOG" "$SPARK_LOCAL_DIR"

uv run resources/text2kgbench/run_domain.py airport --size 100 \
  --from-stage subgraph --skip-sample --engine memory \
  2>&1 | tee "$LOG/airport_100_pipeline_memory.log"