#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
. "$HOME/.cargo/env" || true
echo "[build] cargo build --release"
cargo build --release

echo "[suite] starting conversions"
# Each entry: RAW||DYR (empty if none)||OUT_BASE
CASES=(
  "/mnt/c/Users/matth/OneDrive/repos/raptrix-core/python_tests/test_networks/Texas7k/Texas7k_20210804.RAW||tests/data/external/Texas7k_20210804.dyr||tests/golden/Texas7k_20210804"
  "tests/data/external/Texas2k_series25_case1_summerpeak.RAW||tests/data/external/Texas2k_series25_case1_summerpeak.dyr||tests/golden/Texas2k_series25"
  "tests/data/external/Base_Eastern_Interconnect_515GW.RAW||||tests/golden/Base_Eastern_Interconnect_515GW"
  "tests/data/external/IEEE_14_bus.raw||||tests/golden/IEEE_14_bus"
  "tests/data/external/IEEE_118_Bus.RAW||||tests/golden/IEEE_118_Bus"
  "tests/data/external/NYISO_offpeak2019_v23.raw||||tests/golden/NYISO_offpeak2019_v23"
  "tests/data/external/NYISO_onpeak2019_v23.raw||||tests/golden/NYISO_onpeak2019_v23"
  "tests/data/external/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld.raw||||tests/golden/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld"
  "tests/data/external/Texas7k_2030_20220923.RAW||||tests/golden/Texas7k_2030"
  "tests/data/external/Midwest24k_20220923.RAW||||tests/golden/Midwest24k"
  "tests/data/external/ACTIVSg25k.RAW||||tests/golden/ACTIVSg25k"
  "tests/data/external/ACTIVSg70k.RAW||||tests/golden/ACTIVSg70k"
)

for entry in "${CASES[@]}"; do
  RAW="${entry%%||*}"
  rest="${entry#*||}"
  DYR="${rest%%||*}"
  OUTBASE="${rest#*||}"
  if [ ! -f "$RAW" ]; then
    echo "[skip] RAW not found: $RAW"
    continue
  fi
  if [ -n "$DYR" ] && [ -f "$DYR" ]; then
    has_dyr=1
  else
    has_dyr=0
  fi

  OUT_STATIC="${OUTBASE}_static.rpf"
  echo
  echo "[convert] static: $RAW -> $OUT_STATIC"
  start=$(date +%s%3N)
  ./target/release/raptrix-psse-rs convert --raw "$RAW" --output "$OUT_STATIC"
  end=$(date +%s%3N)
  elapsed=$((end-start))
  if [ "$elapsed" -lt 0 ]; then elapsed=0; fi
  echo "[timing] static ${OUT_STATIC}: ${elapsed} ms"
  echo "[view] $OUT_STATIC"
  ./target/release/raptrix-psse-rs view --input "$OUT_STATIC" || true

  if [ "$has_dyr" -eq 1 ]; then
    OUT_DYN="${OUTBASE}_dynamic.rpf"
    echo
    echo "[convert] dynamic: $RAW + $DYR -> $OUT_DYN"
    start=$(date +%s%3N)
    ./target/release/raptrix-psse-rs convert --raw "$RAW" --dyr "$DYR" --output "$OUT_DYN"
    end=$(date +%s%3N)
    elapsed=$((end-start))
    if [ "$elapsed" -lt 0 ]; then elapsed=0; fi
    echo "[timing] dynamic ${OUT_DYN}: ${elapsed} ms"
    echo "[view] $OUT_DYN"
    ./target/release/raptrix-psse-rs view --input "$OUT_DYN" || true
  fi
done

echo
echo "[suite] finished"
exit 0
