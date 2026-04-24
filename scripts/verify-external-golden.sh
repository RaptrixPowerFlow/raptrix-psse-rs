#!/usr/bin/env bash
# raptrix-psse-rs — external golden corpus verification (mirrors tests/golden_test.rs).
#
# Requires a release binary and the licensed / local files under tests/data/external/
# (same paths as golden_test.rs). On Windows + OneDrive, run from WSL so the repo
# is reachable under /mnt/... and file locks are less problematic.
#
# Usage (from repo root):
#   cargo build --release
#   ./scripts/verify-external-golden.sh
#
# Partial checkout (only some RAWs present):
#   RELAX_MISSING=1 ./scripts/verify-external-golden.sh
#
# Shellcheck: bashisms OK — target WSL2 / Linux.

set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=/dev/null
. "$HOME/.cargo/env" 2>/dev/null || true

RELAX_MISSING="${RELAX_MISSING:-0}"
BIN="./target/release/raptrix-psse-rs"

if [[ ! -f "$BIN" ]]; then
  echo "[verify-external-golden] missing $BIN — run: cargo build --release" >&2
  exit 1
fi

mkdir -p tests/golden

elapsed_ms() {
  local start end
  start=$(date +%s%N)
  "$@"
  end=$(date +%s%N)
  echo $(( (end - start) / 1000000 ))
}

pick_dyn() {
  local base="$1"
  if [[ -f "${base}.dyn" ]]; then
    echo "${base}.dyn"
  elif [[ -f "${base}.dyr" ]]; then
    echo "${base}.dyr"
  else
    echo ""
  fi
}

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    if [[ "$RELAX_MISSING" == "1" ]]; then
      echo "[skip] missing file: $path"
      return 1
    fi
    echo "[error] required file missing: $path (set RELAX_MISSING=1 to skip missing inputs)" >&2
    exit 1
  fi
  return 0
}

run_static() {
  local label="$1" raw="$2" out="$3"
  require_file "$raw" || return 0
  echo "[convert] static: $label"
  local ms
  ms=$(elapsed_ms "$BIN" convert --raw "$raw" --output "$out")
  echo "[timing] ${ms} ms  -> $out"
}

run_dynamic() {
  local label="$1" raw="$2" dyr="$3" out="$4"
  require_file "$raw" || return 0
  require_file "$dyr" || return 0
  echo "[convert] dynamic: $label (+ $(basename "$dyr"))"
  local ms
  ms=$(elapsed_ms "$BIN" convert --raw "$raw" --dyr "$dyr" --output "$out")
  echo "[timing] ${ms} ms  -> $out"
}

echo "[verify-external-golden] repo: $(pwd)"
echo "[verify-external-golden] RELAX_MISSING=$RELAX_MISSING"

# --- Corpus (keep in sync with tests/golden_test.rs) ---

run_static "Texas7k 2021" "tests/data/external/Texas7k_20210804.RAW" "tests/golden/Texas7k_20210804_static.rpf"
if [[ -f "tests/data/external/Texas7k_20210804.dyr" ]]; then
  run_dynamic "Texas7k 2021" "tests/data/external/Texas7k_20210804.RAW" \
    "tests/data/external/Texas7k_20210804.dyr" "tests/golden/Texas7k_20210804_dynamic.rpf"
elif [[ "$RELAX_MISSING" != "1" ]]; then
  echo "[error] missing tests/data/external/Texas7k_20210804.dyr" >&2
  exit 1
else
  echo "[skip] missing file: tests/data/external/Texas7k_20210804.dyr"
fi

run_static "Texas2k summerpeak" "tests/data/external/Texas2k_series25_case1_summerpeak.RAW" "tests/golden/Texas2k_series25_static.rpf"
if [[ -f "tests/data/external/Texas2k_series25_case1_summerpeak.dyr" ]]; then
  run_dynamic "Texas2k summerpeak" "tests/data/external/Texas2k_series25_case1_summerpeak.RAW" \
    "tests/data/external/Texas2k_series25_case1_summerpeak.dyr" "tests/golden/Texas2k_series25_dynamic.rpf"
elif [[ "$RELAX_MISSING" != "1" ]]; then
  echo "[error] missing tests/data/external/Texas2k_series25_case1_summerpeak.dyr" >&2
  exit 1
else
  echo "[skip] missing file: tests/data/external/Texas2k_series25_case1_summerpeak.dyr"
fi

run_static "EI 515GW" "tests/data/external/Base_Eastern_Interconnect_515GW.RAW" "tests/golden/Base_Eastern_Interconnect_515GW_static.rpf"

RAW_A10="tests/data/external/ACTIVSg10k.RAW"
if [[ -f "$RAW_A10" ]]; then
  run_static "ACTIVSg10k" "$RAW_A10" "tests/golden/ACTIVSg10k_static.rpf"
  DYR_A10="$(pick_dyn "tests/data/external/ACTIVSg10k")"
  if [[ -n "$DYR_A10" ]]; then
    run_dynamic "ACTIVSg10k" "$RAW_A10" "$DYR_A10" "tests/golden/ACTIVSg10k_dynamic.rpf"
  elif [[ "$RELAX_MISSING" != "1" ]]; then
    echo "[error] missing ACTIVSg10k.dyr or .dyn next to RAW" >&2
    exit 1
  else
    echo "[skip] missing ACTIVSg10k.dyr and .dyn"
  fi
elif [[ "$RELAX_MISSING" == "1" ]]; then
  echo "[skip] missing file: $RAW_A10"
else
  echo "[error] required file missing: $RAW_A10 (set RELAX_MISSING=1 to skip)" >&2
  exit 1
fi

RAW_GFM="tests/data/external/Texas2k_series24_case6_2024lowloadwithgfm.RAW"
if [[ -f "$RAW_GFM" ]]; then
  DYR_GFM="$(pick_dyn "tests/data/external/Texas2k_series24_case6_2024lowloadwithgfm")"
  if [[ -n "$DYR_GFM" ]]; then
    run_dynamic "Texas2k GFM" "$RAW_GFM" "$DYR_GFM" "tests/golden/Texas2k_series24_gfm_dynamic.rpf"
  elif [[ "$RELAX_MISSING" != "1" ]]; then
    echo "[error] missing GFM .dyr or .dyn" >&2
    exit 1
  else
    echo "[skip] missing GFM dynamics file"
  fi
elif [[ "$RELAX_MISSING" == "1" ]]; then
  echo "[skip] missing file: $RAW_GFM"
else
  echo "[error] required file missing: $RAW_GFM (set RELAX_MISSING=1 to skip)" >&2
  exit 1
fi

run_static "IEEE 14" "tests/data/external/IEEE_14_bus.raw" "tests/golden/IEEE_14_bus_static.rpf"
run_static "IEEE 118" "tests/data/external/IEEE_118_Bus.RAW" "tests/golden/IEEE_118_Bus_static.rpf"
run_static "NYISO offpeak 2019" "tests/data/external/NYISO_offpeak2019_v23.raw" "tests/golden/NYISO_offpeak2019_v23_static.rpf"
run_static "NYISO onpeak 2019" "tests/data/external/NYISO_onpeak2019_v23.raw" "tests/golden/NYISO_onpeak2019_v23_static.rpf"
run_static "NYISO onpeak 2030 PW" "tests/data/external/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld.raw" \
  "tests/golden/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld_static.rpf"
run_static "Texas7k 2030" "tests/data/external/Texas7k_2030_20220923.RAW" "tests/golden/Texas7k_2030_static.rpf"
run_static "Midwest24k" "tests/data/external/Midwest24k_20220923.RAW" "tests/golden/Midwest24k_static.rpf"
run_static "ACTIVSg25k" "tests/data/external/ACTIVSg25k.RAW" "tests/golden/ACTIVSg25k_static.rpf"
run_static "ACTIVSg70k" "tests/data/external/ACTIVSg70k.RAW" "tests/golden/ACTIVSg70k_static.rpf"

echo
echo "[verify-external-golden] OK — all conversions completed."
echo "[verify-external-golden] Run: cargo test --release --test golden_test -- --nocapture"
