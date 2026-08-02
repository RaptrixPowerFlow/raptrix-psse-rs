#!/usr/bin/env bash
# raptrix-psse-rs — external golden corpus verification (mirrors tests/golden_test.rs).
#
# Requires a release binary and the licensed / local files under tests/data/external/
# (same paths as golden_test.rs). On Windows + OneDrive, run from WSL so the repo
# is reachable under /mnt/... and file locks are less problematic.
#
# Policy: when a DYR/DYN companion exists, the canonical plain ``<stem>.rpf`` is
# the *dynamic* conversion. ``*_dynamic.rpf`` is an alias; ``*_static.rpf`` is the
# no-DYR companion for explicit A/B. raptrix-core prefers dynamic by default.
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

# Exact stem .dyr/.dyn, else shortest ``<stem>_*.dyr|dyn`` (e.g. ACTIVSg10k_dynamics.dyr).
pick_dyn() {
  local base="$1"
  if [[ -f "${base}.dyn" ]]; then
    echo "${base}.dyn"
    return 0
  fi
  if [[ -f "${base}.dyr" ]]; then
    echo "${base}.dyr"
    return 0
  fi
  local best="" best_len=999999 cand
  for cand in "${base}"_*.dyr "${base}"_*.dyn; do
    [[ -f "$cand" ]] || continue
    local n
    n=$(basename "$cand")
    if (( ${#n} < best_len )); then
      best="$cand"
      best_len=${#n}
    fi
  done
  echo "$best"
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

# Convert RAW; attach DYR when present. Writes:
#   tests/golden/<out_stem>.rpf          (canonical — dynamic when DYR exists)
#   tests/golden/<out_stem>_dynamic.rpf  (alias, only when DYR used)
#   tests/golden/<out_stem>_static.rpf   (always; no-DYR when companion exists)
convert_case() {
  local label="$1" raw="$2" out_stem="$3"
  local dyr="${4:-}"
  require_file "$raw" || return 0

  local out="tests/golden/${out_stem}.rpf"
  local out_static="tests/golden/${out_stem}_static.rpf"
  local out_dynamic="tests/golden/${out_stem}_dynamic.rpf"
  local ms

  if [[ -n "$dyr" ]]; then
    require_file "$dyr" || return 0
    echo "[convert] dynamic (canonical): $label (+ $(basename "$dyr"))"
    ms=$(elapsed_ms "$BIN" convert --raw "$raw" --dyr "$dyr" --output "$out")
    echo "[timing] ${ms} ms  -> $out"
    cp -f "$out" "$out_dynamic"
    echo "[convert] static companion: $label"
    ms=$(elapsed_ms "$BIN" convert --raw "$raw" --output "$out_static")
    echo "[timing] ${ms} ms  -> $out_static"
  else
    echo "[convert] static (canonical): $label"
    ms=$(elapsed_ms "$BIN" convert --raw "$raw" --output "$out")
    echo "[timing] ${ms} ms  -> $out"
    cp -f "$out" "$out_static"
  fi
}

echo "[verify-external-golden] repo: $(pwd)"
echo "[verify-external-golden] RELAX_MISSING=$RELAX_MISSING"

# --- Corpus (keep in sync with tests/golden_test.rs discovery) ---

convert_case "Texas7k 2022" "tests/data/external/Texas7k_20220923.RAW" "Texas7k_20220923"
convert_case "Texas7k 2021" "tests/data/external/Texas7k_20210804.RAW" "Texas7k_20210804" \
  "$(pick_dyn tests/data/external/Texas7k_20210804)"
convert_case "Texas7k 2021 SAInt update" "tests/data/external/Texas7k_20210804_updated_SAInt.RAW" \
  "Texas7k_20210804_updated_SAInt"

convert_case "Texas2k summerpeak" "tests/data/external/Texas2k_series25_case1_summerpeak.RAW" \
  "Texas2k_series25_case1_summerpeak" \
  "$(pick_dyn tests/data/external/Texas2k_series25_case1_summerpeak)"
# Legacy short stem alias used by some core tests
if [[ -f "tests/golden/Texas2k_series25_case1_summerpeak.rpf" ]]; then
  cp -f "tests/golden/Texas2k_series25_case1_summerpeak.rpf" "tests/golden/Texas2k_series25.rpf"
  if [[ -f "tests/golden/Texas2k_series25_case1_summerpeak_dynamic.rpf" ]]; then
    cp -f "tests/golden/Texas2k_series25_case1_summerpeak_dynamic.rpf" \
      "tests/golden/Texas2k_series25_dynamic.rpf"
  fi
  if [[ -f "tests/golden/Texas2k_series25_case1_summerpeak_static.rpf" ]]; then
    cp -f "tests/golden/Texas2k_series25_case1_summerpeak_static.rpf" \
      "tests/golden/Texas2k_series25_static.rpf"
  fi
fi

convert_case "EI 515GW" "tests/data/external/Base_Eastern_Interconnect_515GW.RAW" \
  "Base_Eastern_Interconnect_515GW"

convert_case "ACTIVSg10k" "tests/data/external/ACTIVSg10k.RAW" "ACTIVSg10k" \
  "$(pick_dyn tests/data/external/ACTIVSg10k)"

for case in \
  Texas2k_series24_case1_2016summerPeak \
  Texas2k_series24_case2_2016lowload \
  Texas2k_series24_case3_2024summerpeak \
  Texas2k_series24_case4_2024lowload \
  Texas2k_series24_case6_2024lowloadwithgfm
do
  convert_case "Texas2k ${case}" "tests/data/external/${case}.RAW" "$case" \
    "$(pick_dyn "tests/data/external/${case}")"
done
# Legacy GFM short name
if [[ -f "tests/golden/Texas2k_series24_case6_2024lowloadwithgfm_dynamic.rpf" ]]; then
  cp -f "tests/golden/Texas2k_series24_case6_2024lowloadwithgfm_dynamic.rpf" \
    "tests/golden/Texas2k_series24_gfm_dynamic.rpf"
fi

convert_case "IEEE 14" "tests/data/external/IEEE_14_bus.raw" "IEEE_14_bus"
convert_case "IEEE 118" "tests/data/external/IEEE_118_Bus.RAW" "IEEE_118_Bus"
convert_case "NYISO offpeak 2019" "tests/data/external/NYISO_offpeak2019_v23.raw" "NYISO_offpeak2019_v23"
convert_case "NYISO onpeak 2019" "tests/data/external/NYISO_onpeak2019_v23.raw" "NYISO_onpeak2019_v23"
convert_case "NYISO onpeak 2030 PW" \
  "tests/data/external/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld.raw" \
  "NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld"
convert_case "Texas7k 2030" "tests/data/external/Texas7k_2030_20220923.RAW" "Texas7k_2030_20220923"
if [[ -f "tests/golden/Texas7k_2030_20220923.rpf" ]]; then
  cp -f "tests/golden/Texas7k_2030_20220923.rpf" "tests/golden/Texas7k_2030.rpf"
  cp -f "tests/golden/Texas7k_2030_20220923_static.rpf" "tests/golden/Texas7k_2030_static.rpf" 2>/dev/null || true
fi
convert_case "Midwest24k" "tests/data/external/Midwest24k_20220923.RAW" "Midwest24k_20220923"
if [[ -f "tests/golden/Midwest24k_20220923.rpf" ]]; then
  cp -f "tests/golden/Midwest24k_20220923.rpf" "tests/golden/Midwest24k.rpf"
  cp -f "tests/golden/Midwest24k_20220923_static.rpf" "tests/golden/Midwest24k_static.rpf" 2>/dev/null || true
fi
convert_case "ACTIVSg25k" "tests/data/external/ACTIVSg25k.RAW" "ACTIVSg25k"
convert_case "ACTIVSg70k" "tests/data/external/ACTIVSg70k.RAW" "ACTIVSg70k" \
  "$(pick_dyn tests/data/external/ACTIVSg70k)"

echo
echo "[verify-external-golden] OK — all conversions completed (dynamic canonical when DYR present)."
echo "[verify-external-golden] Run: cargo test --release --test golden_test -- --nocapture"
