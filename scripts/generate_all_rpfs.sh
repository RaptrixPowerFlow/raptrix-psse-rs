#!/usr/bin/env bash
# Regenerate golden-path .rpf files under tests/golden/ from tests/data/external/.
# Repo-relative only (safe on any clone path). Prefer WSL on Windows + OneDrive.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=/dev/null
. "$HOME/.cargo/env" 2>/dev/null || true

echo "[build] cargo build --release"
cargo build --release

echo "[suite] starting conversions (mirrors tests/golden_test.rs + ACTIVSg10k static)"
BIN="./target/release/raptrix-psse-rs"
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
  if [[ -f "${base}.dyn" ]]; then echo "${base}.dyn"
  elif [[ -f "${base}.dyr" ]]; then echo "${base}.dyr"
  else echo ""; fi
}

run_static() {
  local raw="$1" out="$2"
  [[ -f "$raw" ]] || { echo "[skip] RAW not found: $raw"; return 0; }
  echo "[convert] static: $raw -> $out"
  local ms
  ms=$(elapsed_ms "$BIN" convert --raw "$raw" --output "$out")
  echo "[timing] static ${ms} ms"
}

run_dynamic() {
  local raw="$1" dyr="$2" out="$3"
  [[ -f "$raw" ]] || { echo "[skip] RAW not found: $raw"; return 0; }
  [[ -f "$dyr" ]] || { echo "[skip] DYR/DYN not found: $dyr"; return 0; }
  echo "[convert] dynamic: $raw + $dyr -> $out"
  local ms
  ms=$(elapsed_ms "$BIN" convert --raw "$raw" --dyr "$dyr" --output "$out")
  echo "[timing] dynamic ${ms} ms"
}

run_static "tests/data/external/Texas7k_20210804.RAW" "tests/golden/Texas7k_20210804_static.rpf"
run_dynamic "tests/data/external/Texas7k_20210804.RAW" "tests/data/external/Texas7k_20210804.dyr" "tests/golden/Texas7k_20210804_dynamic.rpf"

run_static "tests/data/external/Texas2k_series25_case1_summerpeak.RAW" "tests/golden/Texas2k_series25_static.rpf"
run_dynamic "tests/data/external/Texas2k_series25_case1_summerpeak.RAW" \
  "tests/data/external/Texas2k_series25_case1_summerpeak.dyr" "tests/golden/Texas2k_series25_dynamic.rpf"

# Canonical names used by raptrix-core / test-suite (no _static suffix).
run_static "tests/data/external/Base_Eastern_Interconnect_515GW.RAW" \
  "tests/golden/Base_Eastern_Interconnect_515GW.rpf"
# Keep _static alias for older scripts.
if [[ -f "tests/golden/Base_Eastern_Interconnect_515GW.rpf" ]]; then
  cp -f "tests/golden/Base_Eastern_Interconnect_515GW.rpf" \
    "tests/golden/Base_Eastern_Interconnect_515GW_static.rpf"
fi

RAW_A10="tests/data/external/ACTIVSg10k.RAW"
if [[ -f "$RAW_A10" ]]; then
  run_static "$RAW_A10" "tests/golden/ACTIVSg10k.rpf"
  cp -f "tests/golden/ACTIVSg10k.rpf" "tests/golden/ACTIVSg10k_static.rpf" 2>/dev/null || true
  dyr="$(pick_dyn "tests/data/external/ACTIVSg10k")"
  if [[ -n "$dyr" ]]; then
    run_dynamic "$RAW_A10" "$dyr" "tests/golden/ACTIVSg10k_dynamic.rpf"
  fi
fi

RAW_GFM="tests/data/external/Texas2k_series24_case6_2024lowloadwithgfm.RAW"
dyr_gfm="$(pick_dyn "tests/data/external/Texas2k_series24_case6_2024lowloadwithgfm")"
if [[ -f "$RAW_GFM" && -n "$dyr_gfm" ]]; then
  run_dynamic "$RAW_GFM" "$dyr_gfm" "tests/golden/Texas2k_series24_gfm_dynamic.rpf"
fi

run_static "tests/data/external/IEEE_14_bus.raw" "tests/golden/IEEE_14_bus_static.rpf"
run_static "tests/data/external/IEEE_118_Bus.RAW" "tests/golden/IEEE_118_Bus_static.rpf"
run_static "tests/data/external/NYISO_offpeak2019_v23.raw" "tests/golden/NYISO_offpeak2019_v23_static.rpf"
run_static "tests/data/external/NYISO_onpeak2019_v23.raw" "tests/golden/NYISO_onpeak2019_v23_static.rpf"
run_static "tests/data/external/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld.raw" \
  "tests/golden/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld_static.rpf"
run_static "tests/data/external/Texas7k_2030_20220923.RAW" "tests/golden/Texas7k_2030_static.rpf"
run_static "tests/data/external/Midwest24k_20220923.RAW" "tests/golden/Midwest24k_static.rpf"
run_static "tests/data/external/ACTIVSg25k.RAW" "tests/golden/ACTIVSg25k.rpf"
run_static "tests/data/external/ACTIVSg70k.RAW" "tests/golden/ACTIVSg70k.rpf"
# Aliases for legacy _static names
for stem in ACTIVSg25k ACTIVSg70k IEEE_14_bus IEEE_118_Bus NYISO_offpeak2019_v23 \
            NYISO_onpeak2019_v23 Midwest24k; do
  if [[ -f "tests/golden/${stem}.rpf" ]]; then
    cp -f "tests/golden/${stem}.rpf" "tests/golden/${stem}_static.rpf" 2>/dev/null || true
  elif [[ -f "tests/golden/${stem}_static.rpf" ]]; then
    cp -f "tests/golden/${stem}_static.rpf" "tests/golden/${stem}.rpf" 2>/dev/null || true
  fi
done
# Align IEEE/NYISO canonical names without _static if only _static exists from earlier lines
for pair in \
  "IEEE_14_bus_static.rpf:IEEE_14_bus.rpf" \
  "IEEE_118_Bus_static.rpf:IEEE_118_Bus.rpf" \
  "NYISO_offpeak2019_v23_static.rpf:NYISO_offpeak2019_v23.rpf" \
  "NYISO_onpeak2019_v23_static.rpf:NYISO_onpeak2019_v23.rpf" \
  "Texas2k_series25_static.rpf:Texas2k_series25_case1_summerpeak.rpf" \
  "Texas7k_20210804_static.rpf:Texas7k_20210804.rpf" \
  "Texas7k_2030_static.rpf:Texas7k_2030_20220923.rpf" \
  "Midwest24k_static.rpf:Midwest24k_20220923.rpf"
do
  src="${pair%%:*}"; dst="${pair##*:}"
  if [[ -f "tests/golden/$src" && ! -f "tests/golden/$dst" ]]; then
    cp -f "tests/golden/$src" "tests/golden/$dst"
  fi
done

# Additional goldens matching on-disk corpus names
run_static "tests/data/external/Texas7k_20220923.RAW" "tests/golden/Texas7k_20220923.rpf"
run_static "tests/data/external/Texas2k_series25_case1_summerpeak.RAW" \
  "tests/golden/Texas2k_series25_case1_summerpeak.rpf"

echo
echo "[suite] finished — RPF schema stamped by raptrix-cim-arrow (single IPC writer)"
