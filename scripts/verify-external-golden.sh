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

# Discover every RAW under tests/data/external. The file list stays on disk.
declare -A seen_raw=()
converted=0
shopt -s nullglob
for raw in tests/data/external/*; do
  [[ -f "$raw" ]] || continue
  ext="${raw##*.}"
  ext_lc=$(printf '%s' "$ext" | tr '[:upper:]' '[:lower:]')
  [[ "$ext_lc" == "raw" ]] || continue
  key=$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')
  [[ -n "${seen_raw[$key]:-}" ]] && continue
  seen_raw[$key]=1
  stem=$(basename "$raw")
  stem="${stem%.*}"
  convert_case "$stem" "$raw" "$stem" "$(pick_dyn "${raw%.*}")"
  converted=$((converted + 1))
done
if [[ "$converted" -eq 0 && "$RELAX_MISSING" != "1" ]]; then
  echo "[error] no RAW files under tests/data/external (set RELAX_MISSING=1 to allow an empty corpus)" >&2
  exit 1
fi

echo
echo "[verify-external-golden] OK — all conversions completed (dynamic canonical when DYR present)."
echo "[verify-external-golden] Run: cargo test --release --test golden_test -- --nocapture"
