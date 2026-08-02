#!/usr/bin/env bash
# Regenerate golden-path .rpf files under tests/golden/ from tests/data/external/.
# Delegates to verify-external-golden.sh (dynamic is canonical when DYR exists).
# Repo-relative only (safe on any clone path). Prefer WSL on Windows + OneDrive.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=/dev/null
. "$HOME/.cargo/env" 2>/dev/null || true

echo "[build] cargo build --release"
cargo build --release

RELAX_MISSING="${RELAX_MISSING:-0}" ./scripts/verify-external-golden.sh

echo
echo "[suite] finished — RPF schema stamped by raptrix-cim-arrow (single IPC writer)"
echo "[suite] policy: canonical <stem>.rpf includes DYR when a companion exists"
