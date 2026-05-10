# scripts/local_iterate

Local iterate-loop for **RPF generation quality** work targeting RAW vs RPF
convergence parity in [`raptrix-core`](../../../raptrix-core).

This directory is **gitignored** except for the script tree
(`run.ps1`, `robust_harness.py`, `parse_log.py`, this README, and the local
`.gitignore`). All generated artifacts (regenerated RPFs, harness logs, the
delta report) live under `out/` and are never committed.

## What it does

1. **Build** the converter via `cargo build --release` (uses
   `CARGO_TARGET_DIR=C:\temp\raptrix-psse-rs-target` to dodge OneDrive /
   Windows Defender Application Control issues).
2. **Convert** every `.RAW` / `.raw` under `tests/data/external/` into
   `out/rpf/<stem>_static.rpf` (or, when `-Cases` is set, only the matching
   stems are staged into `out/raw_subset/` and converted). Captures converter
   stderr per file in `out/logs/convert_stderr.log`.
3. **Run** the local `robust_harness.py` wrapper (a thin shim around
   raptrix-core's `kpf.parse_psse_raw` / `kpf.parse_rpf` / `NewtonRaphson`
   APIs that catches per-case parse / solve exceptions). The wrapper is
   resilient to bad RPFs (e.g. topology errors that block `parse_rpf`) so a
   single broken file cannot bail out the whole sweep — the upstream
   `pv_pq_harness.py` does fail fast on `parse_rpf` errors. Output appends
   to `out/logs/solver_regression.log` in the same JSONL schema as the
   upstream harness; importer warnings stream to
   `out/logs/harness_stderr.log`.
4. **Format** a Markdown delta report at `out/delta_report.md` that pairs
   RAW vs RPF runs by case stem and shows convergence + iteration / Q-switch
   / Q-violation deltas, plus a tally of importer / converter warnings.

## Usage

From the repo root or anywhere:

```powershell
# full run
pwsh scripts\local_iterate\run.ps1

# limit to specific case stems (no extension, no _static suffix)
pwsh scripts\local_iterate\run.ps1 -Cases ACTIVSg25k,Base_Eastern_Interconnect_515GW

# only re-run the harness against existing RPFs
pwsh scripts\local_iterate\run.ps1 -SkipBuild -SkipConvert

# label a snapshot (header tag in delta_report.md)
pwsh scripts\local_iterate\run.ps1 -LabelTag "after-fix-D2"
```

## Requirements

- `raptrix-core` checkout exists at `..\raptrix-core` relative to this repo.
- `raptrix-core/.venv/Scripts/python.exe` is present and `raptrix_powerflow`
  has been built in that venv (the harness imports it).
- `tests/data/external/` is populated with the RAW fixtures.
