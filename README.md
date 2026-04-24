<!--
  raptrix-psse-rs
  Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# raptrix-psse-rs

PSS/E (`.raw` + `.dyr`) to Raptrix PowerFlow Interchange (`.rpf`) conversion — engineered for large cases and deterministic Arrow IPC output.

Part of the Raptrix PowerFlow ecosystem.

For production-scale grids and the commercial Newton-Raphson solver stack, contact **Raptrix PowerFlow** via the [GitHub organization](https://github.com/RaptrixPowerFlow).

## Ecosystem Repos

- [raptrix-cim-rs](https://github.com/RaptrixPowerFlow/raptrix-cim-rs) - Unlimited-size CIM to RPF converter suite.
- [raptrix-psse-rs](https://github.com/RaptrixPowerFlow/raptrix-psse-rs) - Unlimited-size PSS/E to RPF converter.
- [raptrix-studio](https://github.com/RaptrixPowerFlow/raptrix-studio) - Free unlimited RPF viewer/editor.

## Quick Start

```bash
raptrix-psse-rs convert --raw my_case.raw --output my_case.rpf
raptrix-psse-rs convert --raw my_case.raw --dyr my_case.dyr --output my_case_dynamic.rpf
raptrix-psse-rs convert --raw my_case.raw --output my_case_expanded.rpf --transformer-mode expanded
raptrix-psse-rs view --input my_case.rpf
```

## Modern Grid Support Philosophy

The converter is built for modern 2026+ studies while preserving strong legacy PSS/E compatibility.

- Prefer explicit modern-grid representations over lossy legacy flattening.
- Use DYR model families as the primary source for IBR classification and controls.
- Fall back to RAW WMOD where DYR is unavailable.
- Always emit the **18** canonical v0.9.0 required root tables (zero-row where applicable) so downstream pipelines stay deterministic.

## CLI Reference

### convert

```bash
raptrix-psse-rs convert --raw <FILE> [--dyr <FILE>] --output <FILE> [--transformer-mode <MODE>] [--study-purpose <TEXT>] [--scenario-tag <TAG> ...] [--case-mode <MODE>]
```

| Flag | Required | Description |
|------|----------|-------------|
| `--raw <PATH>` | yes | PSS/E RAW file (.raw), versions 23-35. |
| `--dyr <PATH>` | no | Optional dynamic data file. Canonical format is `.dyr`; `.dyn` is accepted as fallback. |
| `--output <PATH>` | yes | Output RPF path. |
| `--transformer-mode <MODE>` | no | `native-3w` (default) or `expanded`. |
| `--study-purpose <TEXT>` | no | Metadata override for `metadata.study_purpose`. |
| `--scenario-tag <TAG>` | no | Repeatable metadata override for `metadata.scenario_tags`. |
| `--case-mode <MODE>` | no | Optional override for `metadata.case_mode` / root `rpf.case_mode`. Allowed: `flat_start_planning`, `warm_start_planning`, `solved_snapshot`, `hour_ahead_advisory`. If omitted, flat vs warm start is inferred from RAW bus voltages. |

### view

```bash
raptrix-psse-rs view --input <FILE>
```

Prints a summary of every table in the .rpf file with row counts.

## RPF v0.9.0 coverage

The converter emits the **18** required root tables from the locked v0.9.0 contract (see [raptrix-cim-rs schema-contract](https://github.com/RaptrixPowerFlow/raptrix-cim-rs/blob/main/docs/schema-contract.md)), including:

- metadata
- buses
- branches
- multi_section_lines
- dc_lines_2w
- generators
- loads
- fixed_shunts
- switched_shunts
- switched_shunt_banks
- transformers_2w
- transformers_3w
- areas
- zones
- owners
- contingencies
- interfaces
- dynamics_models

IBR modeling is **only** on `generators` (`is_ibr`, `ibr_subtype`); the legacy `ibr_devices` table is not emitted.

Metadata includes modern-grid fields plus v0.9.0 nullable Sentinel-readiness columns (left **null** for normal PSS/E planning exports—PSS/E does not carry hour-ahead or SE semantics):

- modern_grid_profile
- ibr_penetration_pct
- has_ibr
- has_smart_valve
- has_multi_terminal_dc
- study_purpose
- scenario_tags
- hour_ahead_uncertainty_band
- commitment_source
- solver_q_limit_infeasible_count
- pv_to_pq_switch_count
- real_time_discovery

The optional **`scenario_context`** root table (Sentinel) is **not** written by default. The library API rejects non-empty `ExportOptions::scenario_context_rows` until `raptrix-cim-arrow` exposes optional-root wiring for that table.

## What's New in v0.3.2

- **Release numbering**: **0.3.2** is the public follow-on to **0.3.1** (same RPF v0.9.0 output shape); use this version for new tags and downloads.
- **CI on every PR**: `fmt`, `clippy`, and full `cargo test` on Ubuntu; public-safety, markdownlint, and version/CHANGELOG consistency checks (aligned with `raptrix-cim-rs` practice).
- **Safer releases**: the **Release** workflow runs **`cargo test --workspace`** before building Windows / Linux / macOS binaries.

See **v0.3.1** in [CHANGELOG.md](CHANGELOG.md) for the detailed RPF v0.9.0 schema alignment notes (tables, metadata, `case_mode`, `scenario_context` API).

See [CHANGELOG.md](CHANGELOG.md) for full release history and [MIGRATION.md](MIGRATION.md) for schema version notes.

## Releases & Downloads

Precompiled binaries are available on the [Releases page](https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases):

- **Windows (x86_64)**: `.exe` binary in `zip` archive.
- **Linux (x86_64)**: Static binary in `tar.gz` archive.
- **macOS (arm64)**: Native Apple Silicon binary in `tar.gz` archive.

To use a release binary, extract the archive and run:

```bash
./raptrix-psse-rs convert --raw my_case.raw --dyr my_case.dyr --output my_case.rpf
```

## Build From Source

Rust 1.85+ is required.

```bash
git clone https://github.com/RaptrixPowerFlow/raptrix-cim-rs.git
git clone https://github.com/RaptrixPowerFlow/raptrix-psse-rs.git
cd raptrix-psse-rs
cargo build --release
```

## Testing

Place any confidential or licensed PSS/E input files under `tests/data/external/`, then run:

```bash
cargo test --release -- --nocapture
```

The **`golden_test`** integration suite (`tests/golden_test.rs`) converts every file in that corpus to v0.9.0 `.rpf` under `tests/golden/` (static where no dynamics deck exists; static **and** dynamic where `.dyr` / `.dyn` is present). Paths are fixed in the test source so CI can skip missing inputs without failing.

### Windows, OneDrive, and WSL

Some Windows setups block or slow direct access to large files under OneDrive. The same pattern as [raptrix-cim-rs](https://github.com/RaptrixPowerFlow/raptrix-cim-rs) applies: run Rust through **WSL Ubuntu** so the repo is under `/mnt/...` and file access matches Linux expectations.

```powershell
# From repo root — same helper as raptrix-cim-rs
.\scripts\test-wsl.ps1 -CargoCommand "test --workspace --release --test golden_test -- --nocapture"
```

### Maintainer verification (full corpus)

After `cargo build --release`, regenerate or check all golden outputs (mirrors `golden_test` paths):

```bash
./scripts/verify-external-golden.sh
```

- **Strict by default**: every input listed in the script must exist (set `RELAX_MISSING=1` to only run what is present).
- On Windows, run that script **inside WSL** from the repo’s `/mnt/...` path, or use:

```powershell
.\scripts\verify.ps1 -ExternalGolden   # fmt + clippy + test, then WSL verify script
```

An optional GitHub Action **External golden (optional)** (`external-golden.yml`) runs the same script with `RELAX_MISSING=1` so the workflow stays green on hosted runners without licensed data.

## Performance snapshot

End-to-end timings are **parse RAW (+ optional DYR) + build Arrow tables + write `.rpf`**, measured inside `golden_test` with `Instant` (or the CLI for spot checks), **release** build, **April 2026**, on a typical developer machine (Windows, OneDrive-backed tree). **WSL** on `/mnt/c/...` is often **noticeably slower** for the same conversions (disk latency); the table below reflects **native Windows** `golden_test` unless noted.

| Case | Mode | Approx. wall time |
|------|------|-------------------|
| IEEE 14-bus | static | ~26 ms |
| IEEE 118-bus | static | ~28 ms |
| Texas2k (2.7k buses) | static | ~45 ms |
| Texas2k (2.7k buses) | + DYR | ~190 ms |
| NYISO ~1.5k-bus snapshots | static | ~70–85 ms |
| Texas7k (~6.7k buses) | static | ~170 ms |
| Texas7k 2030 | static | ~190 ms |
| Texas7k | + DYR | ~210 ms |
| ACTIVSg10k (~10k buses) | + DYR (CLI) | ~1.5 s |
| ACTIVSg25k | static | ~410 ms |
| Midwest24k | static | ~490 ms |
| ACTIVSg70k | static | ~990 ms |
| Eastern Interconnect 515GW | static | ~1.1 s |

These are **local engineering reference numbers**, not vendor benchmarks. Use them to spot regressions between commits; re-run `cargo test --release --test golden_test -- --nocapture` or `./scripts/verify-external-golden.sh` on your host to refresh.

## Solver and RPF completeness (known gaps)

PSS/E → RPF conversion is faithful for parsed sections, but some source data is **not ingested** or is **simplified** today. Downstream solvers should treat the following as limitations unless you augment the RPF elsewhere:

- **`write_psse_to_rpf` roadmap notes**: the rustdoc “C++ port TODO” block is a parity checklist (vector groups, richer DYR families, etc.); **core** `p_sched` / `q_sched` / shunt aggregation is implemented via `build_bus_aggregates` and is described in [`docs/psse-mapping.md`](docs/psse-mapping.md) — treat the checklist as *not exhaustive* of current code.
- **Three-winding star impedance**: `transformers_3w` uses placeholder **winding1 R/X** until per-winding decomposition is implemented (`lib.rs` TODO near the 3W export).
- **RAW sections skipped by the parser**: induction machines (`InductionMachine`); PSS/E v35 **SYSTEM-WIDE DATA** and **SYSTEM SWITCHING DEVICE** blocks are skipped (see `parser.rs`). Unsupported or malformed DC and multi-section rows are counted and logged, not silently accepted.
- **Fields documented as not stored**: e.g. bus **EVHI/EVLO** and other entries marked *(not stored)* in [`docs/psse-mapping.md`](docs/psse-mapping.md) have no v0.9.0 column — they are dropped on import.
- **Dynamics**: DYR coverage focuses on supported machine / IBR model families; records outside that set are not mapped into `dynamics_models` (see mapping doc and parser logs).

When in doubt, cross-check [`docs/psse-mapping.md`](docs/psse-mapping.md) and the golden tests for your case class.

## License

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-blue.svg)](LICENSE)

MPL 2.0 - free to use, modify, and distribute.
