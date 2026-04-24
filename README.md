<!--
  raptrix-psse-rs
  Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# raptrix-psse-rs

PSS/E (`.raw` + `.dyr`) to Raptrix PowerFlow Interchange (`.rpf`) conversion — built for **large cases**, **deterministic** Arrow IPC output, and **modern grid** constructs (IBRs, rich metadata) while staying faithful to legacy PSS/E.

Part of the Raptrix PowerFlow ecosystem.

For production-scale deployments and the broader solver stack, contact **Raptrix PowerFlow** via the [GitHub organization](https://github.com/RaptrixPowerFlow).

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
- Always emit the **18** canonical v0.9.1 required root tables (zero-row where applicable) so downstream pipelines stay deterministic.

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

## RPF v0.9.1 coverage

The converter emits the **18** required root tables from the locked v0.9.1 contract (see [raptrix-cim-rs schema-contract](https://github.com/RaptrixPowerFlow/raptrix-cim-rs/blob/main/docs/schema-contract.md)), including:

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

Metadata includes modern-grid fields plus v0.9.0/0.9.1 **additional nullable columns** (left **null** for typical PSS/E planning exports when the source deck has no values for them):

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

The optional **`scenario_context`** root table is **not** written by default. The library API rejects non-empty `ExportOptions::scenario_context_rows` when optional-root IPC emission is unavailable in the linked `raptrix-cim-arrow` build (see crate error text).

## What's New in v0.3.4

- **Schema-alignment release** (patch **0.3.4**): align to RPF **v0.9.1** and populate `loads` ZIP fidelity columns (`p_i_pu`, `q_i_pu`, `p_y_pu`, `q_y_pu`) from RAW `IP/IQ/YP/YQ`, plus emit root metadata `rpf.loads.zip_fidelity_presence`.
- **CI on every PR**: `fmt`, `clippy`, and full `cargo test` on Ubuntu; public-safety, markdownlint, and version/CHANGELOG consistency checks (aligned with `raptrix-cim-rs` practice).
- **Safer releases**: the **Release** workflow runs **`cargo test --workspace`** before building Windows / Linux / macOS binaries.

See [CHANGELOG.md](CHANGELOG.md) for full per-release schema alignment and CI/release notes.

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

The **`golden_test`** integration suite (`tests/golden_test.rs`) converts every file in that corpus to v0.9.1 `.rpf` under `tests/golden/` (static where no dynamics deck exists; static **and** dynamic where `.dyr` / `.dyn` is present). Paths are fixed in the test source so CI can skip missing inputs without failing.

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

## Known fidelity limits (today’s export)

The converter aims for **predictable, contract-aligned** `.rpf` output. Like any interchange layer, **not every PSS/E field becomes a first-class column**—some are folded into aggregates, omitted when the RPF schema has no home, or left for consumers to interpret from raw dynamics rows. Authoritative per-field rules live in [`docs/psse-mapping.md`](docs/psse-mapping.md); highlights include:

- **Aggregates vs. raw fields**: e.g. line-end shunts feed bus `g_shunt` / `b_shunt`; loads export the PQ portion documented in the mapping doc.
- **Coverage matrix**: see **PSS/E RAW coverage** in the mapping doc for what is exported, folded into other tables, skipped by the parser, or blocked by interchange schema (ZIP loads, optional MTDC / node-breaker tables, etc.).
- **Parser coverage**: some RAW sections and rows are skipped or rejected with counts logged; see the mapping doc and `parser.rs` for current behavior.
- **Dynamics**: DYR numeric rows are preserved where parsed; attachment and interpretation follow the mapping doc—validate against your toolchain.

Golden tests (with local external inputs) help catch regressions; they are not a statement of future scope.

## License

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-blue.svg)](LICENSE)

MPL 2.0 - free to use, modify, and distribute.
