<!--
  raptrix-psse-rs
  Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# raptrix-psse-rs

High-performance PSS/E (.raw + .dyr) to Raptrix PowerFlow Interchange (.rpf) conversion.

Part of the Raptrix PowerFlow ecosystem.

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
- Always emit canonical v0.8.8 required tables, even when zero-row, to keep downstream pipelines deterministic.

## CLI Reference

### convert

```bash
raptrix-psse-rs convert --raw <FILE> [--dyr <FILE>] --output <FILE> [--transformer-mode <MODE>] [--study-purpose <TEXT>] [--scenario-tag <TAG> ...]
```

| Flag | Required | Description |
|------|----------|-------------|
| `--raw <PATH>` | yes | PSS/E RAW file (.raw), versions 23-35. |
| `--dyr <PATH>` | no | Optional dynamic data file. Canonical format is `.dyr`; `.dyn` is accepted as fallback. |
| `--output <PATH>` | yes | Output RPF path. |
| `--transformer-mode <MODE>` | no | `native-3w` (default) or `expanded`. |
| `--study-purpose <TEXT>` | no | Metadata override for `metadata.study_purpose`. |
| `--scenario-tag <TAG>` | no | Repeatable metadata override for `metadata.scenario_tags`. |

### view

```bash
raptrix-psse-rs view --input <FILE>
```

Prints a summary of every table in the .rpf file with row counts.

## RPF v0.8.8 Coverage

The converter emits canonical tables including:

- metadata
- buses
- branches
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
- multi_section_lines
- dc_lines_2w
- ibr_devices
- contingencies
- interfaces
- dynamics_models

Metadata includes v0.8.8 modern-grid fields:

- modern_grid_profile
- ibr_penetration_pct
- has_ibr
- has_smart_valve
- has_multi_terminal_dc
- study_purpose
- scenario_tags

## What's New in v0.3.0

- **Enterprise-Grade Parser Robustness**: Hardened DC line and multi-section line parsing with malformed record detection and detailed logging.
- **Richer IBR Taxonomy**: Device classification now distinguishes `solar_pv`, `wind_type3`, `wind_type4`, `bess`, and `generic_ibr` with comprehensive DYR model family matching (DYR-first, WMOD fallback).
- **Comprehensive Test Coverage**: New synthetic RAW snippet test suite covering parser edge cases and regressions.
- **Production-Ready Release Pipeline**: GitHub Actions-driven binary builds for Windows, Linux, and macOS with automated release notes.

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

Place any confidential or licensed PSS/E input files under tests/data/external/, then run:

```bash
cargo test --release -- --nocapture
```

## License

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-blue.svg)](LICENSE)

MPL 2.0 - free to use, modify, and distribute.
