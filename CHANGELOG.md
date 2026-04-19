<!--
  raptrix-psse-rs
  Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# Changelog

All notable changes to raptrix-psse-rs are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.3.0] - 2026-04-19

### Major Features

- **Stricter Parser Robustness**: Hardened parsing for DC lines and multi-section lines with malformed record detection and informative logging.
- **Richer IBR Classification**: Enhanced device taxonomy distinguishes `solar_pv`, `wind_type3`, `wind_type4`, `bess`, and `generic_ibr` with comprehensive DYR model family matching.
- **Enterprise-Grade Parser Tests**: New synthetic RAW snippet test suite covering edge cases and regression prevention for DC/multi-section/malformed records.

### Improvements

- **Parser token extraction**: Robust token parsing with improved endpoint detection to reduce false positives from numeric fields.
- **Numeric field scanning**: Position-agnostic extraction of DC/MSL parameters after endpoint validation.
- **Malformed row accounting**: Parser now reports counts of skipped malformed/unsupported DC and multi-section line rows.
- **Control mode hints**: Explicit handling of grid-forming (GFM) and VSM indicators in IBR classification.
- **Documentation**: Clearer `--dyr`-canonical / `.dyn`-fallback messaging in CLI and user-facing docs.

### Schema Alignment

- Confirmed compliance with RPF v0.8.8 including new tables (`dc_lines_2w`, `multi_section_lines`, `switched_shunt_banks`, `ibr_devices`) and modern-grid metadata.

### Fixed

- Removed dead code helper function flagged by compiler.
- Deprecated v0.8.7 references in mapping documentation; forward-only v0.8.8 baseline.

### Dependencies

- Rust 1.85+ required (2024 edition).
- Arrow 58.0 for RecordBatch serialization.
- `raptrix-cim-arrow` from main branch (canonical RPF schema support).

---

## [0.2.2] - 2026-04-18

### Features

- **RPF v0.8.8 Schema Upgrade**: Canonical RPF contract now v0.8.8 with four new required tables:
  - `multi_section_lines` — bundled line groupings with section impedance.
  - `dc_lines_2w` — two-terminal DC line definitions (LCC/VSC).
  - `switched_shunt_banks` — capacitor/reactor bank aggregates with per-step curves.
  - `ibr_devices` — inverter-based resource metadata (DYR-derived).

- **Modern-Grid Metadata**: Metadata table extended with:
  - `modern_grid_profile` — human-readable descriptor (e.g., "2026-grid-forming").
  - `ibr_penetration_pct` — renewable/inverter share estimate.
  - `has_ibr`, `has_smart_valve`, `has_multi_terminal_dc` — boolean feature flags.
  - `study_purpose` — CLI-overridable study type.
  - `scenario_tags` — repeatable metadata tags.

- **Branches enrichment**: Parent linkage for multi-section lines:
  - `parent_line_id`, `section_index` — nullable columns for section tracking.

- **DYR-First IBR Derivation**: IBR classification prioritizes DYR model families over RAW WMOD field.

- **CLI Metadata Overrides**:
  - `--study-purpose <TEXT>` — study type override.
  - `--scenario-tag <TAG>` — repeatable tag insertion.

### Improvements

- Switched shunt export now splits steps by bank for solver clarity.
- Parser logs all section-level statistics and rejection counts.
- Deterministic zero-row table emission ensures downstream reproducibility.

### Fixed

- Arrow schema nullability alignment for `metadata.scenario_tags` and `multi_section_lines.section_branch_ids`.
- Cargo.lock revision verification to reflect v0.8.8 contract.

### Schema Migration

See [MIGRATION.md](MIGRATION.md#v020-v023-canonical-rpf-v088-sync) for full details.

---

## [0.2.1] - 2026-04-15

### Features

- **Transformer Representation Invariants**: Single canonical mode per export run:
  - `native_3w` (default) — export 3-winding devices as native `transformers_3w` rows.
  - `expanded` — export 3-winding devices as star-expanded 2-winding legs.
  - Hard fail on ambiguous overlap.

- **Synthetic Star Bus Handling**: Buses ID > 10 000 000 reserved for star expansion; omitted from `buses` table in expanded mode.

- **Metadata Representation Mode**: Root metadata includes `rpf.transformer_representation_mode` for deterministic downstream interpretation.

### Improvements

- CLI flag `--transformer-mode native-3w | expanded` (default: `native-3w`).
- Parser robustness for legacy PSS/E variants.
- Improved error messaging for conflicting transformer materializations.

---

## [0.2.0] - 2026-04-12

### Features

- **Planning-vs-Solved Semantics**: Metadata table fields document case mode and solver provenance.
  - `case_mode = "flat_start_planning"` for all RAW exports.
  - Voltage setpoints (`v_mag_set`, `v_ang_set`) now represent planning flat-start values.
  - Solver fields (`solver_version`, `solver_iterations`, `solver_accuracy`) null for planning cases.

- **Voltage Setpoint Corrections**:
  - `v_mag_set`: Valid generator VS (0.85–1.15 pu) used for PV buses; fallback to 1.0 pu (not snapshot VM).
  - `v_ang_set`: Always 0.0 rad (flat-start), not snapshot VA.

### Improvements

- Per-bus reactive capability aggregation (`q_min`, `q_max`).
- Per-bus active range aggregation (`p_min_agg`, `p_max_agg`).
- Fixed shunt aggregation into `buses.g_shunt` and `buses.b_shunt`.

### Fixed

- Backward compatibility warning: v0.8.3 files with incorrect voltage planning values should be regenerated.

---

## [0.1.0] - 2026-04-10

### Initial Release

- **PSS/E RAW Import**: Sections 0–7, 13, 15, 17 (buses, loads, generators, branches, transformers, areas, zones, owners).
- **PSS/E DYR Import**: Full numeric preservation of dynamic models in `dynamics_models` table.
- **RPF v0.8.6 Export**: Canonical Raptrix PowerFlow Interchange format.
- **Generator Models**: Support for `GENROU`, `GENROE`, `GENSAL`, `GENSAE`, `GENCLS` with inertia (`h`), damping (`D`), and transient reactance (`xd_prime`).
- **Memory-Mapped Parsing**: Zero-copy line iteration via `memmap2`.
- **Comprehensive Testing**: Golden regression suite with IEEE and Texas test cases.

---

## Release Instructions

To create and publish a release:

```bash
# 1. Update version in Cargo.toml and this CHANGELOG
#    (following semantic versioning)

# 2. Commit changes
git add Cargo.toml CHANGELOG.md
git commit -m "chore: bump to v0.3.0"

# 3. Create an annotated tag
git tag -a v0.3.0 -m "Release v0.3.0: parser robustness + richer IBR classification"

# 4. Push commits and tags
git push origin main
git push origin v0.3.0
```

The GitHub Actions `release` workflow will automatically:
- Trigger on tag push (`v*.*.*` pattern).
- Build binaries for Windows (x86_64), Linux (x86_64), and macOS (arm64).
- Create a GitHub Release with auto-generated release notes.
- Attach platform-specific executables and source archives.

---

[0.3.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.0
[0.2.2]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.2
[0.2.1]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.1
[0.2.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.0
[0.1.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.1.0
