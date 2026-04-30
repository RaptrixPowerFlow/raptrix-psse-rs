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

## [Unreleased]

---

## [0.3.7] - 2026-04-30

### Fixed

- **Release automation resilience**: auto-tag workflow now reacts to `CHANGELOG.md` updates as well as `Cargo.toml`, so version-sync fixes can automatically retrigger release artifact publishing.

---

## [0.3.6] - 2026-04-30

### Changed

- **Schema v0.9.3 nominal-kV alignment**: `branches`, `transformers_2w`, and `transformers_3w` nominal-kV columns now export as required non-null values.
- **Nominal-kV fallback policy**: transformer nominal-kV uses RAW `NOMV*` when valid, then bus nominal-kV fallback; expanded star-leg rows can use opposite-side bus nominal-kV when the synthetic star bus has no base-kV row.
- **Fail-fast export semantics**: conversion now errors when required nominal-kV cannot be resolved for v0.9.3 contract columns.

### Tests

- Added contract and representation assertions for non-null positive nominal-kV fields across branches and transformer tables.
- Added smoke coverage for nominal-kV fallback behavior when a transformer side lacks valid `NOMV`.

### Documentation

- Updated `README.md`, `MIGRATION.md`, and `docs/psse-mapping.md` for v0.9.3 required nominal-kV semantics.

---

## [0.3.5] - 2026-04-29

### Added

- **Required generator reactive schedule export**: `generators.q_sched_mvar` is now always exported from PSS/E RAW `QG` (MVAr), aligning with schema v0.9.2 strict per-generator reactive scheduling.

### Changed

- **Generator table contract alignment**: generator batch construction now emits `q_sched_mvar` in canonical order between `p_sched_mw` and `p_min_mw`.
- **Contract tests/golden checks** updated to assert `q_sched_mvar` presence and type.

### Documentation

- `docs/psse-mapping.md` now maps `QG -> generators.q_sched_mvar` and documents its continued contribution to `buses.q_sched`.

---

## [0.3.4] - 2026-04-24

### Added

- **RPF v0.9.1 load ZIP fidelity**: `loads` export now populates nullable `p_i_pu`, `q_i_pu`, `p_y_pu`, and `q_y_pu` from PSS/E `IP/IQ/YP/YQ` (all `/ SBASE`) while preserving sign and keeping existing `p_pu` / `q_pu` behavior unchanged.
- **Root metadata key**: exporter now writes `rpf.loads.zip_fidelity_presence` with `not_available | partial | complete` based on per-row ZIP source-term availability.

### Fixed

- **`buses.q_min` / `q_max` ordering**: when aggregated PSS/E limits end up with `q_min` > `q_max`, swap again so the bus row matches interchange / solver expectations; per-machine `QB`/`QT` on `generators` (and `generators.params`) stay faithful to the deck.

---

## [0.3.3] - 2026-04-24

Patch release: **RAW/DYR parsing and export fidelity** (RPF v0.9.0 wire shape unchanged).

### Fixed

- **PSS/E bus `IDE` parsing**: map PSS/E **2** → PV and **3** → PQ generator (interchange `type` 3 / 2), matching `parse_psse_raw_ex` / phased RAW audits; previously 2 and 3 were swapped.
- **PSS/E v35+ bus records**: optional extra field after `BASKV` (e.g. substation name) no longer shifts `IDE` / `AREA` / `VM` / `VA` one column left — PV buses were mis-read as PQ loads, breaking Texas2k-style v35 decks.

### Changed

- **RAW fidelity on export**: removed `v_mag_set` clamping to NVLO/NVHI and forced-positive “sanitization”; dropped export-time rejection of nonpositive `v_mag_set` on connected buses. `VS` → `v_mag_set` now uses every **non-zero finite** in-service machine value (last in file order wins). Bus **NVHI/NVLO/EVHI/EVLO** are stored as parsed without substituting 1.1 / 0.9 when outside a heuristic band. Crate rustdoc documents the fidelity policy.

### Added

- **`generators.params` PSS/E pass-through**: every generator row now includes a non-null `params` map with RAW machine numerics (`vs`, `ireg` when non-zero, `zr`, `zx`, `rt`, `xt`, `gtap`, `rmpct`, `qg`, `wmod`, `wpf`) plus existing DYR keys (`H`, `xd_prime`, `D`) when finite — closes the gap where VS/IREG/ZIP machine data had no RPF home beyond bus aggregates.

### Documentation

- **`docs/psse-mapping.md`**: new **PSS/E RAW coverage** section (exported vs skipped vs schema-limited); generator table rows aligned with actual RPF column names and `params` behavior.

---

## [0.3.2] - 2026-04-23

### Release & CI

- Bumped crate version to **0.3.2** (0.3.1 was already published; this release carries the RPF v0.9.0 work plus automation).
- Added **CI** workflow: `cargo fmt --check`, `cargo clippy`, `cargo test` on every push/PR to `main`.
- Added **Public Safety** workflow (blocked paths, secrets scan) aligned with `raptrix-cim-rs`.
- Added **Markdown lint** and **version consistency** checks (`CHANGELOG` heading must match `Cargo.toml`).
- **Release** workflow now runs **`cargo test --workspace`** before cross-compiling artifacts.
- **`scripts/verify-external-golden.sh`**: release-mode CLI pass over the full `tests/data/external` corpus (aligned with `golden_test.rs`); strict by default, `RELAX_MISSING=1` for partial trees.
- **`scripts/test-wsl.ps1`** and **`scripts/verify.ps1`** (optional `-ExternalGolden`): same WSL workflow as `raptrix-cim-rs` for Windows / OneDrive file-access issues.
- **`scripts/generate_all_rpfs.sh`**: repo-relative paths only; includes ACTIVSg10k (static + dynamic) and Texas2k GFM dynamic; removed hardcoded `/mnt/c/...` paths.
- Optional **`external-golden.yml`** workflow (manual dispatch) runs the verify script with `RELAX_MISSING=1` on hosted runners without licensed inputs.
- **README**: performance snapshot table, expanded testing / WSL / verification docs, and a short “solver completeness” gap list.

### Fixed

- **Markdownlint**: MD032 (blank lines around lists) in `.githooks/README.md`, `CHANGELOG.md`, and `MIGRATION.md`; MD004 (dash list style) in `MIGRATION.md` “Performance tips”.
- **CI `golden_test`**: IEEE 14/118 and ACTIVSg25k/70k cases now **skip** when the corresponding `tests/data/external` RAW is missing (same pattern as ERCOT/NYISO/EI), so default GitHub runners pass without licensed fixtures.
- **Documentation scope**: README / CHANGELOG / MIGRATION / `docs/psse-mapping.md` and public rustdoc use neutral, release-focused wording for optional v0.9.0 metadata and `scenario_context`; MIGRATION appendix trimmed to schema deltas plus pointers to mapping + golden docs (interchange column names and behavior unchanged).

### Schema (unchanged from 0.3.1 line)

- Output remains RPF **v0.9.0** via `raptrix-cim-arrow` from `main` (see 0.3.1 changelog for field/table details).

---

## [0.3.1] - 2026-04-19

### Schema Alignment

- Completed full canonical RPF **v0.9.0** support (via `raptrix-cim-arrow` on `main`): **18** required root tables; removed `ibr_devices`; IBRs unified on `generators`.
- Extended **`metadata`** with five additional nullable columns in v0.9.0 (typically **null** for PSS/E-only exports; see schema-contract).
- Extended stub **`contingencies`** batch with six additional nullable columns for the same contract (null for minimal planning exports from this converter).
- Added **`case_mode`** override path (`ExportOptions` / CLI `--case-mode`), including enum values defined in the interchange contract (e.g. `hour_ahead_advisory`).
- **`scenario_context`**: `ExportOptions::scenario_context_rows` is reserved; non-empty input errors when optional-root IPC emission is unavailable in the linked `raptrix-cim-arrow` build.
- Added explicit `owner_id` linkage on required exported tables.
- Migrated generator export to unified hierarchical generator shape.

### Tests

- Fixed `parser_robustness_test` RAW snippets to use valid minimal section flow for Two-Terminal DC and Multi-Section Line parser paths.
- Preserved robustness coverage for key field extraction and malformed/same-endpoint/negative-bus rejection behavior.

---

## [0.3.0] - 2026-04-19

### Major Features

- **Stricter Parser Robustness**: Hardened parsing for DC lines and multi-section lines with malformed record detection and informative logging.
- **Richer IBR classification**: Device taxonomy distinguishes `solar_pv`, `wind_type3`, `wind_type4`, `bess`, and `generic_ibr` with broader DYR model-family matching.
- **Parser regression coverage**: Synthetic RAW snippets for DC lines, multi-section lines, and malformed-row handling.

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

- Switched shunt export now splits steps by bank for clearer bank-level representation.
- Parser logs all section-level statistics and rejection counts.
- Deterministic zero-row table emission ensures downstream reproducibility.

### Fixed

- Arrow schema nullability alignment for `metadata.scenario_tags` and `multi_section_lines.section_branch_ids`.
- Cargo.lock revision verification to reflect v0.8.8 contract.

### Schema Migration

See [MIGRATION.md](MIGRATION.md) (RPF v0.8.8 sync section) for full details.

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
git commit -m "chore: bump to v0.3.4"

# 3. Create an annotated tag
git tag -a v0.3.4 -m "Release v0.3.4: RPF v0.9.1 ZIP fidelity + metadata alignment"

# 4. Push commits and tags
git push origin main
git push origin v0.3.4
```

The GitHub Actions `release` workflow will automatically:

- Trigger on tag push (`v*.*.*` pattern).
- Run `cargo test --workspace`, then build release binaries for Windows (x86_64), Linux (x86_64), and macOS (arm64).
- Create a GitHub Release with auto-generated release notes.
- Attach platform-specific executables and source archives.

---

[0.3.4]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.4
[0.3.3]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.3
[0.3.2]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.2
[0.3.1]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.1
[0.3.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.0
[0.2.2]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.2
[0.2.1]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.1
[0.2.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.0
[0.1.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.1.0
