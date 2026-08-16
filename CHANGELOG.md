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

## [0.7.0] - 2026-08-16

### RPF v0.14.0 (raptrix-cim-arrow 0.7.0) — additive MINOR

- Bump to `raptrix-cim-arrow` **0.7.0** / RPF **v0.14.0**. No RAW/EPC semantic change.
- `contingencies` uses the shared 10-column schema; `tpl_category` and `reserved` stay null (zero-row stub).
- `contingency_sequences` is omitted (`include_contingency_sequences = false`).
- Readers accept v0.14.0, v0.13.1, and v0.13.0. Pre-0.13 still requires re-export.
- Dependency: git tag `v0.7.0`.

### Fixed

- **Embedded apostrophes in PSS/E quoted fields**: `tokenize` no longer closes a quoted field on every `'`. A quote closes only when the next significant character is `,` or end-of-string; `''` remains an escaped apostrophe. Bus records such as `'O'Neil Bus 1'` now keep published VM/VA/BASKV instead of collapsing to a flat seed. Regenerate affected local goldens after upgrading.

### Golden corpus — dynamic is canonical

- When a `.dyr` / `.dyn` companion exists, `tests/golden/<stem>.rpf` is the **dynamic** conversion (DYR attached). `_dynamic.rpf` is an alias; `_static.rpf` is the no-DYR companion for explicit A/B.
- `golden_test` asserts `dynamics_models` is non-empty whenever a DYR companion was paired.
- `verify-external-golden.sh` / `generate_all_rpfs.sh` follow the same policy and pick `*_dynamics.dyr` prefix companions (ACTIVSg).

---

## [0.6.0] - 2026-07-30

### RPF **v0.13.0** (`raptrix-cim-arrow` **0.6.0**)

- **Emit RPF v0.13.0 only** (clean cut; re-export required for all pre-0.13 `.rpf` files).
- Provenance: `source_format=psse_raw`, RAW rev → `source_format_version`, `source_identity_scheme=dense_bus_id`.
- Bus types as dictionary tokens `PQ`/`PV`/`Slack`; `controlled_bus_id` null for local IREG; native UTC timestamps.
- Optional `mrid` on loads/shunts (null from PSS/E); `classical_params` on dynamics when DYR params provide H/D/xd'.
- Root metadata stamps `rpf.identity.model=hybrid_solver_flat_v1`.
- **Dependency**: `raptrix-cim-arrow` **0.6.0** / git tag **`v0.6.0`**.

---

## [0.5.7] - 2026-07-16

### RPF **v0.12.5** (`raptrix-cim-arrow` **0.5.7**)

- **Emit RPF v0.12.5**: every `.rpf` carries `raptrix.version` / contract **v0.12.5**.
- **Nullable trailing `buses.latitude` / `buses.longitude`**: emitted as null (PSS/E RAW has no standard WGS84 bus coordinates). Purely additive — electrical payload unchanged from 0.5.6.
- **No re-export required** for existing **v0.12.1+** `.rpf` files; the pinned reader accepts v0.12.1 through v0.12.5.
- **Dependency**: `raptrix-cim-arrow` **0.5.7** / git tag **`v0.5.7`**.

---

## [0.5.6] - 2026-07-02

### RPF **v0.12.4** (`raptrix-cim-arrow` **0.5.6**)

- **Emit RPF v0.12.4**: every `.rpf` carries `raptrix.version` / contract **v0.12.4**. New nullable metadata columns (baseline provenance) and optional solved-state tables introduced by v0.12.3/v0.12.4 remain null / zero-row on the planning export path — the emitted planning payload is unchanged from 0.5.4 apart from the stamped version.
- **No re-export required** for existing **v0.12.1+** `.rpf` files; the pinned reader accepts v0.12.1 through v0.12.4.
- **Dependency**: `raptrix-cim-arrow` **0.5.6** / git tag **`v0.5.6`**.

---

## [0.5.4] - 2026-06-15

### RPF **v0.12.2** (`raptrix-cim-arrow` **0.5.4**)

- **Emit RPF v0.12.2**: every `.rpf` carries `raptrix.version` / contract **v0.12.2** and root metadata `rpf.mrid_support=v1`.
- **Additive `mrid` columns** on `branches`, `generators`, `transformers_2w`, and `transformers_3w`: stable CIM-compatible equipment identifiers synthesized on PSS/E export (`BR_*`, `GEN_*`, `XF2_*`, `XF3_*`; star-expanded 3W legs use `{parent}_H` / `_M` / `_L`).
- **Downstream guidance**: Downstream integrators should prefer `mrid` for equipment_id mapping.
- **No re-export required** for existing **v0.12.1** `.rpf` files — readers pad missing trailing `mrid` as null.
- **Dependency**: `raptrix-cim-arrow` **0.5.4** / git **`c45256e`**.

---

## [0.5.3] - 2026-06-10

### RPF **v0.12.1** (`raptrix-cim-arrow` **0.5.3**)

- **Emit-only v0.12.1**: every `.rpf` from this crate carries `raptrix.version` / contract **v0.12.1** (via `raptrix-cim-arrow::SCHEMA_VERSION`). Optional `remedial_action_schemes` / `contingency_island_analysis` root tables are not emitted on the standard PSS/E path.
- **`SUPPORTED_RPF_VERSIONS`** in the linked crate accepts **only** **v0.12.1** / **0.12.1** — re-export all cached `.rpf` files.
- **Dependency**: `raptrix-cim-arrow` **0.5.3** / git **`298f9958cb9a551e273257f045bcadc1c72cf7bb`**.

---

## [0.5.0] - 2026-05-30

### RPF **v0.11.0** (`raptrix-cim-arrow` **0.5.0**)

- **Emit-only v0.11.0**: every `.rpf` from this crate carries `raptrix.version` / contract **v0.11.0** (via `raptrix-cim-arrow::SCHEMA_VERSION`). v0.11.0 is **purely additive** — optional `protection_contingencies` / `topology_changes` root tables are not emitted on the standard PSS/E path.
- **Dependency**: `raptrix-cim-arrow` **0.5.0** / git **`e172439b96c16c69bdfb4c106bddba23d99e6e60`**.

### Generator WMOD / WPF parser fix

- **`parser.rs`**: `WMOD` and `WPF` are now read after the full owner block (`O1,F1,O2,F2,O3,F3,O4,F4`), not from `F1` / `O2`. Fixes false wind-IBR tagging when owner fraction `F1` is an integer like `1` (C++ `stoi` truncated `"1.0000"` to `1`; Rust `field_u8` silently returned `0`, masking the bug on Texas2k).
- **v35**: `gen_o1_idx` shifted to **20** (after `BASLOD` @ 19); `WMOD` @ 28, `WPF` @ 29.
- **Tests**: v33 full-owner-block and v35 BASLOD unit tests in `parser.rs`; `generator_wmod_fallback_ibr_without_dyr` integration test in `rpf_contract_smoke_test.rs`.

---

## [0.4.0] - 2026-05-10

### RPF **v0.10.0** (`raptrix-cim-arrow` **0.4.0**)

- **Emit-only v0.10.0**: every `.rpf` from this crate carries `raptrix.version` / contract **v0.10.0** (via `raptrix-cim-arrow::SCHEMA_VERSION`). Older interchange files must be re-emitted with **raptrix-psse-rs ≥ 0.4.0** or **raptrix-cim-rs ≥ 0.4.0**.
- **`metadata.computational_load_mode`**: nullable Boolean column present; PSS/E exports write **null** (this converter does not author `computational_load_profiles`).
- **`dynamics_models.perc1_params`**: nullable struct column present; all **null** until PERC1 field mapping exists.
- **Post-write validation**: unchanged — `write_root_rpf_with_metadata` still runs `raptrix_cim_arrow::validate_rpf_file` before returning.
- **Dependency**: `raptrix-cim-arrow` **0.4.0** / git **`b08d841a6c731e2df6d56cdff6d06dba8ced4e26`**; remove the sibling `[patch]` entry in `Cargo.toml` when publishing from crates.io only.

### BRANCH deck diagnostics (RAW vs RPF row-count investigations)

- **`parser::parse_raw_with_branch_deck_stats`** returns `(Network, BranchDeckStats)` with BRANCH-section line counts, a histogram of **raw** `ST` integer tokens at the version-aware column (before non-zero values are folded to in-service in the model), and a count of lines where `parse_branch_record` returned `None`.
- **`branch_deck_scan` binary** (`cargo run --bin branch_deck_scan -- --raw … [--rpf …]`) prints those statistics, optional duplicate in-service `(from,to,ckt)` keys, and an in-service multiset diff between the parsed RAW network and an RPF `branches` table (for reconciling cases like large interconnect models where other tools omit `ST=0` rows).
- **Unit test** `parser::tests::branch_deck_stats_histogram_matches_branch_lines_v33` locks histogram behaviour on a minimal v33 deck.

### PSS/E `IDE=3` / `IDE=4` mapping correctness fix

Highest-impact correctness fix in this release cycle: the parser had been
mapping PSS/E IDE codes 3 and 4 with their meanings swapped relative to the
official PSS®E Program Operation Manual. This silently lost the swing-bus
designation on every RAW that authored an explicit `IDE=3` and routed
disconnected/isolated `IDE=4` buses into the slack-candidate pool.

#### Fixed

- **`parser.rs::psse_bus_ide_raw_to_type`** now follows the official PSS/E
  spec: `IDE=1 → LoadBus`, `IDE=2 → GeneratorPV`, **`IDE=3 → Slack`**, **`IDE=4 → LoadBus`** (folded into PQ to mirror raptrix-core's `psse_parser.cpp` convention `int type = (rb.type == 4) ? 1 : rb.type;`). Previously `IDE=3` was incorrectly mapped to `BusType::GeneratorPQ` (then collapsed to canonical PQ on export) and `IDE=4` was incorrectly mapped to `BusType::Slack`.
- **`models.rs::BusType`** doc-comments rewritten to reflect the correct PSS/E IDE semantics. The `BusType::GeneratorPQ` enum variant is retained for backward compatibility but is no longer assigned by the parser; canonicalization continues to map it to RPF type=1 (PQ).
- **Diagnostic message text** updated everywhere `IDE=4` was used as shorthand for the slack bus to instead say `IDE=3` (`lib.rs::enforce_deterministic_slack`, `validation.rs::MMWG-7.3.1/no-slack`, `scripts/local_iterate/parse_log.py` warning patterns).

#### Companion change in `raptrix-core` (`src/model/rpf_reader.cpp`)

- The IBR-aware automatic PV→PQ demoter (which fired when `model.has_ibr=true` and re-typed zero-Q-span PV buses to PQ) is replaced with a **diagnostic-only audit**. The solver already correctly handles zero-Q-span PV buses via the `std::abs(b.q_max) < 1e-9` skip in `solver.cpp` (lines around 1232/1285/2709/2738/4651), so the demoter was preempting work the solver does correctly and silently diverging from RAW import semantics. The structural-validity guard (which only demotes PV buses with no online machines) is unchanged and continues to fire as before.

#### Tests

- `tests/bus_ide_parsing_test.rs::v33_maps_psse_ide_2_to_pv_3_to_slack_and_4_to_load` (renamed): asserts the corrected mapping for IDE 1/2/3/4.
- `tests/bus_ide_parsing_test.rs::v35_optional_field_after_baskv_keeps_ide_and_vm_aligned`: extended with an `IDE=4` row to cover the disconnected→PQ mapping under the v35 substation-name layout.
- `tests/rpf_contract_smoke_test.rs::buses_type_exports_canonical_codes_for_pv_and_slack`: rewritten to author the slack bus with `IDE=3` (was `IDE=4`).
- `tests/rpf_contract_smoke_test.rs::writer_promotes_connected_replacement_for_disconnected_slack`: rewritten to use `IDE=3` for the orphan slack (was `IDE=4`); still asserts that the connected replacement is promoted.
- `tests/rpf_contract_smoke_test.rs::writer_preserves_psse_ide3_swing_and_demotes_ide4_isolated` (new): explicit regression for the bug — verifies that `IDE=3` round-trips to canonical slack=3 _without_ re-picking, that `IDE=4` exports as canonical PQ=1 and is never a slack candidate, and that the file carries exactly one canonical slack.

#### Iterate-loop verification

Local iterate-loop sweep across 13 cases (8 small/medium + 4 Texas7k + Midwest24k + ACTIVSg25k) confirms the fix produces RAW↔RPF parity:

- **24 RAW runs / 24 RPF runs, 22 converged on each side, 0 convergence asymmetries** (`scripts/local_iterate/out/delta_report.after-ide-fix.md`).
- Every paired case shows `Δ iters = Δ qswitches = Δ qviolations = 0` between RAW and RPF; tolerances match within one order of magnitude (most are bit-equal).
- 0 `[converter] auto-assigned bus * as slack` warnings in `convert_stderr.log` (down from 9 in the pre-fix sweep) — every case now finds the RAW's authored `IDE=3` swing bus directly.
- 0 `RPF import: auto-demoted N zero-Q-span PV buses to PQ` warnings — replaced by 1080 instances of the new `RPF import: detected N zero-Q-span PV buses … kept as type-2` diagnostic from the companion `raptrix-core` change.
- ACTIVSg25k remains non-converged in both RAW and RPF (`tol = 2.68`); a separate solver-limited case unrelated to writer fidelity.
- Spot check on `Base_Eastern_Interconnect_515GW.RAW`: previously the converter auto-picked bus 27840 as slack while the RAW authored bus 50320; with the fix bus 50320 is preserved as canonical slack=3, the 6 IDE=4 buses are correctly emitted as PQ, and PV-mode now converges on the RPF (was failing pre-fix).

### RPF generation quality hardening (raptrix-core parity)

This work targets RAW vs RPF convergence parity in raptrix-core and eliminates
classes of importer warnings that indicate writer-side gaps rather than data
issues.

#### Added

- **Slack-island awareness in `enforce_deterministic_slack`**: connected-degree check now demotes any RAW-authored **swing** bus (`IDE=3`) that sits on a disconnected island (degree==0) and promotes the largest-generation connected bus instead. Eliminates the importer's "auto-assigned slack" warning class for files where the RAW had a slack but on a dead island. If no connected bus exists at all (degenerate / topology-only fixtures) the existing swing token is preserved so the file still carries an explicit slack.
- **Voltage-set sanitization on bus export**: `v_mag_set` and `v_ang_set` are now clamped at writer time (`v_mag_set <= 0 || !finite -> 1.0`, `v_ang_set !finite -> 0.0`). A per-file diagnostic counter is logged. Eliminates the importer's "sanitized invalid v_mag" warning class — measured: 8 import-time sanitization warnings (31 invalid VM total across 4 cases) drop to 0 in the local iterate-loop sweep.
- **Generator Q-limit ledger consistency**: `q_min`/`q_max` are swapped per generator on export when `q_min > q_max`, and non-finite Q-limits are clamped to 0.0. The same swap is propagated into bus-level Q aggregation. Fix improved Midwest24k_20220923 PQ tolerance by 7 orders of magnitude (2.9e-4 → 2.0e-11) in the local iterate-loop sweep.

#### Changed

- Pinned `raptrix-cim-arrow` to **0.4.0** / RPF **v0.10.0** (narrow reader: `SUPPORTED_RPF_VERSIONS` is **only** `v0.10.0` / `0.10.0` in that release). Use **raptrix-core** with `rpf_reader` v0.10.0 support when ingesting new files.

#### Decided not to ship (kept as infrastructure only)

- **Warm-start `buses_solved` seed emission on the PSS/E RAW path**: the cim-rs `seed_only` vocabulary and the writer-side `build_buses_solved_seed_batch` helper landed, but emission is disabled in `write_psse_to_rpf_with_options`. Rationale: the buses table already carries `v_ang_set = bus.va.to_radians()` and `v_mag_set = (sanitized bus.vm, with gen.vs override on PV/Slack)`, which is exactly the warm-start initial condition. The importer's seed loop unconditionally overwrites `bus.v_mag_set` with the seed `v_mag_pu`, and on PV/Slack buses that replaces the scheduled `gen.vs` target with the operating `bus.vm`. In the local iterate-loop sweep, switching the emission on regressed convergence on 4 RPFs by 1e+1 to 1e+4 in tolerance (Texas7k_20210804 / Texas7k_20220923 / Texas7k_2030_20220923 / NYISO_onpeak2019_v23). Helper retained for callers that genuinely have a separately-computed warm-start payload distinct from the planning setpoints.

#### Tests

- New `rpf_contract_smoke_test` cases covering: writer-side voltage sanitization, disconnected-slack promotion, and the negative seed-emission contract (PSS/E RAW path keeps `solved_state_presence = "not_computed"` and does NOT emit a `buses_solved` table even on warm-start RAWs).
- Local iterate-loop infrastructure under `scripts/local_iterate/` (gitignored): `run.ps1` driver, `robust_harness.py` raptrix-core wrapper that catches per-case parse/solve errors, and `parse_log.py` delta-report formatter. Produces a paired RAW vs RPF convergence table plus importer/converter warning tallies. See `scripts/local_iterate/README.md`.

---

## [0.3.10] - 2026-05-08

### Fixed

- Canonical bus-type export alignment: `buses.type` now writes schema-contract values (`1=PQ`, `2=PV`, `3=slack`) instead of reusing internal parser enum discriminants.
- Export-time slack normalization now auto-assigns one deterministic slack bus when RAW parsing yields none (largest connected online generation, then degree, then bus id).
- CI lint gate compliance: addressed strict `rustfmt` / `clippy -D warnings` regressions in test modules so release workflows pass cleanly.

### Added

- Golden folder guardrail checks for canonical static naming and supported RPF version metadata.

### Changed

- Golden generation scripts now follow a strict one-to-one canonical `*_static.rpf` policy (no alias duplicates in `tests/golden`).

### Tests

- `rpf_contract_smoke_test` now asserts canonical `buses.type` values on exported rows.

## [0.3.9] - 2026-05-04

### Added (RPF v0.9.5)

- **`generators.controlled_bus_id`**: required Int32 trailing column — PSS/E **IREG** mapped to dense `bus_id` space (`0` when IREG is unset or equals the machine bus; otherwise the remote regulated bus).
- **`metadata.default_shunt_control_mode`** (nullable Dictionary) and file-level **`rpf.default_shunt_control_mode`**: planning exports (`flat_start_planning`, `warm_start_planning`, `hour_ahead_advisory`) default to `planning_full`, matching `raptrix-cim-rs` planning writers; `solved_snapshot` omits unless overridden.
- **`ExportOptions::default_shunt_control_mode_override`** and CLI **`--default-shunt-control-mode`** for explicit control of the v0.9.5 shunt-mode stamp.
- Golden integration test **`golden_texas7k_updated_saint_static`** and matching **`verify-external-golden.sh`** entry for `tests/data/external/Texas7k_20210804_updated_SAInt.RAW`.

### Changed

- Pinned **`raptrix-cim-arrow`** to git rev **`a556662edccff03739566e95820315843dbaf537`** (RPF / schema **v0.9.5**; `SUPPORTED_RPF_VERSIONS` includes `v0.9.5` / `0.9.5` and retains v0.9.4 / v0.9.3 read aliases).

### Tests

- `generators_hierarchy_ownership_and_metadata_smoke`: remote **IREG** fixture and assertions on `controlled_bus_id` plus root `rpf.default_shunt_control_mode`.

### Documentation

- `docs/psse-mapping.md`, `README.md`, and `MIGRATION.md` updated for v0.9.5.

---

## [0.3.8] - 2026-05-03

### Added (Breaking — RPF v0.9.4)

- **`buses.qd_load_pu`** (v0.9.4): new required column — Σ(in-service load QL) / SBASE per bus; signed (positive for inductive, negative when QL < 0 for capacitive reactive injection through load records).
- **`buses.qg_sched_pu`** (v0.9.4): new required column — Σ(in-service generator QG) / SBASE per bus (any sign).
- The machine-checkable identity `q_sched ≈ qg_sched_pu − qd_load_pu` now holds for every bus row; verified in golden tests.
- Updated `raptrix-cim-arrow` pin to `v0.3.4` (SHA `ea6b02a6`) which carries the schema change and extends `SUPPORTED_RPF_VERSIONS` to include `v0.9.4` / `0.9.4` alongside the existing `v0.9.3` / `0.9.3` backward-compat aliases.

### Changed

- `BusAggregate` struct in `src/lib.rs` gains two new accumulator fields (`qd_load_pu`, `qg_sched_pu`).
- `build_bus_aggregates`: loads loop now increments `agg.qd_load_pu += load.ql / base_mva`; generators loop increments `agg.qg_sched_pu += generator.qg / base_mva`.
- `build_buses_batch`: two new `Float64Builder` columns appended after `bus_uuid` to match `buses_schema()` column order.

### Tests

- `assert_v094_bus_q_decomposition` helper added; called in `golden_ieee14_static` and `golden_texas7k_static` to verify non-null columns and the `q_sched` identity across all bus rows.
- Added `golden_activsg10k_static` so the Rust golden suite matches `scripts/verify-external-golden.sh` static conversion for ACTIVSg10k (feeds `test_v093_raw_vs_rpf_parity` stem pairing in raptrix-core).

### Documentation

- `docs/psse-mapping.md`: bus aggregation table and aggregated-only column reference updated with `qd_load_pu` / `qg_sched_pu` entries.
- `MIGRATION.md`: new `v0.3.8` section with handoff notes for raptrix-core.
- Expanded `tests/golden/README.md` with `.dyn` vs `.dyr` precedence and a table of expected `tests/data/external/` filenames aligned with `golden_test.rs`.

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

To create and publish **v0.4.0** (or any **`vX.Y.Z`** aligned with `[package].version` in `Cargo.toml`):

```bash
# 1. Bump [package].version in Cargo.toml and add/adjust a "## [X.Y.Z] - YYYY-MM-DD"
#    section in this CHANGELOG (Keep a Changelog style).

# 2. Local gates (mirrors CI + release matrix)
./scripts/sync-versions.ps1 -Check   # PowerShell: Cargo.toml version ↔ CHANGELOG heading
./scripts/public-safety-check.sh --mode tracked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
# Or the combined helper (runs fmt, tracked public-safety when bash is available, tests):
./scripts/pre-release-check.ps1

# 3. Commit
git add Cargo.toml CHANGELOG.md README.md MIGRATION.md docs/ scripts/ src/ tests/
git commit -m "chore: release v0.4.0 (RPF v0.10.0 / raptrix-cim-arrow 0.4.0)"

# 4. Push to main (optional: open PR and merge first)
git push origin main

# 5a. Manual tag (full control over tag message)
git tag -a v0.4.0 -m "Release v0.4.0: RPF v0.10.0 interchange (raptrix-cim-arrow 0.4.0)"
git push origin v0.4.0

# 5b. Or rely on Auto Tag Release: pushing Cargo.toml + CHANGELOG.md to main
#     creates v${version} if the tag does not already exist (.github/workflows/auto-tag-release.yml).
```

The GitHub Actions **`release`** workflow (`.github/workflows/release.yml`) will:

- Trigger on tag push matching **`v*.*.*`** (and via **Auto Tag Release** / `workflow_dispatch`).
- **Fail fast on tag push** if **`vX.Y.Z`** does not equal **`[package].version`** in `Cargo.toml` (so `raptrix-psse-rs-v0.4.0-*` archives always match the crate you tagged).
- Resolve the semver from the tag (`v0.4.0` → **`0.4.0`**) or, for non-tag runs, from **`grep '^version = ' Cargo.toml`** — packaging uses that value for **`dist/raptrix-psse-rs-v…`** filenames.
- Run **`./scripts/pre-release-check.ps1`** on each release-matrix runner (fmt + tracked public-safety when `bash` is on `PATH` + `cargo test --workspace`).
- Build **`cargo build --release --target <triple>`** and package with **`scripts/package-windows.ps1`** / **`scripts/package-unix.sh`** using the resolved version.
- Attach **`dist/raptrix-psse-rs-v0.4.0-*`** archives to the GitHub Release when publishing from a version tag.

**Publishing-only note:** remove the sibling **`[patch."https://github.com/RaptrixPowerFlow/raptrix-cim-rs"]`** block in `Cargo.toml` if the release must build from **crates.io** `raptrix-cim-arrow` alone; keep the patch for local / CI checkouts that depend on an unpublished cim-arrow git rev.

---

[0.5.3]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.5.3
[0.5.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.5.0
[0.4.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.4.0
[0.3.4]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.4
[0.3.3]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.3
[0.3.2]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.2
[0.3.1]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.1
[0.3.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.3.0
[0.2.2]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.2
[0.2.1]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.1
[0.2.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.2.0
[0.1.0]: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases/tag/v0.1.0
