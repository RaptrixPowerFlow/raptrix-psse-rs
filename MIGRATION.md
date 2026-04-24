<!--
    raptrix-psse-rs
    Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# Porting Status, Release Notes, and Schema Version Migrations

**raptrix-psse-rs**  
Copyright (c) 2026 Raptrix PowerFlow

**Scope:** This file records **released** crate and **published** RPF interchange deltas so integrators can upgrade safely. It is **not** a delivery forecast: optional columns, dependency capabilities, and parser coverage evolve on their own timelines. For field-level behavior of the converter as shipped, see [`docs/psse-mapping.md`](docs/psse-mapping.md).

---

## RPF Schema Version Migrations

### v0.3.4: RPF v0.9.0 -> **v0.9.1** (Non-breaking)

`raptrix-psse-rs` now aligns with the additive **RPF v0.9.1** load-model extension from the linked `raptrix-cim-arrow` contract.

**What changed:**

- `loads` now exports ZIP terms to new nullable columns:
  - `p_i_pu = IP / SBASE`
  - `q_i_pu = IQ / SBASE`
  - `p_y_pu = YP / SBASE`
  - `q_y_pu = YQ / SBASE`
- Existing `p_pu` / `q_pu` semantics are unchanged (`PL/SBASE`, `QL/SBASE`).
- Root file metadata now includes `rpf.loads.zip_fidelity_presence`:
  - `not_available` | `partial` | `complete`

**Compatibility:**

- Additive only: no required root table order changes and no field removals/renames.
- Readers that ignore unknown columns continue to work; v0.9.1-aware readers can consume ZIP fidelity directly.

### v0.3.1: RPF v0.8.9 generator layout → **v0.9.0** contract (Breaking)

Crate **0.3.4** tracks the latest **`raptrix-cim-arrow`** contract (currently **RPF v0.9.1**). Releases **0.3.1** through **0.3.4** cover the hierarchical generator model and the v0.9.x wire shape (no `ibr_devices`; extended metadata / contingencies). **0.3.4** adds ZIP load-fidelity export columns and metadata without breaking root/table compatibility.

**Generator & ownership (from v0.8.9-era alignment):**

- `generators` unified hierarchical shape: `generator_id`, `unit_type`, `hierarchy_level`, `parent_generator_id`, `aggregation_count`, MW/MVAR-native dispatch fields, `is_ibr`, `ibr_subtype`, `owner_id`, `params`.
- Legacy flat RAW units export as `hierarchy_level = "unit"`, `parent_generator_id = null`, `aggregation_count = null`.
- `generators.owner_id`, `buses.owner_id`, `branches.owner_id`; `owners` shape with `short_name`, `type`, `params`.

**v0.9.x wire contract:**

- Canonical RPF is **v0.9.1** (see `raptrix-cim-rs` `docs/schema-contract.md`).
- **`ibr_devices` removed** — **18** required root tables; IBRs only on **`generators`**.
- **`metadata`**: five additional nullable columns in v0.9.0 (default **null** for PSS/E-only exports): `hour_ahead_uncertainty_band`, `commitment_source`, `solver_q_limit_infeasible_count`, `pv_to_pq_switch_count`, `real_time_discovery`.
- **`contingencies`**: six additional nullable columns in v0.9.0 (null for minimal exports from this converter).
- **`case_mode`**: contract-defined values including `hour_ahead_advisory` (CLI `--case-mode` / `ExportOptions::case_mode_override`; else inferred flat/warm from RAW where applicable).
- **Optional `scenario_context`**: not emitted by default; non-empty `ExportOptions::scenario_context_rows` **errors** when optional-root IPC support is unavailable in the linked `raptrix-cim-arrow` build.

**Behavior:** IBR classification remains DYR-first with RAW WMOD fallback; canonical `ibr_subtype` values remain `solar`, `wind`, `battery`, `generic_ibr`.

**Compatibility:** Hard break vs v0.8.8 generator columns; regenerate `.rpf` artifacts; tooling must be v0.9.0-aware (no `ibr_devices` root column).

### v0.2.3 -> v0.2.4: Canonical RPF v0.8.8 Sync

Summary of changes:

- Canonical RPF contract is now v0.8.8.
- Four new required tables are now emitted on every export:
    - `multi_section_lines`
    - `dc_lines_2w`
    - `switched_shunt_banks`
    - `ibr_devices`
- `branches` now includes nullable linkage fields:
    - `parent_line_id`
    - `section_index`
- `metadata` now includes modern-grid fields:
    - `modern_grid_profile`
    - `ibr_penetration_pct`
    - `has_ibr`
    - `has_smart_valve`
    - `has_multi_terminal_dc`
    - `study_purpose`
    - `scenario_tags`

Behavior notes:

- IBR extraction is DYR-first with RAW WMOD fallback.
- IBR classes now distinguish `solar_pv`, `wind_type3`, `wind_type4`, `bess`, and `generic_ibr`.
- DC/multi-section parsing uses robust token scanning and reports malformed/unsupported rows in parser logs.
- `switched_shunts.b_steps` now exports capacitive steps; full per-bank steps are exported in `switched_shunt_banks`.
- CLI supports metadata overrides for modern studies:
    - `--study-purpose <TEXT>`
    - `--scenario-tag <TAG>` (repeatable)

Compatibility notes:

- v0.8.8 readers require the new tables/columns; converters on older contracts must be upgraded or outputs regenerated.
- This repository targets forward-only v0.8.8 output and does not preserve backward writer compatibility.

### v0.2.2 → v0.2.3: Canonical RPF v0.8.7 Sync

**Summary of changes:**

- Canonical RPF contract is now v0.8.7.
- `rpf.transformer_representation_mode` is now sourced from shared `raptrix-cim-arrow` metadata constants instead of a repo-local copy.
- Default transformer export mode is now `native_3w` to match the canonical interchange default.
- Expanded-mode synthetic star buses now use IDs greater than 10 000 000 and are removed from the exported `buses` table.

**Compatibility notes:**

- Files written before v0.8.7 may omit `rpf.transformer_representation_mode`; canonical readers should treat the missing key as `native_3w`.
- Dual materialization remains a hard export error: no file may contain active native `transformers_3w` rows and active synthetic star-leg `transformers_2w` rows for the same physical unit.
- Regenerate checked-in `.rpf` artifacts so downstream core verification runs against the v0.8.7 contract.

### v0.2.1 → v0.2.2: Transformer Representation Invariants

**Summary of changes:**

- Exporter now enforces a **single transformer representation mode** per run.
- Default mode is `expanded`: 3-winding devices export only as star-expanded rows in `transformers_2w`.
- Optional mode `native-3w`: 3-winding devices export only in `transformers_3w` and synthetic star legs are removed.
- Export fails fast on ambiguous overlap that cannot be safely normalized.
- Root metadata includes stable machine-readable key `rpf.transformer_representation_mode` with values `expanded` or `native_3w`.

**Compatibility notes:**

- Schema remains backward-compatible; no canonical table columns were removed.
- New behavior is correctness-driven: no exported case may contain active duplicate materialization candidates for the same physical 3-winding transformer.
- Existing consumers can continue reading both `transformers_2w` and `transformers_3w`; the representation mode metadata indicates which form is authoritative for 3-winding devices in that file.

### v0.8.3 → v0.8.4: Planning-vs-Solved Semantics

**Summary of changes:**

- **Metadata table**: 6 new columns added (case_mode, solved_state_presence, solver_version, solver_iterations, solver_accuracy, solver_mode)
- **Root metadata**: 2 new keys added (rpf.case_mode, rpf.solved_state_presence)
- **Voltage planning semantics**: v_mag_set and v_ang_set now represent flat-start planning values, not snapshot state

**PSS/E converter impact:**

1. **All RAW files now export as `case_mode = flat_start_planning`** (planning case, not solved snapshot)
2. **Voltage setpoint (`buses.v_mag_set`)**:
   - **Before v0.8.4**: For buses without a valid generator VS, fallback to BUS.VM (snapshot voltage) — **INCORRECT** for planning
   - **After v0.8.4**: For buses without a valid generator VS, use 1.0 pu flat-start default — **CORRECT** for planning
   - Valid generator VS (0.85–1.15 pu) still used for PV buses
3. **Voltage angle (`buses.v_ang_set`)**:
   - **Before v0.8.4**: Used BUS.VA (snapshot angle) — **INCORRECT** for planning
   - **After v0.8.4**: Always 0.0 rad (flat start) — **CORRECT** for planning
4. **Solved state (`buses_solved`, `generators_solved`)**:
   - **Before v0.8.4**: Not present (no solved tables)
   - **After v0.8.4**: Not present (no solved tables; PSS/E is planning-only converter)
5. **Solver provenance**:
   - **Before v0.8.4**: Not present
   - **After v0.8.4**: All null (planning export, no solver provenance)

**Backward compatibility:**

- v0.8.3-produced MFR files with incorrect v_mag_set/v_ang_set will **NOT** auto-convert
- Regenerate all golden test files using v0.8.4 converter
- Reader tools must support both versions (locked contract) but will observe planning vs solved semantics enforced by case_mode

---

## 0.2.1 converter status

`raptrix-psse-rs` 0.2.1 is no longer in the early section-by-section port phase.
The converter now supports production-scale static-network export plus a
solver-usable subset of PSS/E dynamic data.

### Implemented in 0.2.1

- RAW sections 0, 1, 2, 3, 4, 5, 6, 7, 13, 15, and 17 export into canonical RPF tables.
- Bus-level `p_sched`, `q_sched`, `g_shunt`, and `b_shunt` aggregates are materialized in per-unit for solver parity.
- Two-winding transformer `nominal_tap_ratio` is derived from `NOMV1 / NOMV2` when available.
- Transformer `vector_group` is emitted as `"unknown"` instead of a fabricated IEC code.
- All numeric DYR model rows are preserved in `dynamics_models`.
- Supported synchronous-machine families `GENROU`, `GENROE`, `GENSAL`, `GENSAE`, and `GENCLS` populate generator `h`, `D`, and `xd_prime` fields.
- Texas static and dynamic golden cases are regenerated as `.rpf` artifacts during `cargo test`.

### Known limitations (historical 0.2.1 notes)

- Additional DYR model families remain as rows in `dynamics_models`; callers map fields they need—this converter does not expand them into other tables.
- Transformer per-winding impedance decomposition (`winding1_r/x`, `winding2_r/x`) still exports zeros where not decomposed from RAW.
- RAW ZIP load components (`IP`, `IQ`, `YP`, `YQ`) are not represented in older RPF lines referenced here.
- RAW sections 8–12, 14, 18–20, and v35 system switching devices are skipped by the parser.

### Release-validation workflow

1. Run `cargo test` to regenerate and validate `.rpf` artifacts under `tests/golden/` when those tests are enabled locally.
2. Validate outputs with your downstream toolchain as needed.
3. Update `docs/psse-mapping.md` when parser coverage changes so field-level behavior stays documented.

---

## Appendix — implementation references

Long-form “how to port the next RAW section” notes were removed from this file: they read like an internal sequencing plan and are easy to misinterpret as a public roadmap. **Current behavior** is defined by the shipped code and by [`docs/psse-mapping.md`](docs/psse-mapping.md). **Regression workflow** for large cases is described in [`tests/golden/README.md`](tests/golden/README.md).
