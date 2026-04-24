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

---

## RPF Schema Version Migrations

### v0.3.1: RPF v0.8.9 generator layout → **v0.9.0** contract (Breaking)

Crate **0.3.2** tracks the latest **`raptrix-cim-arrow`** contract (currently **RPF v0.9.0**). Releases **0.3.1** and **0.3.2** cover the hierarchical generator model and the v0.9.0 wire shape (no `ibr_devices`; extended metadata / contingencies).

**Generator & ownership (from v0.8.9-era alignment):**

- `generators` unified hierarchical shape: `generator_id`, `unit_type`, `hierarchy_level`, `parent_generator_id`, `aggregation_count`, MW/MVAR-native dispatch fields, `is_ibr`, `ibr_subtype`, `owner_id`, `params`.
- Legacy flat RAW units export as `hierarchy_level = "unit"`, `parent_generator_id = null`, `aggregation_count = null`.
- `generators.owner_id`, `buses.owner_id`, `branches.owner_id`; `owners` shape with `short_name`, `type`, `params`.

**v0.9.0 wire contract:**

- Canonical RPF is **v0.9.0** (see `raptrix-cim-rs` `docs/schema-contract.md`).
- **`ibr_devices` removed** — **18** required root tables; IBRs only on **`generators`**.
- **`metadata`**: five nullable Sentinel-readiness columns (default **null** for PSS/E): `hour_ahead_uncertainty_band`, `commitment_source`, `solver_q_limit_infeasible_count`, `pv_to_pq_switch_count`, `real_time_discovery`.
- **`contingencies`**: six nullable Sentinel columns (null for stub/planning exports).
- **`case_mode`**: supports `hour_ahead_advisory` (CLI `--case-mode` / `ExportOptions::case_mode_override`; else inferred flat/warm from RAW).
- **Optional `scenario_context`**: not emitted by default; non-empty `ExportOptions::scenario_context_rows` **errors** until `raptrix-cim-arrow` optional-root IPC support exists.

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
- Default transformer export mode is now `native_3w` to match the canonical upstream writer default.
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

### Remaining solver-impacting gaps

- Exciters, governors, PSS, renewable controllers, and plant controllers are preserved in `dynamics_models`, but their semantics are not yet projected into solver-specific initialization tables.
- Transformer per-winding impedance decomposition (`winding1_r/x`, `winding2_r/x`) still exports zeros.
- RAW ZIP load components (`IP`, `IQ`, `YP`, `YQ`) are not represented in RPF v0.8.6.
- RAW sections 8–12, 14, 18–20, and v35 system switching devices are still skipped.

### Release-validation workflow

1. Run `cargo test` to regenerate and validate the checked-in `.rpf` artifacts under `tests/golden/`.
2. Hand the updated `.rpf` files to the core solver for capability validation.
3. Review `docs/psse-mapping.md` whenever a new section or DYR family is added so the solver-side reader contract stays aligned with the converter.

---

## Historical porting guide

This repository started as a step-by-step port of the existing C++ PSS/E parser
into `src/parser.rs` and `src/models.rs`. The notes below are retained as
historical implementation guidance.

---

## Guiding principles

1. **One section at a time.** Port section 1 (buses) first, run `cargo test`,
   then proceed to section 2 (loads), and so on.
2. **Zero-copy from the start.** Use `memmap2` to memory-map the input file and
   iterate over lines without allocating `String` values per line.
3. **Match field names exactly.** Keep the same field names as the PSS/E 35
   specification so that reviewers can cross-reference the C++ code directly.
4. **Golden files drive correctness.** Add a new `.raw` / `.rpf` pair to
   `tests/golden/` for each section you port. See `tests/golden/README.md`.

---

## Step 0 — Understand the RAW file format

A PSS/E RAW file is divided into sections separated by a line that starts with
`0 /`. Each section corresponds to a record type. A `Q` line terminates the
file.

```
<case identification record>        ← section 0
<bus data records>                  ← section 1
0 / END OF BUS DATA
<load data records>                 ← section 2
0 / END OF LOAD DATA
...
Q
```

Comments begin with `@` or are wrapped in `/* ... */`.

---

## Step 1 — Port bus data (section 1)

The C++ function to target first is the bus parser. In Rust it maps to:

```rust
// src/parser.rs  — inside parse_raw()
fn parse_bus_record(line: &str) -> Result<Bus, ParseError> {
    // TODO: split on ',' and parse each field.
    // Field order: I, NAME, BASKV, IDE, AREA, ZONE, OWNER, VM, VA,
    //              NVHI, NVLO, EVHI, EVLO
}
```

Remove the `todo!()` placeholder from `parse_raw()` once this function is
ready.

---

## Step 2 — Port load data (section 2)

```rust
fn parse_load_record(line: &str) -> Result<Load, ParseError> {
    // Fields: I, ID, STATUS, AREA, ZONE, PL, QL, IP, IQ, YP, YQ, OWNER,
    //         SCALE, INTRPT
}
```

---

## Step 3 — Port generator data (section 4)

```rust
fn parse_generator_record(line: &str) -> Result<Generator, ParseError> {
    // Fields: I, ID, PG, QG, QT, QB, VS, IREG, MBASE,
    //         ZR, ZX, RT, XT, GTAP, STAT, RMPCT, PT, PB,
    //         O1, F1, ..., O4, F4, WMOD, WPF
}
```

---

## Step 4 — Port branch data (section 5)

```rust
fn parse_branch_record(line: &str) -> Result<Branch, ParseError> {
    // Fields: I, J, CKT, R, X, B, RATEA, RATEB, RATEC,
    //         GI, BI, GJ, BJ, ST, MET, LEN, O1, F1, ..., O4, F4
}
```

---

## Step 5 — Port transformer data (section 6)

Transformers span **four** (2-winding) or **five** (3-winding) lines.  The
C++ code has a look-ahead that checks whether the `K` field is non-zero.

```rust
fn parse_transformer_block(lines: &[&str]) -> Result<Transformer, ParseError> {
    // Line 1: I, J, K, CKT, CW, CZ, CM, MAG1, MAG2, NMETR, NAME, STAT, O1, F1, ...
    // Line 2: R1-2, X1-2, SBASE1-2
    // Line 3: WINDV1, NOMV1, ANG1, RATA1, RATB1, RATC1, COD1, CONT1, ...
    // Line 4: WINDV2, NOMV2
    // (Line 5 only for 3-winding)
}
```

---

## Step 6 — Port DYR parsing

After the static network is complete, port the DYR parser in `parse_dyr()`.
The DYR file is a flat list of records, one per line, with the model name in
column 3.

```rust
// src/parser.rs
fn parse_dyr_record(line: &str) -> Result<DynRecord, ParseError> {
    // Field 1: bus number
    // Field 2: machine ID
    // Field 3: model name (e.g. "GENSAL", "ESST1A")
    // Remaining fields: model parameters (varies by model)
}
```

---

## Error handling

Introduce a `ParseError` enum in `src/parser.rs`:

```rust
#[derive(Debug)]
pub enum ParseError {
    /// A required field is missing.
    MissingField { section: u8, field: &'static str },
    /// A field could not be parsed as the expected type.
    InvalidField { section: u8, field: &'static str, value: String },
    /// I/O error while reading the file.
    Io(std::io::Error),
}
```

---

## Performance tips

* **Memory-map the file** with `memmap2::Mmap` instead of `std::fs::read_to_string`.
* **Split on `\n`** (handle `\r\n` by stripping trailing `\r`).
* **Avoid allocating `String`** for each field — parse directly from `&str` slices.
* Use `str::split_once(',')` or `splitn(n, ',')` rather than `split(',').collect::<Vec<_>>()`.
* Intern bus names with a `HashMap<&str, u32>` index built during section 1 parsing.

---

## Updating the golden tests

After porting each section:

1. Run `cargo run -- convert --raw tests/golden/ieee118.raw --output /tmp/ieee118.rpf`.
2. Visually verify the output with `cargo run -- view --input /tmp/ieee118.rpf`.
3. Copy `/tmp/ieee118.rpf` to `tests/golden/ieee118.rpf`.
4. Add a `#[test]` that calls `parse_raw` and asserts expected bus/load counts.

See `tests/golden/README.md` for details.
