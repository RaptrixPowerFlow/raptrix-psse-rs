<!--
    raptrix-psse-rs
  Copyright (c) 2026 Musto Technologies LLC

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# Porting the C++ PSS/E Parser to Rust

**raptrix-psse-rs**  
Copyright (c) 2026 Musto Technologies LLC

# Porting the C++ PSS/E Parser to Rust & Schema Version Migrations

**raptrix-psse-rs**  
Copyright (c) 2026 Musto Technologies LLC

---

## RPF Schema Version Migrations

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

## C++ PSS/E Parser Porting Guide

This document is a step-by-step guide for incrementally porting the existing
C++ PSS/E parser into `src/parser.rs` and `src/models.rs`.

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
