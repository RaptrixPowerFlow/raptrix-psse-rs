<!--
  raptrix-psse-rs
  Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# Golden-file tests for raptrix-psse-rs

**raptrix-psse-rs**  
Copyright (c) 2026 Raptrix PowerFlow

This directory holds generated `.rpf` outputs used to drive regression tests for
the PSS/E converter. Running `cargo test` rewrites these artifacts for cases
whose licensed inputs are present under `tests/data/external/`.

---

## Directory structure

```
tests/golden/
├── README.md                 ← this file
└── <casename>.rpf            ← generated Raptrix PowerFlow Interchange output

tests/data/external/
├── <casename>.RAW            ← licensed or external PSS/E RAW input
└── <casename>.dyr            ← optional PSS/E DYR input
```

---

## Workflow

### 1 — Add an input file

Place the `.RAW` file and optional `.dyr` file under `tests/data/external/`.
Licensed planning cases stay out of the golden directory; only the generated
`.rpf` outputs are checked in under `tests/golden/`.

### 2 — Generate the reference output

Run the regression suite after changing the converter:

```bash
cargo test -- --nocapture
```

To regenerate a single case manually:

```bash
cargo run --release -- convert \
  --raw tests/data/external/<casename>.RAW \
  [--dyr tests/data/external/<casename>.dyr] \
  --output tests/golden/<casename>.rpf
```

### 3 — Write the regression test

Add a `#[test]` in `tests/` (or `src/`) that:

1. Calls `write_psse_to_rpf` for the relevant RAW/DYR pair.
2. Asserts table counts and key solver-facing invariants.
3. Leaves the regenerated `.rpf` under `tests/golden/` for inspection.
4. Uses `summarize_rpf`, `rpf_file_metadata`, or `read_rpf_tables` to validate the output contract.

Example skeleton:

```rust
#[test]
fn golden_texas2k_dynamic() {
  raptrix_psse_rs::write_psse_to_rpf(
    "tests/data/external/Texas2k_series25_case1_summerpeak.RAW",
    Some("tests/data/external/Texas2k_series25_case1_summerpeak.dyr"),
    "tests/golden/Texas2k_series25_dynamic.rpf",
  )
  .expect("conversion failed");

  let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(
    "tests/golden/Texas2k_series25_dynamic.rpf",
  ))
  .expect("summary failed");

  assert!(summary.has_all_canonical_tables);
  assert!(
    summary.tables.iter().any(|t| t.table_name == "dynamics_models" && t.rows > 0)
  );
}
```

### 4 — Commit both files

Commit the `.raw` input and the `.rpf` golden output together so that CI can
reproduce the comparison deterministically.

---

## Updating golden files

If a deliberate converter change updates the generated `.rpf` files:

1. Re-generate the golden file with the new converter.
2. Review the diff carefully.
3. Commit the updated golden file with a clear commit message explaining the
   format change.
