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

This directory holds input/output pairs used to drive regression tests for the
PSS/E converter.  Adding a golden file is the recommended way to verify that a
newly ported parser section produces byte-identical output.

---

## Directory structure

```
tests/golden/
├── README.md                 ← this file
├── <casename>.raw            ← PSS/E RAW input file
├── <casename>.dyr            ← PSS/E DYR input file (optional)
└── <casename>.rpf            ← expected Raptrix PowerFlow Interchange output
```

---

## Workflow

### 1 — Add an input file

Copy a `.raw` (and optionally `.dyr`) file into this directory.  The file must
be freely distributable (e.g. the IEEE 14-bus, 39-bus, or 118-bus test cases
published by the IEEE PES).

### 2 — Generate the reference output

Run the converter after porting the relevant parser sections:

```bash
cargo run --release -- convert \
  --raw  tests/golden/<casename>.raw \
  [--dyr tests/golden/<casename>.dyr] \
  --output tests/golden/<casename>.rpf
```

Visually verify the output:

```bash
cargo run --release -- view --input tests/golden/<casename>.rpf
```

### 3 — Write the regression test

Add a `#[test]` in `tests/` (or `src/`) that:

1. Calls `raptrix_psse_rs::parser::parse_raw(Path::new("tests/golden/<casename>.raw"))`.
2. Asserts the expected bus count, load count, and generator count.
3. Converts to RPF in a temporary file.
4. Byte-compares the temporary file against `tests/golden/<casename>.rpf`.

Example skeleton:

```rust
#[test]
fn golden_ieee14() {
    use std::path::Path;

    let network = raptrix_psse_rs::parser::parse_raw(
        Path::new("tests/golden/ieee14.raw"),
    )
    .expect("parse failed");

    assert_eq!(network.buses.len(), 14);
    assert_eq!(network.loads.len(), 11);
    assert_eq!(network.generators.len(), 5);

    // TODO: compare RPF bytes once the writer is wired up.
}
```

### 4 — Commit both files

Commit the `.raw` input and the `.rpf` golden output together so that CI can
reproduce the comparison deterministically.

---

## Updating golden files

If a deliberate change to the output format causes golden mismatches:

1. Re-generate the golden file with the new converter.
2. Review the diff carefully.
3. Commit the updated golden file with a clear commit message explaining the
   format change.
