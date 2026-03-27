<!--
  Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
  Copyright (c) 2026 Musto Technologies LLC

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# raptrix-psse-rs

**Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC**  
Copyright (c) 2026 Musto Technologies LLC

High-performance **PSS/E (`.raw` + `.dyr`) → Raptrix PowerFlow Interchange v0.6.0** converter,  
written in Rust 2024 with a zero-copy mindset.

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-blue.svg)](LICENSE)

---

## Table of Contents

1. [Overview](#overview)
2. [Getting Started](#getting-started)
3. [CLI Reference](#cli-reference)
4. [How to depend on `raptrix-cim-arrow`](#how-to-depend-on-raptrix-cim-arrow)
5. [Repository layout](#repository-layout)
6. [Performance snapshot](#performance-snapshot)
7. [Contributing & porting from C++](#contributing--porting-from-c)
8. [License](#license)

---

## Overview

`raptrix-psse-rs` converts **PSS/E RAW** (versions 29 – 35) and optional **DYR**
dynamic data files into the Raptrix PowerFlow Interchange (`.rpf`) format — a
compact, zero-copy Apache Arrow IPC payload used by the Raptrix power-flow
solver.

Serialisation is delegated to the shared [`raptrix-cim-arrow`] crate, which
provides the Arrow schema, encoding, and `.rpf` I/O primitives.

---

## Getting Started

### Prerequisites

* Rust 1.85 or later (`rustup install stable`)
* The sibling workspace `raptrix-cim-rs` checked out at `../raptrix-cim-rs`
  (or update the path dependency — see [below](#how-to-depend-on-raptrix-cim-arrow))

### Build

```bash
cargo build --release
```

### Run the tests

```bash
cargo test
```

---

## CLI Reference

### `convert` — PSS/E → RPF

```bash
raptrix-psse-rs convert \
  --raw  case.raw  \
  [--dyr case.dyr] \
  --output case.rpf
```

| Flag | Required | Description |
|------|----------|-------------|
| `--raw <PATH>`    | ✅ | Path to the PSS/E RAW file (`.raw`). |
| `--dyr <PATH>`    | ❌ | Optional path to the PSS/E dynamic data file (`.dyr`). |
| `--output <PATH>` | ✅ | Destination path for the Raptrix PowerFlow Interchange file (`.rpf`). |

### `view` — inspect an RPF file

```bash
raptrix-psse-rs view --input case.rpf
```

| Flag | Required | Description |
|------|----------|-------------|
| `--input <PATH>` | ✅ | Path to the `.rpf` file to pretty-print. |

---

## How to depend on `raptrix-cim-arrow`

### During development (sibling workspace)

```toml
# Cargo.toml
[dependencies]
raptrix-cim-arrow = { path = "../raptrix-cim-rs/raptrix-cim-arrow" }
```

Ensure `raptrix-cim-rs` is checked out at the path relative to this
repository, e.g.:

```
workspace/
├── raptrix-cim-rs/        ← sibling repo
│   └── raptrix-cim-arrow/
└── raptrix-psse-rs/       ← this repo
```

### From crates.io (future)

```toml
raptrix-cim-arrow = "0.6"
```

---

## Repository layout

```
raptrix-psse-rs/
├── Cargo.toml                  # crate manifest
├── LICENSE                     # MPL-2.0
├── README.md                   # this file
├── MIGRATION.md                # guide for porting C++ parser logic
├── src/
│   ├── lib.rs                  # crate root + PSS/E → RPF converter
│   ├── main.rs                 # CLI entry-point (clap)
│   ├── models.rs               # PSS/E data structures
│   └── parser.rs               # RAW / DYR parser
├── docs/
│   └── psse-mapping.md         # field-by-field PSS/E → RPF mapping rules
└── tests/
    ├── data/
    │   └── external/           # test RAW files (e.g. Texas7k_20210804.RAW)
    ├── golden/
    │   └── README.md           # golden-file workflow
    └── golden_test.rs          # integration test: converts Texas7k and checks row counts
```

---

## Producing your first RPF from Texas7k

```bash
cargo run --release -- convert \
  --raw tests/data/external/Texas7k_20210804.RAW \
  --output case.rpf
```

Then inspect it:

```bash
cargo run --release -- view --input case.rpf
```

Expected output (abbrev.):

```
RPF file: case.rpf
  tables: 15   total rows: <N>   all canonical: true
  metadata                            1 rows
  buses                            7717 rows
  branches                         8082 rows
  generators                        706 rows
  loads                            5135 rows
  ...
```

---

## Performance snapshot

> **Note:** Numbers below are placeholders and will be updated once the C++
> port is complete and benchmarks are run on the reference hardware.

| Case size | Buses | Parse time | Output size |
|-----------|-------|-----------|-------------|
| Small     | 118   | < 1 ms    | < 10 KB     |
| Medium    | 2 868 | < 10 ms   | < 200 KB    |
| Large     | 70 k  | < 500 ms  | < 8 MB      |

Benchmarking methodology: `cargo bench` using `criterion`, measured on
an AMD Ryzen 9 7950X with files mapped via `memmap2`.

---

## Contributing & porting from C++

### Next: port C++ logic

The current Rust parser handles PSS/E RAW format v29–v35 with a simple
line-by-line approach.  To match the speed and completeness of the C++ codebase:

1. **Zero-copy tokeniser** — replace `String`-per-field splitting with a
   `memmap2`-backed zero-copy line iterator.
2. **3-winding transformers** — implement the 5-line record parser and add a
   `ThreeWindingTransformer` struct; populate `TABLE_TRANSFORMERS_3W`.
3. **DYR parsing** — parse GENSAL / GENROU / ESST1A / IEEEG1 records and
   fill `TABLE_DYNAMICS_MODELS`.
4. **Bus aggregated fields** — compute `p_sched`, `q_sched`, `g_shunt`,
   `b_shunt`, `p_min_agg`, `p_max_agg` from loads / generators / shunts.
5. **Vector group mapping** — detect CW/CZ codes and emit correct CIM
   VectorGroup strings in `TABLE_TRANSFORMERS_2W`.

See [MIGRATION.md](MIGRATION.md) for section-by-section porting notes and
[docs/psse-mapping.md](docs/psse-mapping.md) for field-by-field mapping rules.

---

## License

This project is licensed under the **Mozilla Public License, Version 2.0**.  
See [LICENSE](LICENSE) for the full text.

> Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC  
> Copyright (c) 2026 Musto Technologies LLC
