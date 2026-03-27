<!--
  Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
  Copyright (c) 2026 Musto Technologies LLC

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# raptrix-psse-rs

**High-performance PSS/E → Raptrix PowerFlow Interchange converter**  
by [Musto Technologies LLC](https://mustotechnologies.com)

Convert your PSS/E `.raw` (and optional `.dyr`) case files into the open
**Raptrix PowerFlow Interchange** (`.rpf`) format — a compact
[Apache Arrow IPC](https://arrow.apache.org/docs/format/IPC.html) payload
ready for the Raptrix power-flow solver and any Arrow-compatible toolchain.

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/MustoTechnologies/raptrix-psse-rs)](https://github.com/MustoTechnologies/raptrix-psse-rs/releases/latest)

---

## Download a prebuilt binary (no Rust required)

Head to the [**Releases**](https://github.com/MustoTechnologies/raptrix-psse-rs/releases/latest)
page and download the binary for your platform:

| Platform | File |
|----------|------|
| Windows 64-bit | `raptrix-psse-rs-windows-x86_64.zip` |
| Linux 64-bit   | `raptrix-psse-rs-linux-x86_64.tar.gz` |
| macOS (Apple Silicon) | `raptrix-psse-rs-macos-aarch64.tar.gz` |

Unzip and run directly — no installer, no dependencies.

---

## Quick start

### Convert a static case (RAW only)

```
raptrix-psse-rs convert --raw my_case.raw --output my_case.rpf
```

### Convert with dynamics (RAW + DYR)

```
raptrix-psse-rs convert --raw my_case.raw --dyr my_case.dyr --output my_case_dynamic.rpf
```

### Inspect an RPF file

```
raptrix-psse-rs view --input my_case.rpf
```

Example output:

```
RPF file: my_case.rpf
  tables: 15   total rows: 22328   all canonical: true
  metadata                            1 rows
  buses                            6717 rows
  branches                         7173 rows
  generators                        731 rows
  loads                             5095 rows
  fixed_shunts                       205 rows
  switched_shunts                    429 rows
  transformers_2w                   1967 rows
  ...
  dynamics_models                    524 rows   ← populated when --dyr is supplied
```

---

## Windows installation

1. Download `raptrix-psse-rs-windows-x86_64.zip` from the [Releases](https://github.com/MustoTechnologies/raptrix-psse-rs/releases/latest) page.
2. Extract the zip — you will find `raptrix-psse-rs.exe`.
3. Open **Command Prompt** or **PowerShell** in the folder containing the `.exe`.
4. Run:
   ```powershell
   .\raptrix-psse-rs.exe convert --raw C:\path\to\case.raw --output C:\path\to\case.rpf
   ```
5. *(Optional)* Add the folder to your `PATH` so you can run `raptrix-psse-rs` from anywhere.

## Linux installation

```bash
# Download and extract
curl -L https://github.com/MustoTechnologies/raptrix-psse-rs/releases/latest/download/raptrix-psse-rs-linux-x86_64.tar.gz \
  | tar xz

# Run
./raptrix-psse-rs convert --raw /path/to/case.raw --output /path/to/case.rpf

# Optional: install system-wide
sudo mv raptrix-psse-rs /usr/local/bin/
```

---

## CLI reference

### `convert`

```
raptrix-psse-rs convert --raw <FILE> [--dyr <FILE>] --output <FILE>
```

| Flag | Required | Description |
|------|----------|-------------|
| `--raw <PATH>`    | ✅ | PSS/E RAW file (`.raw`), versions 29–35. |
| `--dyr <PATH>`    | ❌ | PSS/E dynamic data file (`.dyr`). Populates `dynamics_models` and enriches generators with H, D, Xd′. |
| `--output <PATH>` | ✅ | Output Raptrix PowerFlow Interchange file (`.rpf`). |

### `view`

```
raptrix-psse-rs view --input <FILE>
```

Prints a summary of every table in the `.rpf` file with row counts.

---

## What does the RPF file contain?

The `.rpf` file is a standard Apache Arrow IPC file.  
It always contains exactly **15 canonical tables**:

| Table | Contents |
|-------|----------|
| `metadata` | Base MVA, frequency, PSS/E version, timestamp |
| `buses` | Bus ID, name, type, voltage set-points, area/zone/owner |
| `branches` | Lines: R, X, B, ratings, status |
| `generators` | P/Q limits, status, machine base, H, D, Xd′ (from DYR) |
| `loads` | P, Q, status |
| `fixed_shunts` | G, B, status |
| `switched_shunts` | Step counts, B per step, control voltage band |
| `transformers_2w` | R, X, tap ratio, phase shift, ratings |
| `transformers_3w` | *(reserved — populated in a future release)* |
| `areas` | Area ID, name, scheduled interchange |
| `zones` | Zone ID, name |
| `owners` | Owner ID, name |
| `contingencies` | *(reserved — populated in a future release)* |
| `interfaces` | *(reserved — populated in a future release)* |
| `dynamics_models` | bus_id, gen_id, model type, params map (H, D, Xd′) — requires `--dyr` |

You can read `.rpf` files with any Arrow IPC reader (Python `pyarrow`, R `arrow`, etc.).

---

## Performance

Measured on an AMD Ryzen 9 7950X, release build (`cargo build --release`):

| Case | Buses | Generators | Elapsed (static) | Elapsed (+ DYR) |
|------|-------|-----------|-----------------|-----------------|
| Texas 7k | 6,717 | 731 | **67 ms** | **70 ms** |

---

## Building from source

You need Rust 1.85+ and the sibling `raptrix-cim-rs` repo:

```bash
# Clone both repos side-by-side
git clone https://github.com/MustoTechnologies/raptrix-cim-rs.git
git clone https://github.com/MustoTechnologies/raptrix-psse-rs.git

cd raptrix-psse-rs
cargo build --release
# Binary is at: target/release/raptrix-psse-rs  (or .exe on Windows)
```

### Run the test suite

Place your PSS/E test case files in `tests/data/external/` (this directory is
git-ignored — files are never committed to the repository), then:

```bash
cargo test --release -- --nocapture
```

---

## Repository layout

```
raptrix-psse-rs/
├── src/
│   ├── lib.rs        # PSS/E → RPF converter + Arrow table builders
│   ├── main.rs       # CLI (clap)
│   ├── models.rs     # PSS/E data structures
│   └── parser.rs     # RAW / DYR parser
├── tests/
│   ├── golden_test.rs          # integration tests (static + dynamic)
│   ├── data/external/          # ⚠ git-ignored — put your .raw/.dyr files here
│   └── golden/                 # generated .rpf output (git-ignored)
├── docs/
│   └── psse-mapping.md         # field-by-field PSS/E → RPF mapping rules
└── .github/workflows/
    └── release.yml             # CI: builds & publishes binaries on git tag push
```

---

## License

Licensed under the **Mozilla Public License, Version 2.0**.  
See [LICENSE](LICENSE) for the full text.

> Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC  
> Copyright (c) 2026 Musto Technologies LLC


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
