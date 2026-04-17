<!--
  raptrix-psse-rs
  Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# raptrix-psse-rs

High-performance PSS/E (.raw + .dyr) to Raptrix PowerFlow Interchange (.rpf) conversion for modern grid workflows.

Part of the Raptrix PowerFlow ecosystem.

## Ecosystem Repos

- [raptrix-cim-rs](https://github.com/RaptrixPowerFlow/raptrix-cim-rs) - Unlimited-size CIM to RPF converter suite.
- [raptrix-psse-rs](https://github.com/RaptrixPowerFlow/raptrix-psse-rs) - Unlimited-size PSS/E to RPF converter.
- [raptrix-studio](https://github.com/RaptrixPowerFlow/raptrix-studio) - Free unlimited RPF viewer/editor.

**Canonical model:** The IEC 61970 CIM is the authoritative source for our data model and mappings. The public repository [raptrix-cim-rs](https://github.com/RaptrixPowerFlow/raptrix-cim-rs) implements the CIM schema and should be treated as the canonical reference for schema, mappings, and conversion logic. This repository no longer contains an embedded `raptrix-cim-arrow` crate; depend on `raptrix-cim-rs` for CIM-related functionality.

## Quick Start

```bash
raptrix-psse-rs convert --raw my_case.raw --output my_case.rpf
raptrix-psse-rs convert --raw my_case.raw --output my_case_expanded.rpf --transformer-mode expanded
raptrix-psse-rs view --input my_case.rpf
```

## Download prebuilt binaries (recommended)

We provide prebuilt release binaries on GitHub Releases for Windows, Linux, and macOS. For most users we recommend downloading the appropriate release artifact rather than building from source — binaries are built with optimization and link-time optimizations enabled for best runtime performance.

See the Releases page: https://github.com/RaptrixPowerFlow/raptrix-psse-rs/releases

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-blue.svg)](LICENSE)

MPL 2.0 — free to use, modify, and distribute.

For production-scale grids, see the commercial Raptrix Core offering. Flexible commercial licensing — contact us for seats, enterprise, or cloud options.

---

## Why RPF? The case for a modern interchange format

PSS/E RAW, CIM/XML, and similar vendor formats were designed for human editors and
1970s–1990s toolchains. They carry significant structural baggage:

| Legacy format pain point | How RPF solves it |
|---|---|
| **Line-by-line ASCII parsing** — slow, fragile, encoding-ambiguous | **Apache Arrow IPC binary** — columnar, memory-mappable, zero-copy into the solver |
| **Loosely specified fields** — optional columns, dialect differences between PSS/E 29–35, silent truncation | **Schema-versioned and strongly typed** — every field has a defined type, nullability, and unit; schema mismatches are caught at read time |
| **No planning vs. solved distinction** — VM/VA in a RAW file could be a solved snapshot or a wild guess | **Explicit case semantics** — `case_mode` field encodes `flat_start_planning`, `warm_start_planning`, or `solved`; no ambiguity for the downstream solver |
| **Monolithic single-file model** — topology, operating point, dynamics, and contingencies all mixed in opaque section blocks | **15 canonical tables** — buses, branches, generators, transformers, contingencies, dynamics models, and more, each independently addressable |
| **Vendor lock-in** — RAW files require a PSS/E license to create or simulate | **MPL 2.0 open standard** — read, write, and inspect with any Apache Arrow library in Rust, Python, R, Go, or Java; no license required |
| **No extensibility** — adding FACTS or hosting-capacity fields requires vendor cooperation | **Nullable extension columns** — FACTS, SCED, POI/hosting-capacity data lives in first-class nullable columns; older readers ignore unknown fields gracefully |

### In practice

- **Faster ingestion**: the entire RPF payload is one contiguous Arrow IPC buffer. A 70 000-bus case loads into the solver in a single memory-map call — no tokenizing, no string-to-float conversion at runtime.
- **Safer pipelines**: schema validation catches unit errors, missing slack buses, and topology anomalies before a single solver iteration runs. The optional `raptrix-psse-rs validate` command runs MMWG §7.3 conformance checks on any RAW file before conversion.
- **Smarter workflows**: contingency tables, interface limits, dynamics model parameters, and area interchange schedules all travel in the same file. No more assembling five separate inputs before a security analysis run.

> RPF is the interchange format that PSS/E RAW would be if it were designed today.

---

## Build From Source

Rust 1.85+ is required. Building from source is supported but not necessary for most users — prefer the prebuilt release artifacts when possible.

```bash
git clone https://github.com/RaptrixPowerFlow/raptrix-cim-rs.git
git clone https://github.com/RaptrixPowerFlow/raptrix-psse-rs.git
cd raptrix-psse-rs
cargo build --release
```

## CLI Reference

### convert

```bash
raptrix-psse-rs convert --raw <FILE> [--dyr <FILE>] --output <FILE> [--transformer-mode <MODE>]
```

| Flag | Required | Description |
|------|----------|-------------|
| `--raw <PATH>` | yes | PSS/E RAW file (.raw), versions 29-35. |
| `--dyr <PATH>` | no | Optional PSS/E dynamic data file (.dyr). All numeric DYR model rows are preserved into `dynamics_models`; supported machine families also populate generator `h`, `D`, and `xd_prime`. |
| `--output <PATH>` | yes | Output RPF file path. |
| `--transformer-mode <MODE>` | no | 3-winding representation policy: `native-3w` (default, native `transformers_3w` rows only) or `expanded` (star legs in `transformers_2w`). |

Each export writes machine-readable root metadata key `rpf.transformer_representation_mode`
with value `expanded` or `native_3w` for deterministic downstream regression checks. Files produced against the v0.8.7 canonical contract default to `native_3w` unless `--transformer-mode expanded` is supplied.

### view

```bash
raptrix-psse-rs view --input <FILE>
```

Prints a summary of every table in the .rpf file with row counts.

## RPF Contents

The .rpf file is an Apache Arrow IPC payload with 15 canonical tables:

- metadata
- buses
- branches
- generators
- loads
- fixed_shunts
- switched_shunts
- transformers_2w
- transformers_3w
- areas
- zones
- owners
- contingencies
- interfaces
- dynamics_models

With a paired `.dyr` file, `dynamics_models` now carries the full numeric model deck in source order using parameter keys `p1..pN`. Synchronous machine families `GENROU`, `GENROE`, `GENSAL`, `GENSAE`, and `GENCLS` are also promoted into the `generators` table for solver initialization fields.

## Testing

Place any confidential or licensed PSS/E input files under tests/data/external/, then run:

```bash
cargo test --release -- --nocapture
```

## License

Licensed under the Mozilla Public License, Version 2.0. See [LICENSE](LICENSE).
