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

## Quick Start

```bash
raptrix-psse-rs convert --raw my_case.raw --output my_case.rpf
raptrix-psse-rs view --input my_case.rpf
```

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-blue.svg)](LICENSE)

MPL 2.0 - free to use, modify, and distribute.

For production-grid solving and security analysis workflows, see the commercial Raptrix core offering: [contact Raptrix PowerFlow](https://github.com/RaptrixPowerFlow).

Flexible commercial licensing - contact us for seats, enterprise, or cloud options.

## Build From Source

Rust 1.85+ is required.

```bash
git clone https://github.com/RaptrixPowerFlow/raptrix-cim-rs.git
git clone https://github.com/RaptrixPowerFlow/raptrix-psse-rs.git
cd raptrix-psse-rs
cargo build --release
```

## CLI Reference

### convert

```bash
raptrix-psse-rs convert --raw <FILE> [--dyr <FILE>] --output <FILE>
```

| Flag | Required | Description |
|------|----------|-------------|
| `--raw <PATH>` | yes | PSS/E RAW file (.raw), versions 29-35. |
| `--dyr <PATH>` | no | Optional PSS/E dynamic data file (.dyr). |
| `--output <PATH>` | yes | Output RPF file path. |

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

## Testing

Place any confidential or licensed PSS/E input files under tests/data/external/, then run:

```bash
cargo test --release -- --nocapture
```

## License

Licensed under the Mozilla Public License, Version 2.0. See [LICENSE](LICENSE).
