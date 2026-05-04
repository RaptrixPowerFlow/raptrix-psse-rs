<!--
  raptrix-psse-rs
  Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# Golden regression outputs (`tests/golden/`)

**raptrix-psse-rs**  
Copyright (c) 2026 Raptrix PowerFlow

Integration tests write `.rpf` artifacts here **when** the matching inputs exist
locally under `tests/data/external/` (that tree is **gitignored**—do not commit
vendor or licensed RAW/DYR files to the public repo).

Generated `tests/golden/*.rpf` files are also **gitignored** by default; they
exist for local inspection and regression, not as checked-in fixtures.

---

## Layout

```text
tests/golden/
├── README.md          ← this file
└── <casename>_*.rpf   ← produced by `golden_test` / manual convert (local only)

tests/data/external/  ← you provide inputs here (not in git)
├── <casename>.RAW
├── <casename>.dyr     ← optional PSS/E dynamics
└── <casename>.dyn     ← optional (some corpora ship `.dyn` instead)
```

### Dynamics deck precedence (`.dyn` vs `.dyr`)

`tests/golden_test.rs` and `scripts/verify-external-golden.sh` stay in sync for the
external corpus. For **ACTIVSg10k** and **Texas2k GFM** (`Texas2k_series24_case6_2024lowloadwithgfm`),
if **both** `.dyn` and `.dyr` exist next to the RAW, **`.dyn` is chosen first**. Remove
or rename the deck you do not want used so conversions stay deterministic. Texas7k
and Texas2k summerpeak dynamic tests use a **fixed `.dyr`** path only.

### Expected `tests/data/external/` filenames

These paths are hard-coded in [`tests/golden_test.rs`](../golden_test.rs) and mirrored in
[`scripts/verify-external-golden.sh`](../../scripts/verify-external-golden.sh). Spelling and
casing must match exactly. Extra files in that directory are ignored until you add a matching
`#[test]` (and script lines, if you use the shell verifier).

| Inputs | Notes |
|--------|--------|
| `Texas7k_20220923.RAW` | static only |
| `Texas7k_20210804.RAW`, `Texas7k_20210804.dyr` | static + dynamic |
| `Texas2k_series25_case1_summerpeak.RAW`, `.dyr` | static + dynamic |
| `Base_Eastern_Interconnect_515GW.RAW` | static |
| `ACTIVSg10k.RAW` | static; dynamic uses `.dyn` if present else `.dyr` |
| `Texas2k_series24_case6_2024lowloadwithgfm.RAW` | dynamic only; `.dyn` preferred over `.dyr` |
| `IEEE_14_bus.raw`, `IEEE_118_Bus.RAW` | static |
| `NYISO_offpeak2019_v23.raw`, `NYISO_onpeak2019_v23.raw` | static |
| `NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld.raw` | static |
| `Texas7k_2030_20220923.RAW` | static |
| `Midwest24k_20220923.RAW`, `ACTIVSg25k.RAW`, `ACTIVSg70k.RAW` | static |

---

## Running regressions locally

1. Place inputs under `tests/data/external/` using the paths referenced in `tests/golden_test.rs`.
2. Run:

```bash
cargo test --release --test golden_test -- --nocapture
```

3. Inspect outputs under `tests/golden/` if needed.

To convert a single case with the CLI:

```bash
cargo run --release -- convert \
  --raw tests/data/external/<casename>.RAW \
  [--dyr tests/data/external/<casename>.dyr] \
  --output tests/golden/<casename>_static.rpf
```

---

## Adding a new regression

1. Add a test in `tests/golden_test.rs` that **skips** when the expected external
   file is missing (so CI without licensed data stays green).
2. Assert interchange invariants that matter for your workflow (table presence,
   row counts, metadata keys)—keep assertions **factual**, not roadmap-shaped.
3. Document any new filename conventions here in one line so maintainers know
   where to drop local inputs.

Example shape (paths illustrative only):

```rust
#[test]
fn golden_example_static() {
    const RAW: &str = "tests/data/external/Example.RAW";
    if !std::path::Path::new(RAW).exists() {
        eprintln!("[skip] {RAW} not found");
        return;
    }
    raptrix_psse_rs::write_psse_to_rpf(RAW, None, "tests/golden/Example_static.rpf")
        .expect("convert");
}
```
