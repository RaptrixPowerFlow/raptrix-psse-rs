<!--
  raptrix-psse-rs
  Copyright (c) 2026 Raptrix PowerFlow

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# PSS/E → Raptrix PowerFlow Interchange — Field Mapping

**raptrix-psse-rs**
Copyright (c) 2026 Raptrix PowerFlow

This document provides the field-by-field rules for translating PSS/E RAW (v23–v35)
and DYR records into the Raptrix PowerFlow Interchange (`.rpf` / RPF v0.8.6) Apache
Arrow schema.

> **Fidelity policy**: numeric fields are written exactly as they appear in the
> source RAW file unless an explicit normalisation rule is documented below.
> No value clamping, substitution, or scaling is applied at parse time except
> where required to match the RPF schema units (e.g. MVA → per-unit on SBASE).
> Validation and singularity handling are the responsibility of the downstream solver.

---

## Version compatibility

| PSS/E RAW revision | Supported | Notes |
|---|---|---|
| v23 – v34 | ✓ | v33 is the most common; treated as baseline layout. |
| v35 | ✓ | Extra fields (branch NAME, generator NREG, switched-shunt NAME/NREG) detected via `VersionOffsets` struct. |

---

## Units and per-unit conventions

All power quantities in RPF are **per-unit on the system MVA base (SBASE)** unless
stated otherwise. Reactive limits, ratings, and shunt values are normalised:

```
value_pu = value_mva / SBASE
```

Angles are stored in **radians** in RPF (PSS/E uses degrees).

---

## Bus aggregation

Before writing the `buses` table the converter accumulates a `BusAggregate` per bus
from generators, loads, fixed shunts, and line-end admittances. The aggregate drives
several `buses` columns:

| `buses` column | Aggregation rule |
|---|---|
| `p_sched` | Σ(in-service generator PG) − Σ(in-service load PL), all / SBASE |
| `q_sched` | Σ(in-service generator QG) − Σ(in-service load QL), all / SBASE |
| `g_shunt` | Bus GL/SBASE + Σ(in-service fixed-shunt GL/SBASE) + Σ(in-service branch GI at from-bus) + Σ(in-service branch GJ at to-bus) |
| `b_shunt` | Bus BL/SBASE + Σ(in-service fixed-shunt BL/SBASE) + Σ(in-service branch BI at from-bus) + Σ(in-service branch BJ at to-bus) |
| `q_min` | min(QB) over in-service generators at bus; −9999 pu for PQ load buses |
| `q_max` | max(QT) over in-service generators at bus; 9999 pu for PQ load buses |
| `p_min_agg` | Σ(in-service generator PB / SBASE) |
| `p_max_agg` | Σ(in-service generator PT / SBASE); 9999 pu for PQ load buses |
| `v_mag_set` | Generator VS if 0.85 ≤ VS ≤ 1.15 pu; otherwise Bus VM |
| `v_ang_set` | Bus VA converted to radians |

> **Design note**: line-end admittances GI/BI/GJ/BJ are folded into the bus shunt
> aggregation rather than stored on the branch, because the solver expects all shunt
> injections in the bus admittance matrix.

---

## Section 0 — Case identification → `metadata` table

| PSS/E field | Rust `CaseId` field | RPF `metadata` column | Notes |
|---|---|---|---|
| SBASE | `sbase` | `base_mva` | System MVA base; default 100 MVA if absent. |
| REV | `rev` | `psse_version` | RAW file revision integer (e.g. 33, 35). |
| BASFRQ | `basfrq` | `frequency_hz` | Nominal system frequency (Hz). |
| `/` comment | `title` | `study_name` | Free-form title on line 1 of the RAW file. |
| — | — | `raptrix_version` | Always `"0.8.6"` written by this converter. |
| — | — | `is_planning_case` | Always `true` for PSS/E RAW imports. |
| — | — | `case_mode` | Always `"flat_start_planning"`. |
| — | — | `timestamp_utc` | UTC wall-clock time of conversion. |

---

## Section 1 — Bus data → `buses` table

| PSS/E field | RAW col | Rust `Bus` field | RPF column | Notes |
|---|---|---|---|---|
| I | 1 | `i` | `bus_id` | Positive integer ≤ 999 997. |
| NAME | 2 | `name` | `name` | Trailing spaces stripped; dictionary-encoded. |
| BASKV | 3 | `baskv` | `nominal_kv` | Base voltage in kV (nullable Float64). |
| IDE | 4 | `ide` | `type` | Int8: 1=PQ, 2=PQ-gen, 3=PV, 4=slack. |
| AREA | 5 | `area` | `area` | Foreign key → `areas.area_id`. |
| ZONE | 6 | `zone` | `zone` | Foreign key → `zones.zone_id`. |
| OWNER | 7 | `owner` | `owner` | Foreign key → `owners.owner_id`. |
| GL | 8* | `gl` | `g_shunt` (partial) | Inline bus shunt conductance (MW @ 1 pu); folded into aggregated `g_shunt`. |
| BL | 9* | `bl` | `b_shunt` (partial) | Inline bus shunt susceptance (MVAr @ 1 pu); folded into aggregated `b_shunt`. |
| VM | — | `vm` | `v_mag_set` (fallback) | Used only when no generator VS override in range 0.85–1.15 pu. |
| VA | — | `va` | `v_ang_set` | Bus.VA × π/180 → radians. |
| NVHI | — | `nvhi` | `v_max` | Normal voltage upper limit (pu). |
| NVLO | — | `nvlo` | `v_min` | Normal voltage lower limit (pu). |
| EVHI | — | `evhi` | *(not stored)* | Emergency voltage limits have no RPF column in v0.8.6. |
| EVLO | — | `evlo` | *(not stored)* | " |

\* GL/BL appear at columns 8–9 in some legacy RAW variants; absent in standard v35 bus records
where they belong in fixed shunt section 3.

**Aggregated-only columns** (no direct PSS/E bus field — derived via `BusAggregate`):

| RPF column | Source |
|---|---|
| `p_sched` | Net scheduled active injection (see bus aggregation table above). |
| `q_sched` | Net scheduled reactive injection. |
| `g_shunt` | Combined conductance from bus GL + fixed shunts + line-end GI/GJ. |
| `b_shunt` | Combined susceptance from bus BL + fixed shunts + line-end BI/BJ. |
| `q_min` / `q_max` | Generator reactive capability range at bus. |
| `p_min_agg` / `p_max_agg` | Generator active range at bus. |
| `bus_uuid` | Synthesized as `"psse:bus:{bus_id}"` for stable cross-file identity. |

---

## Section 2 — Load data → `loads` table

| PSS/E field | Rust `Load` field | RPF column | Notes |
|---|---|---|---|
| I | `i` | `bus_id` | Foreign key → `buses.bus_id`. |
| ID | `id` | `id` | 1–2 char identifier; dictionary-encoded. |
| STATUS | `status` | `status` | Bool: 1 → true, 0 → false. |
| PL | `pl` | `p_pu` | Constant-power active load; PL / SBASE. |
| QL | `ql` | `q_pu` | Constant-power reactive load; QL / SBASE. |
| IP | `ip` | *(not stored)* | Constant-current component discarded; no RPF column in v0.8.6. |
| IQ | `iq` | *(not stored)* | " |
| YP | `yp` | *(not stored)* | Constant-admittance component discarded. |
| YQ | `yq` | *(not stored)* | " |
| AREA | `area` | *(not stored)* | |
| ZONE | `zone` | *(not stored)* | |
| OWNER | `owner` | *(not stored)* | |
| SCALE | `scale` | *(not stored)* | Wind-machine flag. |
| INTRPT | `intrpt` | *(not stored)* | Interruptible load flag. |
| — | — | `name` | Always null (PSS/E loads have no display name). |

> **ZIP load note**: RPF v0.8.6 `loads` carries only the constant-power (PQ) portion.
> IP/IQ constant-current and YP/YQ constant-admittance components are dropped.
> Future RPF versions will add explicit ZIP columns.

---

## Section 3 — Fixed shunt data → `fixed_shunts` table

| PSS/E field | Rust `FixedShunt` field | RPF column | Notes |
|---|---|---|---|
| I | `i` | `bus_id` | |
| ID | `id` | `id` | |
| STATUS | `status` | `status` | Bool. |
| GL | `gl` | `g_pu` | GL / SBASE. |
| BL | `bl` | `b_pu` | BL / SBASE. |

**Inline bus shunts**: any Bus record with `GL ≠ 0` or `BL ≠ 0` generates a
synthetic `fixed_shunts` row with `id = "1"`, in addition to being folded into
the `buses.g_shunt` / `buses.b_shunt` aggregate. This ensures downstream readers
that rebuild shunt injections from `fixed_shunts` alone get the correct totals.

---

## Section 4 — Generator data → `generators` table

| PSS/E field | Rust `Generator` field | RPF column | Notes |
|---|---|---|---|
| I | `i` | `bus_id` | |
| ID | `id` | `id` | Dictionary-encoded. |
| PG | `pg` | `p_sched_pu` | PG / SBASE. |
| PT | `pt` | `p_max_pu` | PT / SBASE. |
| PB | `pb` | `p_min_pu` | PB / SBASE. |
| QT | `qt` | `q_max_pu` | QT / SBASE. |
| QB | `qb` | `q_min_pu` | QB / SBASE. |
| STAT | `stat` | `status` | Bool. |
| MBASE | `mbase` | `mbase_mva` | Machine MVA base in MVA (not normalised). |
| ZX | `zx` | `xd_prime` | Used as Xd′ fallback when no DYR data is provided. |
| VS | `vs` | *(bus aggregation only)* | VS drives `buses.v_mag_set` if 0.85 ≤ VS ≤ 1.15 pu; not stored in `generators`. |
| QG | `qg` | *(bus aggregation only)* | QG contributes to `buses.q_sched`; not stored in `generators`. |
| IREG | `ireg` | *(not stored)* | Remote regulated bus number. |
| ZR | `zr` | *(not stored)* | Positive-sequence resistance (machine base pu). |
| RT | `rt` | *(not stored)* | Step-up transformer resistance. |
| XT | `xt` | *(not stored)* | Step-up transformer reactance. |
| GTAP | `gtap` | *(not stored)* | Step-up transformer off-nominal turns ratio. |
| RMPCT | `rmpct` | *(not stored)* | Fraction of MVAR range for remote voltage control. |
| O1 | `o1` | *(not stored)* | Owner number. |
| WMOD | `wmod` | *(not stored)* | Wind machine flag. |
| WPF | `wpf` | *(not stored)* | Power factor for WMOD modes 2 and 3. |
| — | — | `h` | Inertia constant from DYR; 0.0 if no DYR provided. |
| — | — | `xd_prime` | From DYR; falls back to generator ZX if no DYR. |
| — | — | `D` | Damping coefficient from DYR; 0.0 if no DYR. |
| — | — | `name` | Always null (PSS/E generators have no display name in RAW). |

---

## Section 5 — Branch data → `branches` table

| PSS/E field | Rust `Branch` field | RPF column | Notes |
|---|---|---|---|
| I | `i` | `from_bus_id` | |
| J | `j` | `to_bus_id` | |
| CKT | `ckt` | `ckt` | Dictionary-encoded. |
| R | `r` | `r` | Per-unit on system base. |
| X | `x` | `x` | Per-unit on system base. |
| B | `b` | `b_shunt` | Total line charging susceptance (pu system base). |
| RATEA | `ratea` | `rate_a` | RATEA / SBASE (per-unit). |
| RATEB | `rateb` | `rate_b` | RATEB / SBASE. |
| RATEC | `ratec` | `rate_c` | RATEC / SBASE. |
| ST | `st` | `status` | Bool. |
| GI | `gi` | *(bus agg only)* | From-end shunt conductance folded into `buses.g_shunt`. |
| BI | `bi` | *(bus agg only)* | From-end shunt susceptance folded into `buses.b_shunt`. |
| GJ | `gj` | *(bus agg only)* | To-end shunt conductance folded into `buses.g_shunt`. |
| BJ | `bj` | *(bus agg only)* | To-end shunt susceptance folded into `buses.b_shunt`. |
| MET | `met` | *(not stored)* | Metered end flag. |
| LEN | `len` | *(not stored)* | Line length (user units). |
| O1 | `o1` | *(not stored)* | Owner number. |
| — | — | `tap` | Always 1.0 (PSS/E lines carry no tap). |
| — | — | `phase` | Always 0.0 (no phase shift on line branches). |
| — | — | `branch_id` | 1-based row index synthesized at export time. |
| — | — | `name` | Always null. |
| — | — | `from_nominal_kv` | Looked up from `buses.nominal_kv` at export time. |
| — | — | `to_nominal_kv` | Looked up from `buses.nominal_kv` at export time. |

**v0.8.6 FACTS extension columns** (all null for non-FACTS branches from PSS/E RAW):

| RPF column | Type | Notes |
|---|---|---|
| `device_type` | Dict<Int32,Utf8> | null — no FACTS in PSS/E RAW section 5. |
| `control_mode` | Dict<Int32,Utf8> | null |
| `control_target_flow_mw` | Float64 | null |
| `x_min_pu` | Float64 | null |
| `x_max_pu` | Float64 | null |
| `injected_voltage_mag_pu` | Float64 | null |
| `injected_voltage_angle_deg` | Float64 | null |
| `facts_params` | Map<Utf8,Float64> | null |

> FACTS devices (PSS/E section 18, STATCOM/SSSC/UPFC) are not yet parsed.
> When support is added they will populate these columns; existing readers
> that treat the columns as nullable will continue to work without change.

---

## Section 6 — Transformer data → `transformers_2w` table

PSS/E two-winding transformer records span **four lines** in the RAW file.
Three-winding transformers (K ≠ 0) are expanded at parse time into **three
2-winding legs** with a synthetic star bus; all legs land in `transformers_2w`.

| PSS/E field | RAW line | Rust `TwoWindingTransformer` field | RPF column | Notes |
|---|---|---|---|---|
| I | 1 | `i` | `from_bus_id` | Winding 1 bus. |
| J | 1 | `j` | `to_bus_id` | Winding 2 bus. |
| CKT | 1 | `ckt` | `ckt` | |
| STAT | 1 | `stat` | `status` | Bool. |
| MAG1 | 1 | `mag1` | `g` | Magnetising conductance (pu system base). |
| MAG2 | 1 | `mag2` | `b` | Magnetising susceptance (pu system base). |
| R1-2 | 2 | `r12` | `r` | Series resistance (pu on SBASE1-2 base). |
| X1-2 | 2 | `x12` | `x` | Series reactance. |
| SBASE1-2 | 2 | `sbase12` | *(not stored)* | Winding MVA base; used during parse only. |
| WINDV1 | 3 | `windv1` | `tap_ratio` | Off-nominal turns ratio, winding 1. |
| NOMV1 | 3 | `nomv1` | `from_nominal_kv` | Rated kV; null if NOMV1 = 0. |
| ANG1 | 3 | `ang1` | `phase_shift` | ANG1 × π/180 → radians. |
| RATA1 | 3 | `rata1` | `rate_a` | RATA1 / SBASE (pu). |
| RATB1 | 3 | `ratb1` | `rate_b` | RATB1 / SBASE. |
| RATC1 | 3 | `ratc1` | `rate_c` | RATC1 / SBASE. |
| WINDV2 | 4 | `windv2` | *(not stored)* | Used only during 3W star expansion. |
| NOMV2 | 4 | `nomv2` | `to_nominal_kv` | Rated kV; null if NOMV2 = 0. |
| — | — | — | `nominal_tap_ratio` | Always 1.0 (TODO: derive from NOMV1/NOMV2). |
| — | — | — | `vector_group` | Always `"Yy0"` (TODO: derive from CW/CZ). |
| — | — | — | `winding1_r` / `winding1_x` | Always 0.0 (TODO: per-winding impedance decomposition). |
| — | — | — | `winding2_r` / `winding2_x` | Always 0.0. |
| — | — | — | `name` | Always null. |

**3-winding expansion**: PSS/E 3W transformers produce a synthetic star bus
(numbered above 999 997) and three `transformers_2w` rows (H→star, M→star,
L→star legs). The `transformers_3w` table in RPF is populated by the CIM
importer, not the PSS/E importer.

---

## Section 7 — Area interchange data → `areas` table

| PSS/E field | Rust `Area` field | RPF column | Notes |
|---|---|---|---|
| I | `i` | `area_id` | Integer area number. |
| ARNAM | `arnam` | `name` | Up to 12 characters. |
| PDES | `pdes` | `interchange_mw` | Desired net interchange in MW (Float64). |
| ISW | `isw` | *(not stored)* | Swing bus for the area; no RPF column in v0.8.6. |
| PTOL | `ptol` | *(not stored)* | Interchange tolerance bandwidth in MW. |

---

## Section 13 — Zone data → `zones` table

| PSS/E field | Rust `Zone` field | RPF column | Notes |
|---|---|---|---|
| I | `i` | `zone_id` | Integer zone number. |
| ZONAM | `zonam` | `name` | Up to 12 characters. |

---

## Section 15 — Owner data → `owners` table

| PSS/E field | Rust `Owner` field | RPF column | Notes |
|---|---|---|---|
| I | `i` | `owner_id` | Integer owner number. |
| OWNAM | `ownam` | `name` | Up to 12 characters. |

---

## Section 17 — Switched shunt data → `switched_shunts` table

| PSS/E field | Rust `SwitchedShunt` field | RPF column | Notes |
|---|---|---|---|
| I | `i` | `bus_id` | |
| STAT | `stat` | `status` | Bool. |
| VSWHI | `vswhi` | `v_high` | Voltage upper control limit (pu). |
| VSWLO | `vswlo` | `v_low` | Voltage lower control limit (pu). |
| BINIT | `binit` | `b_init_pu` | BINIT / SBASE. Authoritative initial susceptance. |
| N1–N8 / B1–B8 | `steps` (expanded) | `b_steps` | List<Float64>: each Nk copies of Bk/SBASE. |
| — | — | `current_step` | Estimated from BINIT: closest cumulative step sum index. |
| — | — | `shunt_id` | Synthesized: `"{bus_id}_shunt_{n}"` (1-indexed per bus). |
| MODSW | `modsw` | *(not stored)* | Control mode (0=locked, 1=discrete, 2=continuous). |
| ADJM | `adjm` | *(not stored)* | Adjustment method. |
| SWREM | `swrem` | *(not stored)* | Remotely regulated bus number. |
| RMPCT | `rmpct` | *(not stored)* | Remote reactive fraction. |
| RMIDNT | `rmidnt` | *(not stored)* | Remote bus name. |

---

## DYR dynamic models → `dynamics_models` table

The optional `.dyr` file is parsed for **GENROU**, **GENSAL**, **GENCLS**
(and their enhanced variants GENROE / GENSAE) records only. Exciter, governor,
and PSS model records are silently ignored.

| DYR record field | Rust `DyrGeneratorData` field | RPF column | Notes |
|---|---|---|---|
| Bus number | `bus_id` | `bus_id` | |
| Machine ID | `id` | `gen_id` | Matches `generators.id`. |
| Model name | `model` | `model_type` | e.g. `"GENROU"`, `"GENSAL"`, `"GENCLS"`. |
| H | `h` | `params["H"]` | Inertia constant (MW·s/MVA). |
| D | `d` | `params["D"]` | Damping coefficient (pu torque / pu speed deviation). |
| Xd′ | `xd_prime` | `params["xd_prime"]` | d-axis transient reactance (machine base pu). |

**Interaction with `generators` table**: when DYR data is present the generator
row's `h`, `xd_prime`, and `D` columns are populated from the DYR record.
When absent, `h = 0.0`, `D = 0.0`, and `xd_prime = ZX` are written as fallbacks.

---

## Sections not yet implemented

| PSS/E section | RPF table | Status |
|---|---|---|
| Section 8 — Two-terminal DC | — | Skipped at parse time (records read but not converted). |
| Section 9 — VSC DC | — | Skipped. |
| Section 10 — Impedance correction | — | Skipped. |
| Section 11 — Multi-terminal DC | — | Skipped. |
| Section 12 — Multi-section line | — | Skipped. |
| Section 14 — Inter-area transfer | — | Skipped. |
| Section 18 — FACTS devices | `branches` FACTS columns | Skipped (PSS/E v35). FACTS extension columns in `branches` will be populated when support is added. |
| Section 19 — GNE devices | — | Skipped. |
| Section 20 — Induction machines | — | Skipped. |
| v35 System Switching Devices | — | State-machine advances past them; records not converted. |

---

## RPF table inventory (from PSS/E import)

| RPF table | Rows populated from | Always present |
|---|---|---|
| `metadata` | CaseId (section 0) + converter metadata | ✓ |
| `buses` | Section 1 (Bus) | ✓ |
| `branches` | Section 5 (Branch) | ✓ |
| `generators` | Section 4 (Generator) + optional DYR | ✓ |
| `loads` | Section 2 (Load) | ✓ |
| `fixed_shunts` | Section 3 (FixedShunt) + inline Bus GL/BL | ✓ |
| `switched_shunts` | Section 17 (SwitchedShunt) | ✓ |
| `transformers_2w` | Section 6 (2W and 3W star legs) | ✓ |
| `transformers_3w` | *(not populated by PSS/E importer)* | ✓ (0 rows) |
| `areas` | Section 7 (Area) | ✓ |
| `zones` | Section 13 (Zone) | ✓ |
| `owners` | Section 15 (Owner) | ✓ |
| `contingencies` | *(not populated by PSS/E importer)* | ✓ (0 rows) |
| `interfaces` | *(not populated by PSS/E importer)* | ✓ (0 rows) |
| `dynamics_models` | Optional `.dyr` (GENROU/GENSAL/GENCLS) | ✓ (0 rows if no DYR) |
