<!--
  raptrix-psse-rs
  Copyright (c) 2026 Musto Technologies LLC

  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
  If a copy of the MPL was not distributed with this file, You can obtain one at
  https://mozilla.org/MPL/2.0/.
-->

# PSS/E → Raptrix PowerFlow Interchange — Field Mapping

**raptrix-psse-rs**  
Copyright (c) 2026 Musto Technologies LLC

This document provides the field-by-field rules for translating PSS/E RAW
records into the Raptrix PowerFlow Interchange (`.rpf`) Arrow schema.

> **Status: placeholder** — fill in as each section is ported from C++.

---

## Section 1 — Bus data → `TopologicalNode` / `BusbarSection`

| PSS/E Field | RAW Column | Rust `Bus` field | RPF Arrow field | Notes |
|-------------|-----------|-----------------|----------------|-------|
| I           | 1         | `i`             | `bus_id`       | Integer bus number. |
| NAME        | 2         | `name`          | `name`         | Strip trailing spaces. |
| BASKV       | 3         | `baskv`         | `base_voltage_kv` | |
| IDE         | 4         | `ide`           | `bus_type`     | 1=PQ, 2=PQ-gen, 3=PV, 4=slack. |
| AREA        | 5         | `area`          | `area_id`      | |
| ZONE        | 6         | `zone`          | `zone_id`      | |
| OWNER       | 7         | `owner`         | `owner_id`     | |
| VM          | 8         | `vm`            | `vm_pu`        | Per-unit voltage magnitude. |
| VA          | 9         | `va`            | `va_deg`       | Degrees. |
| NVHI        | 10        | `nvhi`          | `nvhi_pu`      | Normal voltage upper limit. |
| NVLO        | 11        | `nvlo`          | `nvlo_pu`      | Normal voltage lower limit. |
| EVHI        | 12        | `evhi`          | `evhi_pu`      | Emergency voltage upper limit. |
| EVLO        | 13        | `evlo`          | `evlo_pu`      | Emergency voltage lower limit. |

---

## Section 2 — Load data → `EnergyConsumer`

| PSS/E Field | Rust `Load` field | RPF Arrow field | Notes |
|-------------|------------------|----------------|-------|
| I           | `i`              | `bus_id`       | Foreign key → Bus.i. |
| ID          | `id`             | `load_id`      | 1–2 char identifier. |
| STATUS      | `status`         | `in_service`   | Map: 1→true, 0→false. |
| PL          | `pl`             | `p_mw`         | Constant-power active load. |
| QL          | `ql`             | `q_mvar`       | Constant-power reactive load. |
| IP          | `ip`             | `ip_mw`        | Constant-current active load. |
| IQ          | `iq`             | `iq_mvar`      | Constant-current reactive load. |
| YP          | `yp`             | `yp_mw`        | Constant-admittance active load. |
| YQ          | `yq`             | `yq_mvar`      | Constant-admittance reactive load. |

---

## Section 4 — Generator data → `SynchronousMachine` / `GeneratingUnit`

| PSS/E Field | Rust `Generator` field | RPF Arrow field | Notes |
|-------------|----------------------|----------------|-------|
| I           | `i`                  | `bus_id`       | |
| ID          | `id`                 | `machine_id`   | |
| PG          | `pg`                 | `p_mw`         | Active power output. |
| QG          | `qg`                 | `q_mvar`       | Reactive power output. |
| QT          | `qt`                 | `q_max_mvar`   | |
| QB          | `qb`                 | `q_min_mvar`   | |
| VS          | `vs`                 | `vs_pu`        | Voltage setpoint. |
| MBASE       | `mbase`              | `mbase_mva`    | Machine MVA base. |
| STAT        | `stat`               | `in_service`   | Map: 1→true, 0→false. |
| PT          | `pt`                 | `p_max_mw`     | |
| PB          | `pb`                 | `p_min_mw`     | |

---

## Section 5 — Branch data → `ACLineSegment`

| PSS/E Field | Rust `Branch` field | RPF Arrow field | Notes |
|-------------|--------------------|-----------------|-|
| I           | `i`                | `from_bus_id`  | |
| J           | `j`                | `to_bus_id`    | |
| CKT         | `ckt`              | `circuit_id`   | |
| R           | `r`                | `r_pu`         | Per-unit on system base. |
| X           | `x`                | `x_pu`         | |
| B           | `b`                | `b_pu`         | Total line charging. |
| RATEA       | `ratea`            | `rate_a_mva`   | Normal rating. |
| RATEB       | `rateb`            | `rate_b_mva`   | Emergency rating. |
| RATEC       | `ratec`            | `rate_c_mva`   | Short-term rating. |
| ST          | `st`               | `in_service`   | Map: 1→true, 0→false. |

---

## Section 6 — Transformer data → `PowerTransformer`

> **TODO:** Expand with full 2-winding and 3-winding field lists once the
> transformer parser is ported from C++.

---

## Section 17 — Switched shunt data → `ShuntCompensator`

> **TODO:** Expand with full field list.

---

## DYR dynamic models

> **TODO:** Map GENSAL, GENROU, ESST1A, EXAC1, IEEEG1, GGOV1 parameters to
> the RPF dynamic extension schema once the DYR parser is ported from C++.
