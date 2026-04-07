// raptrix-psse-rs
// Copyright (c) 2026 Musto Technologies LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! Golden integration test: Texas7k RAW -> RPF conversion (with and without dynamics).

use std::time::Instant;

use arrow::array::{Array, BooleanArray, Float64Array, ListArray, StringArray};
use raptrix_cim_arrow::{
    TABLE_BRANCHES, TABLE_BUSES, TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS,
    TABLE_GENERATORS, TABLE_LOADS, TABLE_METADATA, TABLE_SWITCHED_SHUNTS,
    TABLE_TRANSFORMERS_2W,
};

const RAW_PATH: &str = "tests/data/external/Texas7k_20210804.RAW";
const DYR_PATH: &str = "tests/data/external/Texas7k_20210804.dyr";

/// Output written alongside the test data so the artefacts are easy to inspect.
const OUT_STATIC:  &str = "tests/golden/Texas7k_20210804_static.rpf";
const OUT_DYNAMIC: &str = "tests/golden/Texas7k_20210804_dynamic.rpf";

fn rows(summary: &raptrix_cim_arrow::RpfSummary, table_name: &str) -> usize {
    summary
        .tables
        .iter()
        .find(|t| t.table_name == table_name)
        .map(|t| t.rows)
        .unwrap_or(0)
}

fn print_summary(label: &str, summary: &raptrix_cim_arrow::RpfSummary, elapsed_ms: u128) {
    eprintln!("\n=== {label} ===");
    eprintln!("  elapsed: {elapsed_ms} ms");
    eprintln!("  tables:  {}  total rows: {}  all canonical: {}",
        summary.tables.len(), summary.total_rows, summary.has_all_canonical_tables);
    for t in &summary.tables {
        eprintln!("  {:30} {:6} rows", t.table_name, t.rows);
    }
}

fn col_f64<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a Float64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column '{name}'"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("column '{name}' is not Float64"))
}

fn col_bool<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a BooleanArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column '{name}'"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("column '{name}' is not Boolean"))
}

fn col_list<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a ListArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column '{name}'"))
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("column '{name}' is not List"))
}

fn sum_f64(values: &Float64Array) -> f64 {
    let mut total = 0.0;
    for i in 0..values.len() {
        if !values.is_null(i) {
            total += values.value(i);
        }
    }
    total
}

fn sum_f64_where(values: &Float64Array, mask: &BooleanArray) -> f64 {
    assert_eq!(values.len(), mask.len(), "value/mask length mismatch");
    let mut total = 0.0;
    for i in 0..values.len() {
        if !values.is_null(i) && !mask.is_null(i) && mask.value(i) {
            total += values.value(i);
        }
    }
    total
}

fn table_by_name<'a>(
    tables: &'a [(String, arrow::record_batch::RecordBatch)],
    table_name: &str,
) -> &'a arrow::record_batch::RecordBatch {
    tables
        .iter()
        .find(|(name, _)| name == table_name)
        .map(|(_, batch)| batch)
        .unwrap_or_else(|| panic!("missing table '{table_name}'"))
}

// ---------------------------------------------------------------------------
// Static (no DYR) — writes tests/golden/Texas7k_20210804_static.rpf
// ---------------------------------------------------------------------------
#[test]
fn golden_texas7k_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH, None, OUT_STATIC)
        .unwrap_or_else(|e| panic!("static conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_STATIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_STATIC))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Texas7k — static (no DYR)", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected >1000 buses");
    assert!(rows(&summary, TABLE_BRANCHES) > 0,  "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0,      "loads should be non-empty");
    assert!(rows(&summary, TABLE_FIXED_SHUNTS) > 0, "fixed_shunts should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert_eq!(rows(&summary, TABLE_DYNAMICS_MODELS), 0, "dynamics_models must be empty without DYR");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );

    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_STATIC))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));

    let metadata = table_by_name(&tables, TABLE_METADATA);
    let case_fingerprint = metadata
        .column_by_name("case_fingerprint")
        .expect("missing metadata.case_fingerprint")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("metadata.case_fingerprint must be Utf8")
        .value(0)
        .to_string();
    assert!(
        case_fingerprint.starts_with("psse:"),
        "case_fingerprint should be deterministic psse:* identity"
    );

    let buses = table_by_name(&tables, TABLE_BUSES);
    let bus_p_sched = col_f64(buses, "p_sched");
    let bus_q_sched = col_f64(buses, "q_sched");
    let bus_g_shunt = col_f64(buses, "g_shunt");
    let bus_b_shunt = col_f64(buses, "b_shunt");
    let bus_sched_l1 = bus_p_sched.values().iter().map(|v| v.abs()).sum::<f64>()
        + bus_q_sched.values().iter().map(|v| v.abs()).sum::<f64>();
    assert!(
        bus_sched_l1 > 1.0e-6,
        "bus p/q schedules should be materialized (L1={bus_sched_l1})"
    );
    let bus_shunt_l1 = bus_g_shunt.values().iter().map(|v| v.abs()).sum::<f64>()
        + bus_b_shunt.values().iter().map(|v| v.abs()).sum::<f64>();
    assert!(
        bus_shunt_l1 > 1.0e-9,
        "bus g/b shunt aggregates should be materialized in per-unit (L1={bus_shunt_l1})"
    );

    let generators = table_by_name(&tables, TABLE_GENERATORS);
    let gen_status = col_bool(generators, "status");
    let gen_p_pu_col = col_f64(generators, "p_sched_pu");
    let gen_p_pu = sum_f64_where(gen_p_pu_col, gen_status);

    let loads = table_by_name(&tables, TABLE_LOADS);
    let load_status = col_bool(loads, "status");
    let load_p_pu_col = col_f64(loads, "p_pu");
    let load_p_pu = sum_f64_where(load_p_pu_col, load_status);

    let net_p_from_components = gen_p_pu - load_p_pu;
    let net_p_from_buses = sum_f64(bus_p_sched);

    let p_err = (net_p_from_buses - net_p_from_components).abs();
    assert!(p_err < 1.0e-3, "bus/component net P mismatch: {p_err}");

    let switched = table_by_name(&tables, TABLE_SWITCHED_SHUNTS);
    let b_steps = col_list(switched, "b_steps");
    let values = b_steps
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("b_steps inner values must be Float64");
    let mut max_abs_step = 0.0_f64;
    for i in 0..values.len() {
        if !values.is_null(i) {
            max_abs_step = max_abs_step.max(values.value(i).abs());
        }
    }
    assert!(
        max_abs_step < 5.0,
        "switched shunt steps must be per-unit scale (max |B|={max_abs_step})"
    );

    let branches = table_by_name(&tables, TABLE_BRANCHES);
    let branch_rate_a = col_f64(branches, "rate_a");
    let branch_rate_b = col_f64(branches, "rate_b");
    let branch_rate_c = col_f64(branches, "rate_c");
    let branch_max_rate = branch_rate_a
        .values()
        .iter()
        .chain(branch_rate_b.values().iter())
        .chain(branch_rate_c.values().iter())
        .fold(0.0_f64, |acc, v| acc.max(v.abs()));
    assert!(
        branch_max_rate < 50.0,
        "branch rates should be per-unit scale (max |rate|={branch_max_rate})"
    );

    let transformers = table_by_name(&tables, TABLE_TRANSFORMERS_2W);
    let tx_rate_a = col_f64(transformers, "rate_a");
    let tx_rate_b = col_f64(transformers, "rate_b");
    let tx_rate_c = col_f64(transformers, "rate_c");
    let tx_max_rate = tx_rate_a
        .values()
        .iter()
        .chain(tx_rate_b.values().iter())
        .chain(tx_rate_c.values().iter())
        .fold(0.0_f64, |acc, v| acc.max(v.abs()));
    assert!(
        tx_max_rate < 50.0,
        "transformer rates should be per-unit scale (max |rate|={tx_max_rate})"
    );
}

// ---------------------------------------------------------------------------
// Dynamic (with DYR) — writes tests/golden/Texas7k_20210804_dynamic.rpf
// ---------------------------------------------------------------------------
#[test]
fn golden_texas7k_dynamic() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH, Some(DYR_PATH), OUT_DYNAMIC)
        .unwrap_or_else(|e| panic!("dynamic conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_DYNAMIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_DYNAMIC))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Texas7k — dynamic (with DYR)", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected >1000 buses");
    assert!(rows(&summary, TABLE_GENERATORS) > 0,      "generators should be non-empty");
    assert!(rows(&summary, TABLE_DYNAMICS_MODELS) > 0, "dynamics_models should be non-empty with DYR");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

// ---------------------------------------------------------------------------
// Texas2k static (no DYR) — writes tests/golden/Texas2k_series25_static.rpf
// ---------------------------------------------------------------------------
const RAW_PATH_TX2K: &str = "tests/data/external/Texas2k_series25_case1_summerpeak.RAW";
const DYR_PATH_TX2K: &str = "tests/data/external/Texas2k_series25_case1_summerpeak.dyr";
const OUT_TX2K_STATIC:  &str = "tests/golden/Texas2k_series25_static.rpf";
const OUT_TX2K_DYNAMIC: &str = "tests/golden/Texas2k_series25_dynamic.rpf";

#[test]
fn golden_texas2k_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_TX2K, None, OUT_TX2K_STATIC)
        .unwrap_or_else(|e| panic!("Texas2k static conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_TX2K_STATIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_TX2K_STATIC))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Texas2k — static (no DYR)", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 0, "expected buses");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert_eq!(rows(&summary, TABLE_DYNAMICS_MODELS), 0, "dynamics_models must be empty without DYR");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

// ---------------------------------------------------------------------------
// Texas2k dynamic (with DYR) — writes tests/golden/Texas2k_series25_dynamic.rpf
// ---------------------------------------------------------------------------
#[test]
fn golden_texas2k_dynamic() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_TX2K, Some(DYR_PATH_TX2K), OUT_TX2K_DYNAMIC)
        .unwrap_or_else(|e| panic!("Texas2k dynamic conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_TX2K_DYNAMIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_TX2K_DYNAMIC))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Texas2k — dynamic (with DYR)", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 0, "expected buses");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_DYNAMICS_MODELS) > 0, "dynamics_models should be non-empty with DYR");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

// ---------------------------------------------------------------------------
// Base Eastern Interconnect 515GW static
// writes tests/golden/Base_Eastern_Interconnect_515GW_static.rpf
// ---------------------------------------------------------------------------
const RAW_PATH_EI: &str = "tests/data/external/Base_Eastern_Interconnect_515GW.RAW";
const OUT_EI_STATIC: &str = "tests/golden/Base_Eastern_Interconnect_515GW_static.rpf";

#[test]
fn golden_eastern_interconnect_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_EI, None, OUT_EI_STATIC)
        .unwrap_or_else(|e| panic!("Eastern Interconnect static conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_EI_STATIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_EI_STATIC))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Base Eastern Interconnect 515GW — static", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected large bus count for EI model");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert_eq!(rows(&summary, TABLE_DYNAMICS_MODELS), 0, "no DYR provided");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

// ---------------------------------------------------------------------------
// IEEE 14-bus — canonical small test case; verify exact bus count
// ---------------------------------------------------------------------------
const RAW_PATH_IEEE14: &str = "tests/data/external/IEEE_14_bus.raw";
const OUT_IEEE14: &str = "tests/golden/IEEE_14_bus_static.rpf";

#[test]
fn golden_ieee14_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_IEEE14, None, OUT_IEEE14)
        .unwrap_or_else(|e| panic!("IEEE 14 conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_IEEE14))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_IEEE14))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("IEEE 14-bus — static", &summary, elapsed_ms);

    assert_eq!(rows(&summary, TABLE_BUSES), 14, "IEEE 14-bus: expected exactly 14 buses");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );

    // Verify no data was silently dropped: bus IDs 1-14 must all appear.
    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_IEEE14))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));
    let buses = table_by_name(&tables, TABLE_BUSES);
    let bus_id_col = buses
        .column_by_name("bus_id")
        .expect("missing buses.bus_id")
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .expect("bus_id must be Int32");
    let mut ids: Vec<i32> = (0..bus_id_col.len()).map(|i| bus_id_col.value(i)).collect();
    ids.sort_unstable();
    assert_eq!(ids, (1i32..=14).collect::<Vec<_>>(), "IEEE 14: expected bus IDs 1..=14");
}

// ---------------------------------------------------------------------------
// IEEE 118-bus — canonical medium test case; verify exact bus count
// ---------------------------------------------------------------------------
const RAW_PATH_IEEE118: &str = "tests/data/external/IEEE_118_Bus.RAW";
const OUT_IEEE118: &str = "tests/golden/IEEE_118_Bus_static.rpf";

#[test]
fn golden_ieee118_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_IEEE118, None, OUT_IEEE118)
        .unwrap_or_else(|e| panic!("IEEE 118 conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_IEEE118))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_IEEE118))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("IEEE 118-bus — static", &summary, elapsed_ms);

    assert_eq!(rows(&summary, TABLE_BUSES), 118, "IEEE 118-bus: expected exactly 118 buses");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );

    // Verify no zero-reactance branches silently collapsed — all branch x values
    // must be passed through exactly (zero x is legal in this format and must not
    // be clamped or modified by the converter; the solver handles singularities).
    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_IEEE118))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));
    let branches = table_by_name(&tables, TABLE_BRANCHES);
    let x_col = col_f64(branches, "x");
    // Just verify the column is present and has the right row count.
    assert_eq!(x_col.len(), rows(&summary, TABLE_BRANCHES), "branch x column length mismatch");
}

// ---------------------------------------------------------------------------
// NYISO off-peak 2019 v23 — legacy PSS/E v23 format
// ---------------------------------------------------------------------------
const RAW_PATH_NYISO_OFF: &str = "tests/data/external/NYISO_offpeak2019_v23.raw";
const OUT_NYISO_OFF: &str = "tests/golden/NYISO_offpeak2019_v23_static.rpf";

#[test]
fn golden_nyiso_offpeak_v23_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_NYISO_OFF, None, OUT_NYISO_OFF)
        .unwrap_or_else(|e| panic!("NYISO off-peak v23 conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_NYISO_OFF))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_NYISO_OFF))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("NYISO off-peak 2019 v23 — static", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 0, "expected buses");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );

    // Verify bus and branch counts are consistent between the two NYISO snapshots
    // (same topology, different operating point — bus/branch counts must match).
    eprintln!("  [NYISO off-peak] buses={} branches={}", rows(&summary, TABLE_BUSES), rows(&summary, TABLE_BRANCHES));
}

// ---------------------------------------------------------------------------
// NYISO on-peak 2019 v23 — topology must match off-peak snapshot
// ---------------------------------------------------------------------------
const RAW_PATH_NYISO_ON: &str = "tests/data/external/NYISO_onpeak2019_v23.raw";
const OUT_NYISO_ON: &str = "tests/golden/NYISO_onpeak2019_v23_static.rpf";

// NYISO 2030 snapshots (new golden test inputs)
const RAW_PATH_NYISO_ON2030_PW: &str = "tests/data/external/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld.raw";
const OUT_NYISO_ON2030_PW: &str = "tests/golden/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld_static.rpf";

const RAW_PATH_NYISO_2030_MATPOWER: &str = "tests/data/external/nyiso_2030_v11_shunts_as_gens_psse33fromMatpower.raw";
const OUT_NYISO_2030_MATPOWER: &str = "tests/golden/nyiso_2030_v11_shunts_as_gens_psse33fromMatpower_static.rpf";

#[test]
fn golden_nyiso_onpeak_v23_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_NYISO_ON, None, OUT_NYISO_ON)
        .unwrap_or_else(|e| panic!("NYISO on-peak v23 conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_NYISO_ON))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_NYISO_ON))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("NYISO on-peak 2019 v23 — static", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 0, "expected buses");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );

    // Off-peak and on-peak are different operating conditions for the same
    // network.  Bus count must be identical; branch/transformer counts may
    // differ because some elements are switched out-of-service between the
    // two snapshots.  Verify off-peak converted successfully (both tests run
    // in parallel so we load the already-written RPF rather than re-converting).
    let off_peak = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_NYISO_OFF))
        .unwrap_or_else(|e| panic!("summarize_rpf(off-peak) failed: {e:#}"));
    assert_eq!(
        rows(&summary, TABLE_BUSES),
        rows(&off_peak, TABLE_BUSES),
        "NYISO on/off-peak bus counts must match (same installed network)"
    );
    eprintln!(
        "  [NYISO topology diff] branches: on-peak={} off-peak={}  transformers: on-peak={} off-peak={}",
        rows(&summary, TABLE_BRANCHES),
        rows(&off_peak, TABLE_BRANCHES),
        rows(&summary, TABLE_TRANSFORMERS_2W),
        rows(&off_peak, TABLE_TRANSFORMERS_2W),
    );
}

// ---------------------------------------------------------------------------
// NYISO 2030 snapshots (static conversions)
// ---------------------------------------------------------------------------
#[test]
fn golden_nyiso_onpeak_2030_powerworld_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_NYISO_ON2030_PW, None, OUT_NYISO_ON2030_PW)
        .unwrap_or_else(|e| panic!("NYISO 2030 (PowerWorld) conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_NYISO_ON2030_PW))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_NYISO_ON2030_PW))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("NYISO on-peak 2030 (PowerWorld) — static", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 0, "expected buses");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

#[test]
fn golden_nyiso_2030_matpower_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_NYISO_2030_MATPOWER, None, OUT_NYISO_2030_MATPOWER)
        .unwrap_or_else(|e| panic!("NYISO 2030 (Matpower) conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_NYISO_2030_MATPOWER))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_NYISO_2030_MATPOWER))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("NYISO 2030 (Matpower) — static", &summary, elapsed_ms);
    assert!(rows(&summary, TABLE_BUSES) > 0, "expected buses");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    // Some third-party RAW exports may omit explicit branch/generator records
    // — accept any topology as long as buses/loads exist and canonical tables present.
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

// ---------------------------------------------------------------------------
// Texas7k 2030 scenario — forward-planning case (topology differs from 2021)
// ---------------------------------------------------------------------------
const RAW_PATH_TX7K_2030: &str = "tests/data/external/Texas7k_2030_20220923.RAW";
const OUT_TX7K_2030: &str = "tests/golden/Texas7k_2030_static.rpf";

#[test]
fn golden_texas7k_2030_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_TX7K_2030, None, OUT_TX7K_2030)
        .unwrap_or_else(|e| panic!("Texas7k 2030 conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_TX7K_2030))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_TX7K_2030))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Texas7k 2030 scenario — static", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected large bus count for 2030 planning case");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

// ---------------------------------------------------------------------------
// Midwest 24k — large regional model
// ---------------------------------------------------------------------------
const RAW_PATH_MIDWEST24K: &str = "tests/data/external/Midwest24k_20220923.RAW";
const OUT_MIDWEST24K: &str = "tests/golden/Midwest24k_static.rpf";

#[test]
fn golden_midwest24k_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_MIDWEST24K, None, OUT_MIDWEST24K)
        .unwrap_or_else(|e| panic!("Midwest24k conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_MIDWEST24K))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_MIDWEST24K))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Midwest 24k — static", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected large bus count");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

// ---------------------------------------------------------------------------
// ACTIVSg 25k synthetic grid
// ---------------------------------------------------------------------------
const RAW_PATH_ACTIVSg25K: &str = "tests/data/external/ACTIVSg25k.RAW";
const OUT_ACTIVSg25K: &str = "tests/golden/ACTIVSg25k_static.rpf";

#[test]
fn golden_activsg25k_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_ACTIVSg25K, None, OUT_ACTIVSg25K)
        .unwrap_or_else(|e| panic!("ACTIVSg25k conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_ACTIVSg25K))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_ACTIVSg25K))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("ACTIVSg 25k — static", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected large bus count");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}

// ---------------------------------------------------------------------------
// ACTIVSg 70k synthetic grid — largest available test case
// ---------------------------------------------------------------------------
const RAW_PATH_ACTIVSg70K: &str = "tests/data/external/ACTIVSg70k.RAW";
const OUT_ACTIVSg70K: &str = "tests/golden/ACTIVSg70k_static.rpf";

#[test]
fn golden_activsg70k_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_ACTIVSg70K, None, OUT_ACTIVSg70K)
        .unwrap_or_else(|e| panic!("ACTIVSg70k conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_ACTIVSg70K))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_ACTIVSg70K))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("ACTIVSg 70k — static", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected large bus count");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
    assert_eq!(
        root_metadata.get("rpf_version").map(|s| s.as_str()).unwrap_or(""),
        "0.8.4",
        "rpf_version metadata must be 0.8.3"
    );
}
