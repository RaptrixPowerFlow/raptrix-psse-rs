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
        "v0.8.2",
        "rpf_version metadata must be v0.8.2"
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
        bus_shunt_l1 < 1.0e-9,
        "buses.g_shunt/b_shunt should be zeroed for fixed_shunt rebuild path (L1={bus_shunt_l1})"
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
        "v0.8.2",
        "rpf_version metadata must be v0.8.2"
    );
}
