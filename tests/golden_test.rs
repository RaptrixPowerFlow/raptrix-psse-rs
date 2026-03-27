// Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
// Copyright (c) 2026 Musto Technologies LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! Golden integration test: Texas7k RAW -> RPF conversion (with and without dynamics).

use std::time::Instant;

use raptrix_cim_arrow::{
    TABLE_BRANCHES, TABLE_BUSES, TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS,
    TABLE_GENERATORS, TABLE_LOADS, TABLE_TRANSFORMERS_2W,
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

    print_summary("Texas7k — static (no DYR)", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected >1000 buses");
    assert!(rows(&summary, TABLE_BRANCHES) > 0,  "branches should be non-empty");
    assert!(rows(&summary, TABLE_GENERATORS) > 0, "generators should be non-empty");
    assert!(rows(&summary, TABLE_LOADS) > 0,      "loads should be non-empty");
    assert!(rows(&summary, TABLE_FIXED_SHUNTS) > 0, "fixed_shunts should be non-empty");
    assert!(rows(&summary, TABLE_TRANSFORMERS_2W) > 0, "transformers_2w should be non-empty");
    assert_eq!(rows(&summary, TABLE_DYNAMICS_MODELS), 0, "dynamics_models must be empty without DYR");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
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

    print_summary("Texas7k — dynamic (with DYR)", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 1000, "expected >1000 buses");
    assert!(rows(&summary, TABLE_GENERATORS) > 0,      "generators should be non-empty");
    assert!(rows(&summary, TABLE_DYNAMICS_MODELS) > 0, "dynamics_models should be non-empty with DYR");
    assert!(summary.has_all_canonical_tables, "RPF must contain all canonical tables");
}
