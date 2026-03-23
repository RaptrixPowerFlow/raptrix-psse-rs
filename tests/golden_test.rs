// Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
// Copyright (c) 2026 Musto Technologies LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! Golden integration test: Texas7k RAW -> RPF conversion.

use raptrix_cim_arrow::{
    TABLE_BRANCHES, TABLE_BUSES, TABLE_FIXED_SHUNTS, TABLE_GENERATORS, TABLE_LOADS,
    TABLE_TRANSFORMERS_2W,
};

const RAW_PATH: &str = "tests/data/external/Texas7k_20210804.RAW";

fn rows(summary: &raptrix_cim_arrow::RpfSummary, table_name: &str) -> usize {
    summary
        .tables
        .iter()
        .find(|t| t.table_name == table_name)
        .map(|t| t.rows)
        .unwrap_or(0)
}

#[test]
fn golden_texas7k_convert() {
    let out = std::env::temp_dir().join("raptrix_texas7k_golden_test.rpf");
    let out_str = out.to_str().expect("temp path should be valid UTF-8");

    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH, None, out_str)
        .unwrap_or_else(|e| panic!("conversion failed: {e:#}"));

    let summary = raptrix_cim_arrow::summarize_rpf(&out)
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));

    eprintln!("\n=== RPF summary for Texas7k ===");
    for t in &summary.tables {
        eprintln!("  {:30} {:6} rows", t.table_name, t.rows);
    }

    assert!(rows(&summary, TABLE_BUSES) > 0, "buses should be non-empty");
    assert!(rows(&summary, TABLE_BRANCHES) > 0, "branches should be non-empty");
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_FIXED_SHUNTS) > 0,
        "fixed_shunts should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );

    // Texas7k sanity check (expect a large case, not a tiny parse).
    assert!(
        rows(&summary, TABLE_BUSES) > 1000,
        "expected >1000 buses in Texas7k parse"
    );
}
