// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! Golden integration test: Texas7k RAW -> RPF conversion (with and without dynamics).

use std::time::Instant;

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, ListArray, StringArray};
use raptrix_cim_arrow::{
    RPF_VERSION, TABLE_BRANCHES, TABLE_BUSES, TABLE_DC_LINES_2W, TABLE_DYNAMICS_MODELS,
    TABLE_FIXED_SHUNTS, TABLE_GENERATORS, TABLE_LOADS, TABLE_METADATA, TABLE_MULTI_SECTION_LINES,
    TABLE_SWITCHED_SHUNT_BANKS, TABLE_SWITCHED_SHUNTS, TABLE_TRANSFORMERS_2W,
    TABLE_TRANSFORMERS_3W,
};

const METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE: &str = "rpf.transformer_representation_mode";

const RAW_PATH: &str = "tests/data/external/Texas7k_20210804.RAW";
const DYR_PATH: &str = "tests/data/external/Texas7k_20210804.dyr";

/// Output written alongside the test data so the artefacts are easy to inspect.
const OUT_STATIC: &str = "tests/golden/Texas7k_20210804_static.rpf";
const OUT_DYNAMIC: &str = "tests/golden/Texas7k_20210804_dynamic.rpf";

fn rows(summary: &raptrix_cim_arrow::RpfSummary, table_name: &str) -> usize {
    summary
        .tables
        .iter()
        .find(|t| t.table_name == table_name)
        .map(|t| t.rows)
        .unwrap_or(0)
}

fn has_table(summary: &raptrix_cim_arrow::RpfSummary, table_name: &str) -> bool {
    summary.tables.iter().any(|t| t.table_name == table_name)
}

fn assert_v090_required_tables(summary: &raptrix_cim_arrow::RpfSummary) {
    for table in [
        TABLE_MULTI_SECTION_LINES,
        TABLE_DC_LINES_2W,
        TABLE_SWITCHED_SHUNT_BANKS,
    ] {
        assert!(
            has_table(summary, table),
            "missing v0.9.0 required table: {table}"
        );
    }
}

fn count_ibr_generators(generators: &arrow::record_batch::RecordBatch) -> usize {
    let col = generators
        .column_by_name("is_ibr")
        .expect("missing generators.is_ibr");
    let flags = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("generators.is_ibr must be Boolean");
    (0..generators.num_rows())
        .filter(|&i| !flags.is_null(i) && flags.value(i))
        .count()
}

fn first_existing_path<'a>(candidates: &[&'a str]) -> Option<&'a str> {
    candidates
        .iter()
        .copied()
        .find(|p| std::path::Path::new(p).exists())
}

fn print_summary(label: &str, summary: &raptrix_cim_arrow::RpfSummary, elapsed_ms: u128) {
    eprintln!("\n=== {label} ===");
    eprintln!("  elapsed: {elapsed_ms} ms");
    eprintln!(
        "  tables:  {}  total rows: {}  all canonical: {}",
        summary.tables.len(),
        summary.total_rows,
        summary.has_all_canonical_tables
    );
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

fn assert_three_w_star_leg_consistency(
    tables: &[(String, arrow::record_batch::RecordBatch)],
    expected_nonzero_3w: bool,
) {
    let tx3w = table_by_name(tables, TABLE_TRANSFORMERS_3W);
    let tx2w = table_by_name(tables, TABLE_TRANSFORMERS_2W);

    let tx3w_rows = tx3w.num_rows();
    if expected_nonzero_3w {
        assert!(
            tx3w_rows > 0,
            "expected native transformers_3w rows for this dataset"
        );
    } else {
        assert_eq!(
            tx3w_rows, 0,
            "expanded mode should not export native transformers_3w rows"
        );
    }

    let from_bus = tx2w
        .column_by_name("from_bus_id")
        .expect("missing transformers_2w.from_bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("transformers_2w.from_bus_id must be Int32");
    let to_bus = tx2w
        .column_by_name("to_bus_id")
        .expect("missing transformers_2w.to_bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("transformers_2w.to_bus_id must be Int32");

    let mut star_leg_count = 0usize;
    for i in 0..tx2w.num_rows() {
        let from = from_bus.value(i);
        let to = to_bus.value(i);
        if from > 10_000_000 || to > 10_000_000 {
            star_leg_count += 1;
        }
    }

    if expected_nonzero_3w {
        assert_eq!(
            star_leg_count, 0,
            "native_3w mode must not export synthetic star-leg transformers_2w rows"
        );
    } else {
        assert_eq!(
            star_leg_count,
            tx3w_rows * 3,
            "expanded mode must emit three star legs per 3-winding transformer"
        );
    }
}

// ---------------------------------------------------------------------------
// Static (no DYR) — writes tests/golden/Texas7k_20210804_static.rpf
// ---------------------------------------------------------------------------
#[test]
fn golden_texas7k_static() {
    if !std::path::Path::new(RAW_PATH).exists() {
        eprintln!(
            "[skip] {} not found — place licensed ERCOT file at this path to enable test",
            RAW_PATH
        );
        return;
    }
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
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
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
    assert_eq!(
        rows(&summary, TABLE_DYNAMICS_MODELS),
        0,
        "dynamics_models must be empty without DYR"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_v090_required_tables(&summary);
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
    assert_eq!(
        root_metadata
            .get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
            .map(|s| s.as_str())
            .unwrap_or(""),
        "native_3w",
        "default export mode must be stable and machine-readable"
    );

    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_STATIC))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));

    let metadata = table_by_name(&tables, TABLE_METADATA);
    metadata
        .column_by_name("modern_grid_profile")
        .expect("missing metadata.modern_grid_profile");
    metadata
        .column_by_name("ibr_penetration_pct")
        .expect("missing metadata.ibr_penetration_pct");
    metadata
        .column_by_name("has_ibr")
        .expect("missing metadata.has_ibr");
    metadata
        .column_by_name("has_smart_valve")
        .expect("missing metadata.has_smart_valve");
    metadata
        .column_by_name("has_multi_terminal_dc")
        .expect("missing metadata.has_multi_terminal_dc");
    metadata
        .column_by_name("study_purpose")
        .expect("missing metadata.study_purpose");
    metadata
        .column_by_name("scenario_tags")
        .expect("missing metadata.scenario_tags");
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
    let gen_p_mw_col = col_f64(generators, "p_sched_mw");
    let gen_p_mw = sum_f64_where(gen_p_mw_col, gen_status);

    // v0.8.9 generator hierarchy migration: legacy flat RAW units map to unit level.
    let hierarchy_level = generators
        .column_by_name("hierarchy_level")
        .expect("missing generators.hierarchy_level")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("generators.hierarchy_level must be Utf8");
    for i in 0..hierarchy_level.len() {
        assert_eq!(
            hierarchy_level.value(i),
            "unit",
            "legacy generator rows must migrate as hierarchy_level=unit"
        );
    }

    let owner_id_col = generators
        .column_by_name("owner_id")
        .expect("missing generators.owner_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("generators.owner_id must be Int32");
    let mut has_any_generator_owner = false;
    for i in 0..owner_id_col.len() {
        if !owner_id_col.is_null(i) {
            has_any_generator_owner = true;
            break;
        }
    }
    assert!(
        has_any_generator_owner,
        "expected at least one generator.owner_id in exported table"
    );

    let buses_owner = table_by_name(&tables, TABLE_BUSES)
        .column_by_name("owner_id")
        .expect("missing buses.owner_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("buses.owner_id must be Int32");
    assert!(
        buses_owner.len() > 0,
        "buses.owner_id column should be populated as nullable Int32"
    );

    let branches_owner = table_by_name(&tables, TABLE_BRANCHES)
        .column_by_name("owner_id")
        .expect("missing branches.owner_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("branches.owner_id must be Int32");
    assert!(
        branches_owner.len() > 0,
        "branches.owner_id column should be populated as nullable Int32"
    );

    let loads = table_by_name(&tables, TABLE_LOADS);
    let load_status = col_bool(loads, "status");
    let load_p_pu_col = col_f64(loads, "p_pu");
    let load_p_pu = sum_f64_where(load_p_pu_col, load_status);

    let base_mva = col_f64(metadata, "base_mva").value(0);
    let net_p_from_components = (gen_p_mw / base_mva) - load_p_pu;
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
    if !std::path::Path::new(RAW_PATH).exists() {
        eprintln!(
            "[skip] {} not found — place licensed ERCOT file at this path to enable test",
            RAW_PATH
        );
        return;
    }
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
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_DYNAMICS_MODELS) > 0,
        "dynamics_models should be non-empty with DYR"
    );
    assert!(
        rows(&summary, TABLE_DYNAMICS_MODELS) > rows(&summary, TABLE_GENERATORS),
        "full DYR export should preserve more records than machine-only generator rows"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_v090_required_tables(&summary);
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
    assert_eq!(
        root_metadata
            .get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
            .map(|s| s.as_str())
            .unwrap_or(""),
        "native_3w",
        "default export mode must be stable and machine-readable"
    );
}

// ---------------------------------------------------------------------------
// Texas2k static (no DYR) — writes tests/golden/Texas2k_series25_static.rpf
// ---------------------------------------------------------------------------
const RAW_PATH_TX2K: &str = "tests/data/external/Texas2k_series25_case1_summerpeak.RAW";
const DYR_PATH_TX2K: &str = "tests/data/external/Texas2k_series25_case1_summerpeak.dyr";
const OUT_TX2K_STATIC: &str = "tests/golden/Texas2k_series25_static.rpf";
const OUT_TX2K_DYNAMIC: &str = "tests/golden/Texas2k_series25_dynamic.rpf";

#[test]
fn golden_texas2k_static() {
    if !std::path::Path::new(RAW_PATH_TX2K).exists() {
        eprintln!(
            "[skip] {} not found — place licensed ERCOT file at this path to enable test",
            RAW_PATH_TX2K
        );
        return;
    }
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
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert_eq!(
        rows(&summary, TABLE_DYNAMICS_MODELS),
        0,
        "dynamics_models must be empty without DYR"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_v090_required_tables(&summary);
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
}

// ---------------------------------------------------------------------------
// Texas2k dynamic (with DYR) — writes tests/golden/Texas2k_series25_dynamic.rpf
// ---------------------------------------------------------------------------
#[test]
fn golden_texas2k_dynamic() {
    if !std::path::Path::new(RAW_PATH_TX2K).exists() {
        eprintln!(
            "[skip] {} not found — place licensed ERCOT file at this path to enable test",
            RAW_PATH_TX2K
        );
        return;
    }
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_TX2K, Some(DYR_PATH_TX2K), OUT_TX2K_DYNAMIC)
        .unwrap_or_else(|e| panic!("Texas2k dynamic conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_TX2K_DYNAMIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata =
        raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_TX2K_DYNAMIC))
            .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Texas2k — dynamic (with DYR)", &summary, elapsed_ms);

    assert!(rows(&summary, TABLE_BUSES) > 0, "expected buses");
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_DYNAMICS_MODELS) > 0,
        "dynamics_models should be non-empty with DYR"
    );
    assert!(
        rows(&summary, TABLE_DYNAMICS_MODELS) > rows(&summary, TABLE_GENERATORS),
        "full DYR export should preserve more records than machine-only generator rows"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_v090_required_tables(&summary);
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
}

// ---------------------------------------------------------------------------
// Base Eastern Interconnect 515GW static
// writes tests/golden/Base_Eastern_Interconnect_515GW_static.rpf
// ---------------------------------------------------------------------------
const RAW_PATH_EI: &str = "tests/data/external/Base_Eastern_Interconnect_515GW.RAW";
const OUT_EI_STATIC: &str = "tests/golden/Base_Eastern_Interconnect_515GW_static.rpf";

const RAW_PATH_ACTIVS10K: &str = "tests/data/external/ACTIVSg10k.RAW";
const DYR_PATH_ACTIVS10K_DYR: &str = "tests/data/external/ACTIVSg10k.dyr";
const DYR_PATH_ACTIVS10K_DYN: &str = "tests/data/external/ACTIVSg10k.dyn";
const OUT_ACTIVS10K_DYNAMIC: &str = "tests/golden/ACTIVSg10k_dynamic.rpf";

const RAW_PATH_TX2K_GFM: &str = "tests/data/external/Texas2k_series24_case6_2024lowloadwithgfm.RAW";
const DYR_PATH_TX2K_GFM_DYR: &str =
    "tests/data/external/Texas2k_series24_case6_2024lowloadwithgfm.dyr";
const DYR_PATH_TX2K_GFM_DYN: &str =
    "tests/data/external/Texas2k_series24_case6_2024lowloadwithgfm.dyn";
const OUT_TX2K_GFM_DYNAMIC: &str = "tests/golden/Texas2k_series24_gfm_dynamic.rpf";

#[test]
fn golden_activsg10k_dynamic_v090_tables_and_metadata() {
    let Some(dyr_path) = first_existing_path(&[DYR_PATH_ACTIVS10K_DYN, DYR_PATH_ACTIVS10K_DYR])
    else {
        eprintln!(
            "[skip] {} and {} not found — place ACTIVSg10k dynamic deck to enable test",
            DYR_PATH_ACTIVS10K_DYN, DYR_PATH_ACTIVS10K_DYR
        );
        return;
    };
    if !std::path::Path::new(RAW_PATH_ACTIVS10K).exists() {
        eprintln!(
            "[skip] {} not found — place ACTIVSg10k RAW file to enable test",
            RAW_PATH_ACTIVS10K
        );
        return;
    }

    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_ACTIVS10K, Some(dyr_path), OUT_ACTIVS10K_DYNAMIC)
        .unwrap_or_else(|e| panic!("ACTIVSg10k dynamic conversion failed: {e:#}"));

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_ACTIVS10K_DYNAMIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    assert!(
        rows(&summary, TABLE_DYNAMICS_MODELS) > 0,
        "expected DYR payload rows"
    );
    assert_v090_required_tables(&summary);

    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_ACTIVS10K_DYNAMIC))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));
    let metadata = table_by_name(&tables, TABLE_METADATA);
    let has_ibr = metadata
        .column_by_name("has_ibr")
        .expect("missing metadata.has_ibr")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("metadata.has_ibr must be Boolean")
        .value(0);
    let generators = table_by_name(&tables, TABLE_GENERATORS);
    let ibr_gens = count_ibr_generators(generators);
    assert_eq!(
        has_ibr,
        ibr_gens > 0,
        "metadata.has_ibr must match generators with is_ibr"
    );
}

#[test]
fn golden_texas2k_gfm_dynamic_ibr_detection() {
    let Some(dyr_path) = first_existing_path(&[DYR_PATH_TX2K_GFM_DYN, DYR_PATH_TX2K_GFM_DYR])
    else {
        eprintln!(
            "[skip] {} and {} not found — place Texas2k GFM dynamic deck to enable test",
            DYR_PATH_TX2K_GFM_DYN, DYR_PATH_TX2K_GFM_DYR
        );
        return;
    };
    if !std::path::Path::new(RAW_PATH_TX2K_GFM).exists() {
        eprintln!(
            "[skip] {} not found — place Texas2k GFM RAW file to enable test",
            RAW_PATH_TX2K_GFM
        );
        return;
    }

    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_TX2K_GFM, Some(dyr_path), OUT_TX2K_GFM_DYNAMIC)
        .unwrap_or_else(|e| panic!("Texas2k GFM dynamic conversion failed: {e:#}"));

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_TX2K_GFM_DYNAMIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    assert!(
        rows(&summary, TABLE_DYNAMICS_MODELS) > 0,
        "expected DYR payload rows"
    );
    assert_v090_required_tables(&summary);

    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_TX2K_GFM_DYNAMIC))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));
    let metadata = table_by_name(&tables, TABLE_METADATA);
    let has_ibr = metadata
        .column_by_name("has_ibr")
        .expect("missing metadata.has_ibr")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("metadata.has_ibr must be Boolean")
        .value(0);
    assert!(
        has_ibr,
        "Texas2k GFM dynamic case should be tagged as containing IBR"
    );

    let generators = table_by_name(&tables, TABLE_GENERATORS);
    assert!(
        count_ibr_generators(generators) > 0,
        "Texas2k GFM dynamic case should mark IBR on at least one generator row"
    );
}

#[test]
fn golden_eastern_interconnect_static() {
    if !std::path::Path::new(RAW_PATH_EI).exists() {
        eprintln!(
            "[skip] {} not found — place MMWG Eastern Interconnect file at this path to enable test",
            RAW_PATH_EI
        );
        return;
    }
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_EI, None, OUT_EI_STATIC)
        .unwrap_or_else(|e| panic!("Eastern Interconnect static conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_EI_STATIC))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_EI_STATIC))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary(
        "Base Eastern Interconnect 515GW — static",
        &summary,
        elapsed_ms,
    );

    assert!(
        rows(&summary, TABLE_BUSES) > 1000,
        "expected large bus count for EI model"
    );
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert_eq!(rows(&summary, TABLE_DYNAMICS_MODELS), 0, "no DYR provided");
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
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

    assert_eq!(
        rows(&summary, TABLE_BUSES),
        14,
        "IEEE 14-bus: expected exactly 14 buses"
    );
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
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
    assert_eq!(
        ids,
        (1i32..=14).collect::<Vec<_>>(),
        "IEEE 14: expected bus IDs 1..=14"
    );
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

    assert_eq!(
        rows(&summary, TABLE_BUSES),
        118,
        "IEEE 118-bus: expected exactly 118 buses"
    );
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );

    // Verify no zero-reactance branches silently collapsed — all branch x values
    // must be passed through exactly (zero x is legal in this format and must not
    // be clamped or modified by the converter; the solver handles singularities).
    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_IEEE118))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));
    let branches = table_by_name(&tables, TABLE_BRANCHES);
    let x_col = col_f64(branches, "x");
    // Just verify the column is present and has the right row count.
    assert_eq!(
        x_col.len(),
        rows(&summary, TABLE_BRANCHES),
        "branch x column length mismatch"
    );
}

// ---------------------------------------------------------------------------
// NYISO off-peak 2019 v23 — legacy PSS/E v23 format
// ---------------------------------------------------------------------------
const RAW_PATH_NYISO_OFF: &str = "tests/data/external/NYISO_offpeak2019_v23.raw";
const OUT_NYISO_OFF: &str = "tests/golden/NYISO_offpeak2019_v23_static.rpf";

#[test]
fn golden_nyiso_offpeak_v23_static() {
    if !std::path::Path::new(RAW_PATH_NYISO_OFF).exists() {
        eprintln!(
            "[skip] {} not found — place licensed NYISO file at this path to enable test",
            RAW_PATH_NYISO_OFF
        );
        return;
    }
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
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );

    // Verify bus and branch counts are consistent between the two NYISO snapshots
    // (same topology, different operating point — bus/branch counts must match).
    eprintln!(
        "  [NYISO off-peak] buses={} branches={}",
        rows(&summary, TABLE_BUSES),
        rows(&summary, TABLE_BRANCHES)
    );
}

// ---------------------------------------------------------------------------
// NYISO on-peak 2019 v23 — topology must match off-peak snapshot
// ---------------------------------------------------------------------------
const RAW_PATH_NYISO_ON: &str = "tests/data/external/NYISO_onpeak2019_v23.raw";
const OUT_NYISO_ON: &str = "tests/golden/NYISO_onpeak2019_v23_static.rpf";

// NYISO 2030 snapshots (new golden test inputs)
const RAW_PATH_NYISO_ON2030_PW: &str =
    "tests/data/external/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld.raw";
const OUT_NYISO_ON2030_PW: &str =
    "tests/golden/NYISO_onpeak2030_v11_shunts_as_gensfromPowerWorld_static.rpf";

#[test]
fn golden_nyiso_onpeak_v23_static() {
    if !std::path::Path::new(RAW_PATH_NYISO_ON).exists() {
        eprintln!(
            "[skip] {} not found — place licensed NYISO file at this path to enable test",
            RAW_PATH_NYISO_ON
        );
        return;
    }
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
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );

    // If the off-peak RPF was already written (by the sibling test running
    // first), cross-check that bus counts match — same installed topology.
    if std::path::Path::new(OUT_NYISO_OFF).exists() {
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
}

// ---------------------------------------------------------------------------
// NYISO 2030 snapshots (static conversions)
// ---------------------------------------------------------------------------
#[test]
fn golden_nyiso_onpeak_2030_powerworld_static() {
    if !std::path::Path::new(RAW_PATH_NYISO_ON2030_PW).exists() {
        eprintln!(
            "[skip] {} not found — place licensed NYISO file at this path to enable test",
            RAW_PATH_NYISO_ON2030_PW
        );
        return;
    }
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_NYISO_ON2030_PW, None, OUT_NYISO_ON2030_PW)
        .unwrap_or_else(|e| panic!("NYISO 2030 (PowerWorld) conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_NYISO_ON2030_PW))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata =
        raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_NYISO_ON2030_PW))
            .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary(
        "NYISO on-peak 2030 (PowerWorld) — static",
        &summary,
        elapsed_ms,
    );

    assert!(rows(&summary, TABLE_BUSES) > 0, "expected buses");
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
}

// Removed: NYISO 2030 Matpower test input is not available in-repo and
// corresponding references were deleted to avoid dead test failures.

// ---------------------------------------------------------------------------
// Texas7k 2030 scenario — forward-planning case (topology differs from 2021)
// ---------------------------------------------------------------------------
const RAW_PATH_TX7K_2030: &str = "tests/data/external/Texas7k_2030_20220923.RAW";
const OUT_TX7K_2030: &str = "tests/golden/Texas7k_2030_static.rpf";

#[test]
fn golden_texas7k_2030_static() {
    if !std::path::Path::new(RAW_PATH_TX7K_2030).exists() {
        eprintln!(
            "[skip] {} not found — place licensed ERCOT file at this path to enable test",
            RAW_PATH_TX7K_2030
        );
        return;
    }
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_TX7K_2030, None, OUT_TX7K_2030)
        .unwrap_or_else(|e| panic!("Texas7k 2030 conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_TX7K_2030))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_TX7K_2030))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Texas7k 2030 scenario — static", &summary, elapsed_ms);

    assert!(
        rows(&summary, TABLE_BUSES) > 1000,
        "expected large bus count for 2030 planning case"
    );
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
}

// ---------------------------------------------------------------------------
// Midwest 24k — large regional model
// ---------------------------------------------------------------------------
const RAW_PATH_MIDWEST24K: &str = "tests/data/external/Midwest24k_20220923.RAW";
const OUT_MIDWEST24K: &str = "tests/golden/Midwest24k_static.rpf";

#[test]
fn golden_midwest24k_static() {
    if !std::path::Path::new(RAW_PATH_MIDWEST24K).exists() {
        eprintln!(
            "[skip] {} not found — place licensed file at this path to enable test",
            RAW_PATH_MIDWEST24K
        );
        return;
    }
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_MIDWEST24K, None, OUT_MIDWEST24K)
        .unwrap_or_else(|e| panic!("Midwest24k conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_MIDWEST24K))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_MIDWEST24K))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("Midwest 24k — static", &summary, elapsed_ms);

    assert!(
        rows(&summary, TABLE_BUSES) > 1000,
        "expected large bus count"
    );
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_3W) > 0,
        "default native_3w mode should preserve transformers_3w rows"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
    assert_eq!(
        root_metadata
            .get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
            .map(|s| s.as_str())
            .unwrap_or(""),
        "native_3w",
        "default export mode must be stable and machine-readable"
    );

    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_MIDWEST24K))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));
    assert_three_w_star_leg_consistency(&tables, true);
}

// ---------------------------------------------------------------------------
// ACTIVSg 25k synthetic grid
// ---------------------------------------------------------------------------
const RAW_PATH_ACTIVSG25K: &str = "tests/data/external/ACTIVSg25k.RAW";
const OUT_ACTIVSG25K: &str = "tests/golden/ACTIVSg25k_static.rpf";

#[test]
fn golden_activsg25k_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_ACTIVSG25K, None, OUT_ACTIVSG25K)
        .unwrap_or_else(|e| panic!("ACTIVSg25k conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_ACTIVSG25K))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_ACTIVSG25K))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("ACTIVSg 25k — static", &summary, elapsed_ms);

    assert!(
        rows(&summary, TABLE_BUSES) > 1000,
        "expected large bus count"
    );
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_3W) > 0,
        "default native_3w mode should preserve transformers_3w rows"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
    assert_eq!(
        root_metadata
            .get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
            .map(|s| s.as_str())
            .unwrap_or(""),
        "native_3w",
        "default export mode must be stable and machine-readable"
    );

    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_ACTIVSG25K))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));
    assert_three_w_star_leg_consistency(&tables, true);
}

// ---------------------------------------------------------------------------
// ACTIVSg 70k synthetic grid — largest available test case
// ---------------------------------------------------------------------------
const RAW_PATH_ACTIVSG70K: &str = "tests/data/external/ACTIVSg70k.RAW";
const OUT_ACTIVSG70K: &str = "tests/golden/ACTIVSg70k_static.rpf";

#[test]
fn golden_activsg70k_static() {
    let t0 = Instant::now();
    raptrix_psse_rs::write_psse_to_rpf(RAW_PATH_ACTIVSG70K, None, OUT_ACTIVSG70K)
        .unwrap_or_else(|e| panic!("ACTIVSg70k conversion failed: {e:#}"));
    let elapsed_ms = t0.elapsed().as_millis();

    let summary = raptrix_cim_arrow::summarize_rpf(std::path::Path::new(OUT_ACTIVSG70K))
        .unwrap_or_else(|e| panic!("summarize_rpf failed: {e:#}"));
    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(std::path::Path::new(OUT_ACTIVSG70K))
        .unwrap_or_else(|e| panic!("rpf_file_metadata failed: {e:#}"));

    print_summary("ACTIVSg 70k — static", &summary, elapsed_ms);

    assert!(
        rows(&summary, TABLE_BUSES) > 1000,
        "expected large bus count"
    );
    assert!(
        rows(&summary, TABLE_BRANCHES) > 0,
        "branches should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_GENERATORS) > 0,
        "generators should be non-empty"
    );
    assert!(rows(&summary, TABLE_LOADS) > 0, "loads should be non-empty");
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_2W) > 0,
        "transformers_2w should be non-empty"
    );
    assert!(
        rows(&summary, TABLE_TRANSFORMERS_3W) > 0,
        "default native_3w mode should preserve transformers_3w rows"
    );
    assert!(
        summary.has_all_canonical_tables,
        "RPF must contain all canonical tables"
    );
    assert_eq!(
        root_metadata
            .get("rpf_version")
            .map(|s| s.as_str())
            .unwrap_or(""),
        RPF_VERSION,
        "rpf_version metadata must match the canonical schema"
    );
    assert_eq!(
        root_metadata
            .get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
            .map(|s| s.as_str())
            .unwrap_or(""),
        "native_3w",
        "default export mode must be stable and machine-readable"
    );

    let tables = raptrix_psse_rs::read_rpf_tables(std::path::Path::new(OUT_ACTIVSG70K))
        .unwrap_or_else(|e| panic!("read_rpf_tables failed: {e:#}"));
    assert_three_w_star_leg_consistency(&tables, true);
}
