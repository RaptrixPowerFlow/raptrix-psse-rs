// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! Smoke tests for the **locked RPF interchange contract** (current `raptrix-cim-arrow` /
//! schema-contract expectations): generator hierarchy / IBR / ownership, nullable extended
//! metadata on typical PSS/E exports, `scenario_context` write guard, and `case_mode` override.
//! Names here are intentionally **not** tied to a single schema patch version.

use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, StringArray};
use raptrix_cim_arrow::{
    METADATA_KEY_CASE_MODE, TABLE_BRANCHES, TABLE_BUSES, TABLE_GENERATORS, TABLE_METADATA,
    TABLE_OWNERS,
};

fn unique_temp_path(stem: &str, ext: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be after epoch")
        .as_nanos();
    path.push(format!("raptrix_psse_rs_{stem}_{nanos}.{ext}"));
    path
}

#[test]
fn generators_hierarchy_ownership_and_metadata_smoke() {
    let raw_path = unique_temp_path("rpf_contract_smoke", "raw");
    let dyr_path = unique_temp_path("rpf_contract_smoke", "dyr");
    let out_path = unique_temp_path("rpf_contract_smoke", "rpf");

    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / RPF_CONTRACT_SMOKE
CONTRACT SMOKE
CONTRACT SMOKE
1,'BUS1',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'BUS2',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
2,'1',1,1,1,40.0,15.0,0,0,0,0,1,1,0
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
1,'1',75.0,10.0,40.0,-20.0,1.02,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0,1,1,1.0
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
0 / END OF TRANSFORMER DATA, BEGIN AREA INTERCHANGE DATA
1,1,0.0,10.0,'AREA1'
0 / END OF AREA INTERCHANGE DATA, BEGIN TWO-TERMINAL DC DATA
0 / END OF TWO-TERMINAL DC DATA, BEGIN VSC DC LINE DATA
0 / END OF VSC DC LINE DATA, BEGIN IMPEDANCE CORRECTION DATA
0 / END OF IMPEDANCE CORRECTION DATA, BEGIN MULTI-TERMINAL DC DATA
0 / END OF MULTI-TERMINAL DC DATA, BEGIN MULTI-SECTION LINE DATA
0 / END OF MULTI-SECTION LINE DATA, BEGIN ZONE DATA
1,'ZONE1'
0 / END OF ZONE DATA, BEGIN INTER-AREA TRANSFER DATA
0 / END OF INTER-AREA TRANSFER DATA, BEGIN OWNER DATA
1,'OWNER1'
0 / END OF OWNER DATA, BEGIN FACTS DEVICE DATA
0 / END OF FACTS DEVICE DATA, BEGIN SWITCHED SHUNT DATA
0 / END OF SWITCHED SHUNT DATA, BEGIN GNE DEVICE DATA
0 / END OF GNE DEVICE DATA, BEGIN INDUCTION MACHINE DATA
0 / END OF INDUCTION MACHINE DATA
"#;
    fs::write(&raw_path, raw).expect("failed to write smoke RAW");

    let dyr = "1 'REGCA' 1 1.0 /\n";
    fs::write(&dyr_path, dyr).expect("failed to write smoke DYR");

    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().expect("raw path must be utf-8"),
        Some(dyr_path.to_str().expect("dyr path must be utf-8")),
        out_path.to_str().expect("out path must be utf-8"),
    )
    .expect("conversion should succeed");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("failed to read RPF");

    let generators = tables
        .iter()
        .find(|(name, _)| name == TABLE_GENERATORS)
        .map(|(_, batch)| batch)
        .expect("missing generators table");

    let hierarchy = generators
        .column_by_name("hierarchy_level")
        .expect("missing generators.hierarchy_level")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("generators.hierarchy_level must be Utf8");
    assert_eq!(hierarchy.value(0), "unit");

    let is_ibr = generators
        .column_by_name("is_ibr")
        .expect("missing generators.is_ibr")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("generators.is_ibr must be Boolean");
    assert!(is_ibr.value(0));

    let ibr_subtype = generators
        .column_by_name("ibr_subtype")
        .expect("missing generators.ibr_subtype")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("generators.ibr_subtype must be Utf8");
    assert_eq!(ibr_subtype.value(0), "solar");

    let generator_owner = generators
        .column_by_name("owner_id")
        .expect("missing generators.owner_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("generators.owner_id must be Int32");
    assert_eq!(generator_owner.value(0), 1);

    let buses = tables
        .iter()
        .find(|(name, _)| name == TABLE_BUSES)
        .map(|(_, batch)| batch)
        .expect("missing buses table");
    let bus_owner = buses
        .column_by_name("owner_id")
        .expect("missing buses.owner_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("buses.owner_id must be Int32");
    assert_eq!(bus_owner.value(0), 1);

    let branches = tables
        .iter()
        .find(|(name, _)| name == TABLE_BRANCHES)
        .map(|(_, batch)| batch)
        .expect("missing branches table");
    let branch_owner = branches
        .column_by_name("owner_id")
        .expect("missing branches.owner_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("branches.owner_id must be Int32");
    assert_eq!(branch_owner.value(0), 1);

    let owners = tables
        .iter()
        .find(|(name, _)| name == TABLE_OWNERS)
        .map(|(_, batch)| batch)
        .expect("missing owners table");
    owners
        .column_by_name("short_name")
        .expect("missing owners.short_name");
    owners.column_by_name("type").expect("missing owners.type");
    owners
        .column_by_name("params")
        .expect("missing owners.params");

    let metadata = tables
        .iter()
        .find(|(name, _)| name == TABLE_METADATA)
        .map(|(_, batch)| batch)
        .expect("missing metadata table");
    let band = metadata
        .column_by_name("hour_ahead_uncertainty_band")
        .expect("missing metadata.hour_ahead_uncertainty_band")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("hour_ahead_uncertainty_band must be Float64");
    assert!(
        band.is_null(0),
        "legacy PSS/E export keeps extended metadata columns null"
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(dyr_path);
    let _ = fs::remove_file(out_path);
}

#[test]
fn scenario_context_rows_rejected_when_unsupported() {
    let raw_path = unique_temp_path("sc_ctx", "raw");
    let out_path = unique_temp_path("sc_ctx", "rpf");

    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / SC_CTX
SC
SC
1,'B1',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
0 / END OF TRANSFORMER DATA, BEGIN AREA INTERCHANGE DATA
0 / END OF AREA INTERCHANGE DATA, BEGIN TWO-TERMINAL DC DATA
0 / END OF TWO-TERMINAL DC DATA, BEGIN VSC DC LINE DATA
0 / END OF VSC DC LINE DATA, BEGIN IMPEDANCE CORRECTION DATA
0 / END OF IMPEDANCE CORRECTION DATA, BEGIN MULTI-TERMINAL DC DATA
0 / END OF MULTI-TERMINAL DC DATA, BEGIN MULTI-SECTION LINE DATA
0 / END OF MULTI-SECTION LINE DATA, BEGIN ZONE DATA
0 / END OF ZONE DATA, BEGIN INTER-AREA TRANSFER DATA
0 / END OF INTER-AREA TRANSFER DATA, BEGIN OWNER DATA
0 / END OF OWNER DATA, BEGIN FACTS DEVICE DATA
0 / END OF FACTS DEVICE DATA, BEGIN SWITCHED SHUNT DATA
0 / END OF SWITCHED SHUNT DATA, BEGIN GNE DEVICE DATA
0 / END OF GNE DEVICE DATA, BEGIN INDUCTION MACHINE DATA
0 / END OF INDUCTION MACHINE DATA
"#;
    fs::write(&raw_path, raw).expect("write raw");

    let row = raptrix_psse_rs::ScenarioContextRow {
        scenario_context_id: 1,
        case_id: "x".into(),
        source_type: "real_time".into(),
        priority: "low".into(),
        violation_type: None,
        nerc_recovery_status: None,
        recovery_time_min: None,
        cleared_by_reserves: None,
        planning_feedback_flag: false,
        planning_assumption_violated: None,
        recommended_action: None,
        investigation_summary: None,
        load_forecast_error_pct: None,
        created_timestamp_utc: "2026-01-01T00:00:00Z".into(),
        params: vec![],
    };
    let opts = raptrix_psse_rs::ExportOptions {
        scenario_context_rows: vec![row],
        ..Default::default()
    };
    let err = raptrix_psse_rs::write_psse_to_rpf_with_options(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
        &opts,
    )
    .expect_err("non-empty scenario_context_rows must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("scenario_context"),
        "expected scenario_context error, got: {msg}"
    );

    let _ = fs::remove_file(raw_path);
}

#[test]
fn case_mode_override_round_trip_smoke() {
    let raw_path = unique_temp_path("case_mode", "raw");
    let out_path = unique_temp_path("case_mode", "rpf");

    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / CMODE
CM
CM
1,'B1',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
0 / END OF TRANSFORMER DATA, BEGIN AREA INTERCHANGE DATA
0 / END OF AREA INTERCHANGE DATA, BEGIN TWO-TERMINAL DC DATA
0 / END OF TWO-TERMINAL DC DATA, BEGIN VSC DC LINE DATA
0 / END OF VSC DC LINE DATA, BEGIN IMPEDANCE CORRECTION DATA
0 / END OF IMPEDANCE CORRECTION DATA, BEGIN MULTI-TERMINAL DC DATA
0 / END OF MULTI-TERMINAL DC DATA, BEGIN MULTI-SECTION LINE DATA
0 / END OF MULTI-SECTION LINE DATA, BEGIN ZONE DATA
0 / END OF ZONE DATA, BEGIN INTER-AREA TRANSFER DATA
0 / END OF INTER-AREA TRANSFER DATA, BEGIN OWNER DATA
0 / END OF OWNER DATA, BEGIN FACTS DEVICE DATA
0 / END OF FACTS DEVICE DATA, BEGIN SWITCHED SHUNT DATA
0 / END OF SWITCHED SHUNT DATA, BEGIN GNE DEVICE DATA
0 / END OF GNE DEVICE DATA, BEGIN INDUCTION MACHINE DATA
0 / END OF INDUCTION MACHINE DATA
"#;
    fs::write(&raw_path, raw).expect("write raw");

    let opts = raptrix_psse_rs::ExportOptions {
        case_mode_override: Some("hour_ahead_advisory".into()),
        ..Default::default()
    };
    raptrix_psse_rs::write_psse_to_rpf_with_options(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
        &opts,
    )
    .expect("conversion with case_mode override");

    let meta = raptrix_cim_arrow::rpf_file_metadata(&out_path).expect("rpf metadata");
    assert_eq!(
        meta.get(METADATA_KEY_CASE_MODE).map(|s| s.as_str()),
        Some("hour_ahead_advisory")
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}
