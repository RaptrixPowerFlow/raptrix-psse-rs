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

use arrow::array::{
    Array, BooleanArray, DictionaryArray, Float64Array, Int32Array, MapArray, StringArray,
};
use arrow::datatypes::Int32Type;
use raptrix_cim_arrow::{
    BUS_TYPE_PQ, BUS_TYPE_PV, BUS_TYPE_SLACK, IDENTITY_MODEL_HYBRID_SOLVER_FLAT_V1,
    METADATA_KEY_CASE_MODE, METADATA_KEY_DEFAULT_SHUNT_CONTROL_MODE, METADATA_KEY_IDENTITY_MODEL,
    METADATA_KEY_MRID_SUPPORT, RPF_VERSION, RootWriteOptions, TABLE_BRANCHES, TABLE_BUSES,
    TABLE_CONTINGENCIES, TABLE_CONTINGENCY_SEQUENCES, TABLE_GENERATORS, TABLE_LOADS,
    TABLE_METADATA, TABLE_OWNERS, read_rpf_tables, rpf_file_metadata,
};

fn dict_utf8_at(col: &dyn Array, i: usize) -> &str {
    let dict = col
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("expected Dictionary<Int32, Utf8>");
    assert!(!dict.is_null(i), "dictionary entry {i} must be non-null");
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("dictionary values must be Utf8");
    values.value(dict.key(i).expect("dictionary key"))
}

const METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE: &str = "rpf.loads.zip_fidelity_presence";

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
fn buses_type_exports_canonical_codes_for_pv_and_slack() {
    let raw_path = unique_temp_path("bus_type_codes", "raw");
    let out_path = unique_temp_path("bus_type_codes", "rpf");
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / BUS_TYPE_CODES
BUS TYPE
BUS TYPE
1,'PVBUS',230.0,2,1,1,1,1.02,0.00,1.10,0.90,1.10,0.90
2,'SWBUS',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
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
    fs::write(&raw_path, raw).expect("write bus type raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("failed to read RPF");
    let buses = tables
        .iter()
        .find(|(name, _)| name == TABLE_BUSES)
        .map(|(_, batch)| batch)
        .expect("missing buses table");
    let bus_type = buses.column_by_name("type").expect("missing buses.type");
    assert_eq!(
        dict_utf8_at(bus_type.as_ref(), 0),
        BUS_TYPE_PV,
        "RAW IDE=2 must export canonical PV"
    );
    assert_eq!(
        dict_utf8_at(bus_type.as_ref(), 1),
        BUS_TYPE_SLACK,
        "RAW IDE=3 (swing) must export canonical Slack"
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
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
1,'1',75.0,10.0,40.0,-20.0,1.02,2,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0,1,1,1.0
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

    let q_sched_mvar = generators
        .column_by_name("q_sched_mvar")
        .expect("missing generators.q_sched_mvar")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("generators.q_sched_mvar must be Float64");
    assert_eq!(q_sched_mvar.value(0), 10.0);

    let controlled_bus_id = generators
        .column_by_name("controlled_bus_id")
        .expect("missing generators.controlled_bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("generators.controlled_bus_id must be Int32");
    assert!(
        !controlled_bus_id.is_null(0),
        "IREG=2 must map to non-null controlled_bus_id"
    );
    assert_eq!(
        controlled_bus_id.value(0),
        2,
        "IREG=2 must map to controlled_bus_id=2"
    );

    let root_meta = raptrix_cim_arrow::rpf_file_metadata(&out_path).expect("rpf root metadata");
    assert_eq!(
        root_meta
            .get(METADATA_KEY_IDENTITY_MODEL)
            .map(|s| s.as_str()),
        Some(IDENTITY_MODEL_HYBRID_SOLVER_FLAT_V1),
        "planning export must stamp rpf.identity.model=hybrid_solver_flat_v1"
    );
    assert_eq!(
        root_meta
            .get(METADATA_KEY_DEFAULT_SHUNT_CONTROL_MODE)
            .map(|s| s.as_str()),
        Some("planning_full"),
        "planning export must stamp rpf.default_shunt_control_mode"
    );

    let params_col = generators
        .column_by_name("params")
        .expect("missing generators.params");
    assert!(
        !params_col.is_null(0),
        "generators.params must carry PSS/E RAW machine fields"
    );
    let params_map = params_col
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("generators.params must be a Map array");
    assert!(params_map.is_valid(0));
    assert!(
        params_map.value_length(0) >= 10,
        "expected PSS/E vs/zr/zx/… keys in params map"
    );

    let buses = tables
        .iter()
        .find(|(name, _)| name == TABLE_BUSES)
        .map(|(_, batch)| batch)
        .expect("missing buses table");
    let bus_type = buses.column_by_name("type").expect("missing buses.type");
    assert_eq!(
        dict_utf8_at(bus_type.as_ref(), 0),
        BUS_TYPE_SLACK,
        "RAW IDE=3 (swing) must export canonical Slack"
    );
    assert_eq!(
        dict_utf8_at(bus_type.as_ref(), 1),
        BUS_TYPE_PQ,
        "RAW IDE=1 must export canonical buses.type=PQ"
    );
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
    let branch_from_nominal_kv = branches
        .column_by_name("from_nominal_kv")
        .expect("missing branches.from_nominal_kv")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("branches.from_nominal_kv must be Float64");
    let branch_to_nominal_kv = branches
        .column_by_name("to_nominal_kv")
        .expect("missing branches.to_nominal_kv")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("branches.to_nominal_kv must be Float64");
    assert!(
        !branch_from_nominal_kv.is_null(0) && branch_from_nominal_kv.value(0) > 0.0,
        "schema v0.9.3 requires non-null positive branches.from_nominal_kv"
    );
    assert!(
        !branch_to_nominal_kv.is_null(0) && branch_to_nominal_kv.value(0) > 0.0,
        "schema v0.9.3 requires non-null positive branches.to_nominal_kv"
    );

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

    let loads = tables
        .iter()
        .find(|(name, _)| name == TABLE_LOADS)
        .map(|(_, batch)| batch)
        .expect("missing loads table");
    let p_i = loads
        .column_by_name("p_i_pu")
        .expect("missing loads.p_i_pu")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("loads.p_i_pu must be Float64");
    let q_i = loads
        .column_by_name("q_i_pu")
        .expect("missing loads.q_i_pu")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("loads.q_i_pu must be Float64");
    let p_y = loads
        .column_by_name("p_y_pu")
        .expect("missing loads.p_y_pu")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("loads.p_y_pu must be Float64");
    let q_y = loads
        .column_by_name("q_y_pu")
        .expect("missing loads.q_y_pu")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("loads.q_y_pu must be Float64");
    assert_eq!(p_i.value(0), 0.0);
    assert_eq!(q_i.value(0), 0.0);
    assert_eq!(p_y.value(0), 0.0);
    assert_eq!(q_y.value(0), 0.0);

    let root_meta = raptrix_cim_arrow::rpf_file_metadata(&out_path).expect("rpf metadata");
    assert_eq!(
        root_meta
            .get(METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE)
            .map(String::as_str),
        Some("complete"),
        "loads ZIP fidelity should be complete when all ZIP terms are present in source rows"
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(dyr_path);
    let _ = fs::remove_file(out_path);
}

#[test]
fn loads_zip_fidelity_presence_classification_smoke() {
    let raw_not_available = unique_temp_path("zip_presence_na", "raw");
    let out_not_available = unique_temp_path("zip_presence_na", "rpf");
    let raw_partial = unique_temp_path("zip_presence_partial", "raw");
    let out_partial = unique_temp_path("zip_presence_partial", "rpf");

    let raw_not_available_text = r#"0, 100.0, 33, 0, 0, 60.0 / ZIP_PRESENCE_NA
ZIP PRESENCE
ZIP PRESENCE
1,'BUS1',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'BUS2',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
2,'1',1,1,1,40.0,15.0
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
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
    fs::write(&raw_not_available, raw_not_available_text).expect("write not_available raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_not_available.to_str().unwrap(),
        None,
        out_not_available.to_str().unwrap(),
    )
    .expect("conversion should succeed for not_available path");
    let meta_not_available = raptrix_cim_arrow::rpf_file_metadata(&out_not_available)
        .expect("metadata read for not_available");
    assert_eq!(
        meta_not_available
            .get(METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE)
            .map(String::as_str),
        Some("not_available")
    );

    let raw_partial_text = r#"0, 100.0, 33, 0, 0, 60.0 / ZIP_PRESENCE_PARTIAL
ZIP PRESENCE
ZIP PRESENCE
1,'BUS1',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'BUS2',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
3,'BUS3',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
2,'1',1,1,1,40.0,15.0
3,'1',1,1,1,20.0,8.0,1.0,2.0,3.0,4.0,1,1,0
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
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
    fs::write(&raw_partial, raw_partial_text).expect("write partial raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_partial.to_str().unwrap(),
        None,
        out_partial.to_str().unwrap(),
    )
    .expect("conversion should succeed for partial path");
    let meta_partial =
        raptrix_cim_arrow::rpf_file_metadata(&out_partial).expect("metadata read for partial");
    assert_eq!(
        meta_partial
            .get(METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE)
            .map(String::as_str),
        Some("partial")
    );

    let _ = fs::remove_file(raw_not_available);
    let _ = fs::remove_file(out_not_available);
    let _ = fs::remove_file(raw_partial);
    let _ = fs::remove_file(out_partial);
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

#[test]
fn nominal_kv_required_uses_opposite_bus_fallback_for_star_or_missing_side() {
    let raw_path = unique_temp_path("nominal_kv_required_fail", "raw");
    let out_path = unique_temp_path("nominal_kv_required_fail", "rpf");

    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / NOMINAL_KV_REQUIRED_FAIL
NOMINAL KV FAIL
NOMINAL KV FAIL
1,'B1',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'B2',0.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
1,2,0,'1',1,1,1,0.0,0.0,1,'',1
0.01,0.10,100.0
1.0,0.0,0.0,100.0,110.0,120.0
1.0,0.0
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
    fs::write(&raw_path, raw).expect("write raw");

    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().expect("raw path must be utf-8"),
        None,
        out_path.to_str().expect("out path must be utf-8"),
    )
    .expect("conversion must succeed with opposite-bus nominal-kV fallback");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("failed to read RPF");
    let tx2w = tables
        .iter()
        .find(|(name, _)| name == raptrix_cim_arrow::TABLE_TRANSFORMERS_2W)
        .map(|(_, batch)| batch)
        .expect("missing transformers_2w");
    let to_nominal_kv = tx2w
        .column_by_name("to_nominal_kv")
        .expect("missing transformers_2w.to_nominal_kv")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("transformers_2w.to_nominal_kv must be Float64");
    assert!(
        !to_nominal_kv.is_null(0) && to_nominal_kv.value(0) > 0.0,
        "required to_nominal_kv must be populated from fallback"
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

/// WMOD at the correct tail index (not F1) drives IBR classification when no DYR is present.
#[test]
fn generator_wmod_fallback_ibr_without_dyr() {
    let raw_path = unique_temp_path("wmod_ibr", "raw");
    let out_path = unique_temp_path("wmod_ibr", "rpf");
    // v33 gen: F1=1 (integer) at idx 19, WMOD=1 at idx 26 — must not mis-read F1 as WMOD.
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / WMOD_IBR
WMOD IBR
WMOD IBR
1,'BUS1',230.0,2,1,1,1,1.02,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
1,'1',75.0,10.0,40.0,-20.0,1.02,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0,1,1,0,1.0000,0,1.0000,0,1.0000,1,1.0
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
    fs::write(&raw_path, raw).expect("write wmod-ibr raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("read RPF");
    let generators = tables
        .iter()
        .find(|(name, _)| name == TABLE_GENERATORS)
        .map(|(_, batch)| batch)
        .expect("missing generators table");

    let is_ibr = generators
        .column_by_name("is_ibr")
        .expect("missing generators.is_ibr")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("generators.is_ibr must be Boolean");
    assert!(
        is_ibr.value(0),
        "WMOD=1 at tail must classify as IBR without DYR"
    );

    let ibr_subtype = generators
        .column_by_name("ibr_subtype")
        .expect("missing generators.ibr_subtype")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("generators.ibr_subtype must be Utf8");
    assert_eq!(ibr_subtype.value(0), "wind");

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

// ---------------------------------------------------------------------------
// RPF v0.9.6 quality hardening — Fix A / Fix B / Fix D2 regression coverage.
// ---------------------------------------------------------------------------

/// Fix A — invalid `bus.vm` values must be sanitized to flat-start defaults
/// on export so the raptrix-core importer's "sanitized invalid v_mag" warning
/// class is suppressed.
///
/// Bus 1 has `VM=0.0` (PSS/E uninitialized / disconnected sentinel). Bus 2 has
/// a healthy `VM=1.0`. The exporter must emit `v_mag_set=1.0` for both rows.
#[test]
fn writer_clamps_invalid_v_mag_to_flat_start_default() {
    let raw_path = unique_temp_path("vmag_sanitize", "raw");
    let out_path = unique_temp_path("vmag_sanitize", "rpf");
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / VMAG_SANITIZE
VMAG SANITIZE
VMAG SANITIZE
1,'BAD',230.0,1,1,1,1,0.00,0.00,1.10,0.90,1.10,0.90
2,'OK',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
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
    fs::write(&raw_path, raw).expect("write vmag-sanitize raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("failed to read RPF");
    let buses = tables
        .iter()
        .find(|(name, _)| name == TABLE_BUSES)
        .map(|(_, batch)| batch)
        .expect("missing buses table");
    let v_mag_set = buses
        .column_by_name("v_mag_set")
        .expect("missing buses.v_mag_set")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("buses.v_mag_set must be Float64");
    assert!(
        (v_mag_set.value(0) - 1.0).abs() < 1.0e-12,
        "VM=0 must be clamped to 1.0 pu on export, got {}",
        v_mag_set.value(0)
    );
    assert!(
        (v_mag_set.value(1) - 1.0).abs() < 1.0e-12,
        "valid VM=1.0 must be preserved, got {}",
        v_mag_set.value(1)
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

/// Fix B — when the only RAW IDE=3 (swing) bus is on a disconnected island and
/// a connected replacement candidate exists, the converter must demote the
/// orphan slack and promote the connected candidate so the .rpf carries
/// exactly one type-3 bus on a connected island. Suppresses the importer's
/// "auto-assigned slack" warning class for valid source data.
#[test]
fn writer_promotes_connected_replacement_for_disconnected_slack() {
    let raw_path = unique_temp_path("disconnected_slack", "raw");
    let out_path = unique_temp_path("disconnected_slack", "rpf");
    // Bus 1 (IDE=3 swing) is the orphan slack — no branches/transformers reference it.
    // Bus 2 (IDE=2 PV) and bus 3 (IDE=1 PQ) form a connected island via one branch.
    // Bus 2 has the only online generator, so it should win the promotion.
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / DISCONNECTED_SLACK
DISCONNECTED SLACK
DISCONNECTED SLACK
1,'ORPHAN',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'PVISLAND',230.0,2,1,1,1,1.02,0.00,1.10,0.90,1.10,0.90
3,'PQISLAND',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
3,'1',1,1,1,40.0,15.0,0,0,0,0,1,1,0
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
2,'1',75.0,10.0,40.0,-20.0,1.02,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
2,3,'1',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
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
    fs::write(&raw_path, raw).expect("write disconnected-slack raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("failed to read RPF");
    let buses = tables
        .iter()
        .find(|(name, _)| name == TABLE_BUSES)
        .map(|(_, batch)| batch)
        .expect("missing buses table");
    let bus_type = buses.column_by_name("type").expect("missing buses.type");
    assert_eq!(
        dict_utf8_at(bus_type.as_ref(), 0),
        BUS_TYPE_PV,
        "orphan IDE=3 (swing) on a disconnected island must be demoted to PV, got {}",
        dict_utf8_at(bus_type.as_ref(), 0)
    );
    assert_eq!(
        dict_utf8_at(bus_type.as_ref(), 1),
        BUS_TYPE_SLACK,
        "connected PV bus with online generator must be promoted to Slack, got {}",
        dict_utf8_at(bus_type.as_ref(), 1)
    );
    assert_eq!(
        dict_utf8_at(bus_type.as_ref(), 2),
        BUS_TYPE_PQ,
        "connected PQ bus must stay PQ, got {}",
        dict_utf8_at(bus_type.as_ref(), 2)
    );

    let slack_count = (0..bus_type.len())
        .filter(|&i| dict_utf8_at(bus_type.as_ref(), i) == BUS_TYPE_SLACK)
        .count();
    assert_eq!(slack_count, 1, "exactly one canonical Slack bus expected");

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

/// Fix D2 (revised) — even when the RAW carries a non-flat operating point,
/// the converter must keep `solved_state_presence = "not_computed"` and must
/// NOT emit a `buses_solved` seed table on the PSS/E RAW path. Rationale: the
/// importer's seed loop unconditionally overwrites `bus.v_mag_set` with the
/// seed `v_mag_pu`. For PV/Slack buses, our writer sets `v_mag_set = gen.vs`
/// (the scheduled target), but `bus.vm` (the operating value) differs by the
/// machine's reactive trim. Letting the importer overwrite the target with
/// the operating value measurably regresses convergence on Texas7k / 1.5k-bus snapshots
/// planning files. The seed is emitted again only by callers that genuinely
/// carry a separately-computed warm-start payload.
#[test]
fn writer_keeps_not_computed_for_warm_start_raw_no_seed_emission() {
    let raw_path = unique_temp_path("seed_only", "raw");
    let out_path = unique_temp_path("seed_only", "rpf");
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / SEED_ONLY
SEED ONLY
SEED ONLY
1,'B1',230.0,3,1,1,1,1.02,-3.50,1.10,0.90,1.10,0.90
2,'B2',230.0,2,1,1,1,1.04,1.20,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
1,'1',75.0,10.0,40.0,-20.0,1.02,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
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
    fs::write(&raw_path, raw).expect("write warm-start raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");

    let meta = raptrix_cim_arrow::rpf_file_metadata(&out_path).expect("rpf root metadata");
    assert_eq!(
        meta.get(METADATA_KEY_CASE_MODE).map(|s| s.as_str()),
        Some("warm_start_planning"),
        "non-flat RAW must still resolve to warm_start_planning"
    );
    assert_eq!(
        meta.get("rpf.solved_state_presence").map(|s| s.as_str()),
        Some("not_computed"),
        "PSS/E RAW path must keep solved_state_presence=not_computed",
    );

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("failed to read RPF");
    let has_buses_solved = tables
        .iter()
        .any(|(name, _)| name == raptrix_cim_arrow::TABLE_BUSES_SOLVED);
    assert!(
        !has_buses_solved,
        "PSS/E RAW path must NOT emit a buses_solved seed table"
    );

    let generators = tables
        .iter()
        .find(|(name, _)| name == TABLE_GENERATORS)
        .map(|(_, batch)| batch)
        .expect("missing generators table");
    let controlled_bus_id = generators
        .column_by_name("controlled_bus_id")
        .expect("missing generators.controlled_bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("generators.controlled_bus_id must be Int32");
    assert!(
        controlled_bus_id.is_null(0),
        "IREG=0 must export controlled_bus_id=null (local regulation), not 0"
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

/// Fix D2 negative — flat-start RAWs must keep the original
/// `solved_state_presence = "not_computed"` and must NOT emit a buses_solved
/// payload (the converter would otherwise pollute the file with empty seed data).
#[test]
fn writer_does_not_emit_seed_only_for_flat_start_raw() {
    let raw_path = unique_temp_path("flat_no_seed", "raw");
    let out_path = unique_temp_path("flat_no_seed", "rpf");
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / FLAT_NO_SEED
FLAT NO SEED
FLAT NO SEED
1,'B1',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'B2',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
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
    fs::write(&raw_path, raw).expect("write flat raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");

    let meta = raptrix_cim_arrow::rpf_file_metadata(&out_path).expect("rpf root metadata");
    assert_eq!(
        meta.get(METADATA_KEY_CASE_MODE).map(|s| s.as_str()),
        Some("flat_start_planning"),
        "flat RAW (VM=1, VA=0) must resolve to flat_start_planning"
    );
    assert_eq!(
        meta.get("rpf.solved_state_presence").map(|s| s.as_str()),
        Some("not_computed"),
        "flat-start export must keep solved_state_presence=not_computed"
    );

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("failed to read RPF");
    let has_buses_solved = tables
        .iter()
        .any(|(name, _)| name == raptrix_cim_arrow::TABLE_BUSES_SOLVED);
    assert!(
        !has_buses_solved,
        "flat-start export must not emit a buses_solved table"
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

/// Regression for the IDE=3 / IDE=4 mapping bug: previously the parser swapped
/// the two codes, which silently lost the swing-bus designation on RAW files
/// authored with IDE=3 and instead promoted IDE=4 (disconnected/isolated) buses
/// into the slack-candidate pool. After the fix:
///   * IDE=3 buses must round-trip to canonical RPF `type=3` (slack) — the
///     converter's deterministic-slack pass should not re-pick a different bus
///     when the RAW already designates a connected swing bus.
///   * IDE=4 buses must export as canonical RPF `type=1` (PQ) and never be
///     considered as slack candidates.
#[test]
fn writer_preserves_psse_ide3_swing_and_demotes_ide4_isolated() {
    let raw_path = unique_temp_path("ide3_ide4_mapping", "raw");
    let out_path = unique_temp_path("ide3_ide4_mapping", "rpf");
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / IDE3_IDE4_MAPPING
IDE3 IDE4
IDE3 IDE4
1,'SWING ',230.0,3,1,1,1,1.02,0.00,1.10,0.90,1.10,0.90
2,'PV    ',230.0,2,1,1,1,1.04,1.50,1.10,0.90,1.10,0.90
3,'PQ    ',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
4,'ISO   ',230.0,4,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
3,'1',1,1,1,40.0,15.0,0,0,0,0,1,1,0
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
1,'1',75.0,10.0,40.0,-20.0,1.02,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0
2,'1',60.0,5.0,30.0,-15.0,1.04,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,80.0,5.0
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
2,3,'1',0.02,0.06,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
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
    fs::write(&raw_path, raw).expect("write IDE3/IDE4 raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("failed to read RPF");
    let buses = tables
        .iter()
        .find(|(name, _)| name == TABLE_BUSES)
        .map(|(_, batch)| batch)
        .expect("missing buses table");
    let bus_id = buses
        .column_by_name("bus_id")
        .expect("missing buses.bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("buses.bus_id must be Int32");
    let bus_type = buses.column_by_name("type").expect("missing buses.type");

    let mut by_id: std::collections::HashMap<i32, String> = std::collections::HashMap::new();
    for i in 0..bus_id.len() {
        by_id.insert(
            bus_id.value(i),
            dict_utf8_at(bus_type.as_ref(), i).to_string(),
        );
    }
    assert_eq!(
        by_id.get(&1).map(|s| s.as_str()),
        Some(BUS_TYPE_SLACK),
        "RAW IDE=3 (swing) on a connected island must round-trip to canonical Slack"
    );
    assert_eq!(
        by_id.get(&2).map(|s| s.as_str()),
        Some(BUS_TYPE_PV),
        "RAW IDE=2 (PV) must export as canonical PV"
    );
    assert_eq!(
        by_id.get(&3).map(|s| s.as_str()),
        Some(BUS_TYPE_PQ),
        "RAW IDE=1 (PQ) must export as canonical PQ"
    );
    assert_eq!(
        by_id.get(&4).map(|s| s.as_str()),
        Some(BUS_TYPE_PQ),
        "RAW IDE=4 (disconnected/isolated) must export as canonical PQ, never Slack"
    );

    let slack_count = (0..bus_type.len())
        .filter(|&i| dict_utf8_at(bus_type.as_ref(), i) == BUS_TYPE_SLACK)
        .count();
    assert_eq!(
        slack_count, 1,
        "exactly one canonical Slack bus expected when RAW already has IDE=3"
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

#[test]
fn written_rpf_passes_validate_rpf_file_default_options() {
    let raw_path = unique_temp_path("validate_rpf_contract", "raw");
    let out_path = unique_temp_path("validate_rpf_contract", "rpf");
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / BUS_TYPE_CODES
BUS TYPE
BUS TYPE
1,'PVBUS',230.0,2,1,1,1,1.02,0.00,1.10,0.90,1.10,0.90
2,'SWBUS',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
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
    fs::write(&raw_path, raw).expect("write minimal raw");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");
    let validate_opts = RootWriteOptions {
        contingencies_are_stub: true,
        dynamics_are_stub: true,
        include_solved_state: false,
        ..Default::default()
    };
    raptrix_psse_rs::validate_rpf_file(&out_path, &validate_opts).expect(
        "validate_rpf_file must succeed with the same optional-root flags as the PSS/E writer",
    );
    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

#[test]
fn exported_equipment_tables_carry_v0122_mrid_columns() {
    let raw_path = unique_temp_path("mrid_contract", "raw");
    let out_path = unique_temp_path("mrid_contract", "rpf");
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / MRID_CONTRACT
BUS TYPE
BUS TYPE
1,'BUS1',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'BUS2',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
1,'1 ',1,1,0.0,0.0,0,0,0,0
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
1,'1 ',100.0,0.0,9999.0,-9999.0,1.0,0,100.0,0.0,1.0,0,1.0,1.0,1.0,1.0,1.0,1,1.0,1.0
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1 ',0.01,0.10,0.0,100.0,0.0,0.0,0.0,0.0,0.0,0,0,0,0,0,0,0,0,1,1,0.0,1
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
    fs::write(&raw_path, raw).expect("write minimal raw with branch and generator");

    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("conversion should succeed");

    assert_eq!(RPF_VERSION, "v0.14.0");
    let metadata = rpf_file_metadata(&out_path).expect("rpf_file_metadata");
    assert_eq!(
        metadata
            .get("rpf_version")
            .map(|v| v.as_str())
            .unwrap_or(""),
        RPF_VERSION
    );
    assert_eq!(
        metadata
            .get(METADATA_KEY_IDENTITY_MODEL)
            .map(|v| v.as_str())
            .unwrap_or(""),
        IDENTITY_MODEL_HYBRID_SOLVER_FLAT_V1
    );
    assert_eq!(
        metadata
            .get(METADATA_KEY_MRID_SUPPORT)
            .map(|v| v.as_str())
            .unwrap_or(""),
        "v1"
    );

    let tables = read_rpf_tables(&out_path).expect("read_rpf_tables");
    let (_, generators) = tables
        .iter()
        .find(|(name, _)| name == TABLE_GENERATORS)
        .expect("generators table");
    assert_eq!(generators.schema().fields().len(), 26);
    assert_eq!(generators.schema().field(25).name(), "mrid");

    let (_, branches) = tables
        .iter()
        .find(|(name, _)| name == TABLE_BRANCHES)
        .expect("branches table");
    let branch_mrid = branches
        .column_by_name("mrid")
        .expect("branches.mrid")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("branches.mrid must be Utf8");
    let gen_mrid = generators
        .column_by_name("mrid")
        .expect("generators.mrid")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("generators.mrid must be Utf8");

    assert!(
        (0..branch_mrid.len()).any(|i| !branch_mrid.is_null(i)),
        "at least one branch row must carry non-null mrid"
    );
    assert!(
        (0..gen_mrid.len()).any(|i| !gen_mrid.is_null(i)),
        "at least one generator row must carry non-null mrid"
    );
    assert_eq!(branch_mrid.value(0), "BR_1_2_1");
    assert_eq!(gen_mrid.value(0), "GEN_1_1");

    let (_, contingencies) = tables
        .iter()
        .find(|(name, _)| name == TABLE_CONTINGENCIES)
        .expect("contingencies table");
    assert_eq!(contingencies.schema().fields().len(), 10);
    assert_eq!(contingencies.schema().field(8).name(), "tpl_category");
    assert_eq!(contingencies.schema().field(9).name(), "reserved");
    assert_eq!(contingencies.num_rows(), 0);
    assert!(
        tables
            .iter()
            .all(|(name, _)| name != TABLE_CONTINGENCY_SEQUENCES),
        "PSS/E path must omit contingency_sequences"
    );

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}
