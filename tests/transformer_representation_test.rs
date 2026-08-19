// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::array::{Array, BooleanArray, DictionaryArray, Float64Array, Int32Array, StringArray};
use arrow::datatypes::Int32Type;
use raptrix_cim_arrow::{
    TABLE_BUSES, TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, TAP_CONTROL_COLUMNS,
};

const METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE: &str = "rpf.transformer_representation_mode";

fn unique_temp_path(stem: &str, ext: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be after epoch")
        .as_nanos();
    path.push(format!("raptrix_psse_rs_{stem}_{nanos}.{ext}"));
    path
}

fn write_synthetic_raw(path: &std::path::Path) {
    // Includes one native 2W transformer plus one 3W transformer so mode
    // normalization can be validated deterministically.
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / SYNTHETIC_3W
SYNTHETIC CASE
SYNTHETIC CASE
1,'BUS1',230.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
2,'BUS2',115.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
3,'BUS3',13.8,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
40,'BUS40',230.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
50,'BUS50',230.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
40,50,0,'R1',1,1,1,0.0,0.0,1,'',1
0.01,0.10,100.0
1.0,230.0,0.0,100.0,110.0,120.0
1.0,230.0
1,2,3,'T3',1,1,1,0.0,0.0,1,'',1
0.01,0.10,100.0,0.02,0.20,100.0,0.03,0.30,100.0
1.0,230.0,0.0,100.0,110.0,120.0
1.0,115.0,0.0,90.0,100.0,110.0
1.0,13.8,0.0,80.0,90.0,100.0
0 / END OF TRANSFORMER DATA, BEGIN AREA INTERCHANGE DATA
0 / END OF AREA INTERCHANGE DATA, BEGIN TWO-TERMINAL DC DATA
"#;

    fs::write(path, raw).expect("failed to write synthetic RAW file");
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

fn assert_no_synthetic_star_buses_in_buses_table(
    tables: &[(String, arrow::record_batch::RecordBatch)],
) {
    let buses = table_by_name(tables, TABLE_BUSES);
    let bus_ids = buses
        .column_by_name("bus_id")
        .expect("missing buses.bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("buses.bus_id must be Int32");

    for row in 0..bus_ids.len() {
        assert!(
            bus_ids.value(row) <= 10_000_000,
            "synthetic star bus IDs must not be emitted in buses table"
        );
    }
}

fn assert_no_dual_materialization(tables: &[(String, arrow::record_batch::RecordBatch)]) {
    let tx3w = table_by_name(tables, TABLE_TRANSFORMERS_3W);
    let tx2w = table_by_name(tables, TABLE_TRANSFORMERS_2W);

    let star_bus = tx3w
        .column_by_name("star_bus_id")
        .expect("missing transformers_3w.star_bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("transformers_3w.star_bus_id must be Int32");
    let tx3_status = tx3w
        .column_by_name("status")
        .expect("missing transformers_3w.status")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("transformers_3w.status must be Boolean");

    let from_2w = tx2w
        .column_by_name("from_bus_id")
        .expect("missing transformers_2w.from_bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("transformers_2w.from_bus_id must be Int32");
    let to_2w = tx2w
        .column_by_name("to_bus_id")
        .expect("missing transformers_2w.to_bus_id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("transformers_2w.to_bus_id must be Int32");
    let tx2_status = tx2w
        .column_by_name("status")
        .expect("missing transformers_2w.status")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("transformers_2w.status must be Boolean");

    for row3 in 0..tx3w.num_rows() {
        if !tx3_status.value(row3) {
            continue;
        }
        let star = star_bus.value(row3);
        let overlap_count = (0..tx2w.num_rows())
            .filter(|row2| tx2_status.value(*row2))
            .filter(|row2| from_2w.value(*row2) == star || to_2w.value(*row2) == star)
            .count();
        assert_eq!(
            overlap_count,
            0,
            "active transformers_3w row {} overlaps active transformers_2w star-leg rows",
            row3 + 1
        );
    }
}

fn assert_non_null_positive_f64_column(
    batch: &arrow::record_batch::RecordBatch,
    column: &str,
    context: &str,
) {
    let values = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing {context}.{column}"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("{context}.{column} must be Float64"));
    for row in 0..values.len() {
        assert!(
            !values.is_null(row) && values.value(row) > 0.0,
            "schema v0.9.3 requires non-null positive {context}.{column} (row {})",
            row + 1
        );
    }
}

#[test]
fn expanded_only_output_contains_no_native_3w_rows() {
    let raw_path = unique_temp_path("expanded_case", "raw");
    let out_path = unique_temp_path("expanded_case", "rpf");
    write_synthetic_raw(&raw_path);

    let opts = raptrix_psse_rs::ExportOptions {
        transformer_representation_mode: raptrix_psse_rs::TransformerRepresentationMode::Expanded,
        ..Default::default()
    };
    raptrix_psse_rs::write_psse_to_rpf_with_options(
        raw_path.to_str().expect("raw path must be utf-8"),
        None,
        out_path.to_str().expect("out path must be utf-8"),
        &opts,
    )
    .expect("expanded mode conversion must succeed");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path)
        .expect("read_rpf_tables must succeed for expanded output");
    let tx3w = table_by_name(&tables, TABLE_TRANSFORMERS_3W);
    let tx2w = table_by_name(&tables, TABLE_TRANSFORMERS_2W);
    assert_eq!(
        tx3w.num_rows(),
        0,
        "expanded mode must omit transformers_3w"
    );
    assert!(
        tx2w.num_rows() >= 4,
        "expanded mode must retain native 2W plus 3W star-leg expansions"
    );
    assert_non_null_positive_f64_column(tx2w, "from_nominal_kv", "transformers_2w");
    assert_non_null_positive_f64_column(tx2w, "to_nominal_kv", "transformers_2w");

    let mrid = tx2w
        .column_by_name("mrid")
        .expect("transformers_2w.mrid")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("transformers_2w.mrid must be Utf8");
    let mut star_leg_mrids: Vec<&str> = Vec::new();
    for row in 0..mrid.len() {
        if !mrid.is_null(row) {
            let value = mrid.value(row);
            if value.ends_with("_H") || value.ends_with("_M") || value.ends_with("_L") {
                star_leg_mrids.push(value);
            }
        }
    }
    assert!(
        star_leg_mrids
            .iter()
            .any(|v| v.ends_with("_H") && v.contains("XF3_1_2_3_T3")),
        "expanded star legs must use parent XF3 mrid with _H/_M/_L suffixes, got {star_leg_mrids:?}"
    );

    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(&out_path)
        .expect("rpf_file_metadata must succeed for expanded output");
    assert_eq!(
        root_metadata
            .get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
            .map(|s| s.as_str())
            .unwrap_or(""),
        "expanded"
    );
    assert_no_dual_materialization(&tables);
    assert_no_synthetic_star_buses_in_buses_table(&tables);

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

#[test]
fn native_3w_only_output_contains_no_star_legs() {
    let raw_path = unique_temp_path("native_case", "raw");
    let out_path = unique_temp_path("native_case", "rpf");
    write_synthetic_raw(&raw_path);

    let opts = raptrix_psse_rs::ExportOptions {
        transformer_representation_mode: raptrix_psse_rs::TransformerRepresentationMode::Native3W,
        ..Default::default()
    };
    raptrix_psse_rs::write_psse_to_rpf_with_options(
        raw_path.to_str().expect("raw path must be utf-8"),
        None,
        out_path.to_str().expect("out path must be utf-8"),
        &opts,
    )
    .expect("native_3w mode conversion must succeed");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path)
        .expect("read_rpf_tables must succeed for native output");
    let tx3w = table_by_name(&tables, TABLE_TRANSFORMERS_3W);
    let tx2w = table_by_name(&tables, TABLE_TRANSFORMERS_2W);
    assert_eq!(tx3w.num_rows(), 1, "native mode must keep one 3W row");
    assert_eq!(
        tx2w.num_rows(),
        1,
        "native mode must keep only real 2W rows and drop 3W star-leg expansions"
    );
    assert_non_null_positive_f64_column(tx2w, "from_nominal_kv", "transformers_2w");
    assert_non_null_positive_f64_column(tx2w, "to_nominal_kv", "transformers_2w");
    assert_non_null_positive_f64_column(tx3w, "nominal_kv_h", "transformers_3w");
    assert_non_null_positive_f64_column(tx3w, "nominal_kv_m", "transformers_3w");
    assert_non_null_positive_f64_column(tx3w, "nominal_kv_l", "transformers_3w");

    let tx3w_mrid = tx3w
        .column_by_name("mrid")
        .expect("transformers_3w.mrid")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("transformers_3w.mrid must be Utf8");
    assert_eq!(tx3w_mrid.len(), 1);
    assert!(!tx3w_mrid.is_null(0));
    assert_eq!(tx3w_mrid.value(0), "XF3_1_2_3_T3");

    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(&out_path)
        .expect("rpf_file_metadata must succeed for native output");
    assert_eq!(
        root_metadata
            .get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
            .map(|s| s.as_str())
            .unwrap_or(""),
        "native_3w"
    );
    assert_no_dual_materialization(&tables);
    assert_no_synthetic_star_buses_in_buses_table(&tables);

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

#[test]
fn mixed_input_normalizes_to_single_mode_default_native_3w() {
    let raw_path = unique_temp_path("default_case", "raw");
    let out_path = unique_temp_path("default_case", "rpf");
    write_synthetic_raw(&raw_path);

    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().expect("raw path must be utf-8"),
        None,
        out_path.to_str().expect("out path must be utf-8"),
    )
    .expect("default conversion must succeed");

    let root_metadata = raptrix_cim_arrow::rpf_file_metadata(&out_path)
        .expect("rpf_file_metadata must succeed for default output");
    assert_eq!(
        root_metadata
            .get(METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE)
            .map(|s| s.as_str())
            .unwrap_or(""),
        "native_3w"
    );

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path)
        .expect("read_rpf_tables must succeed for default output");
    assert_no_dual_materialization(&tables);
    assert_no_synthetic_star_buses_in_buses_table(&tables);

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

/// Winding-1 record 3: WINDV1,NOMV1,ANG1,RATA1,RATB1,RATC1,COD1,CONT1,RMA1,RMI1,VMA1,VMI1,NTP1
fn write_two_winding_raw(path: &std::path::Path, winding3: &str) {
    let raw = format!(
        r#"0, 100.0, 33, 0, 0, 60.0 / TAP_CONTROL
TAP CONTROL
TAP CONTROL
40,'BUS40',230.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
50,'BUS50',230.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
40,50,0,'R1',1,1,1,0.0,0.0,1,'',1
0.01,0.10,100.0
{winding3}
1.0,230.0
0 / END OF TRANSFORMER DATA, BEGIN AREA INTERCHANGE DATA
0 / END OF AREA INTERCHANGE DATA, BEGIN TWO-TERMINAL DC DATA
"#
    );
    fs::write(path, raw).expect("write tap-control RAW");
}

fn dict_utf8_at(col: &dyn Array, i: usize) -> &str {
    let dict = col
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("tap_limit_unit must be Dictionary<Int32, Utf8>");
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("dictionary values must be Utf8");
    values.value(dict.key(i).expect("dictionary key"))
}

#[test]
fn tap_control_cod_zero_writes_all_eight_null() {
    let raw_path = unique_temp_path("tap_cod0", "raw");
    let out_path = unique_temp_path("tap_cod0", "rpf");
    write_two_winding_raw(
        &raw_path,
        "1.0,230.0,0.0,100.0,110.0,120.0,0,0,1.1,0.9,1.1,0.9,33",
    );

    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("COD=0 conversion");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("read");
    let xfmr = table_by_name(&tables, TABLE_TRANSFORMERS_2W);
    assert_eq!(xfmr.num_rows(), 1);
    for name in TAP_CONTROL_COLUMNS {
        let col = xfmr
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(col.null_count(), 1, "{name} must be null when COD=0");
    }
    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

#[test]
fn tap_control_cod3_stamps_degrees_as_writer_default() {
    let raw_path = unique_temp_path("tap_cod3", "raw");
    let out_path = unique_temp_path("tap_cod3", "rpf");
    write_two_winding_raw(
        &raw_path,
        "1.0,230.0,0.0,100.0,110.0,120.0,3,0,30.0,-30.0,0.0,0.0,61",
    );

    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("COD=3 conversion");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("read");
    let xfmr = table_by_name(&tables, TABLE_TRANSFORMERS_2W);
    let mode = xfmr
        .column_by_name("tap_control_mode")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(mode.value(0), 3);
    let unit = xfmr.column_by_name("tap_limit_unit").unwrap();
    assert_eq!(dict_utf8_at(unit.as_ref(), 0), "degrees");
    let step = xfmr
        .column_by_name("tap_step")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((step.value(0) - 1.0).abs() < 1e-12);
    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

#[test]
fn tap_control_voltage_cod_stamps_ratio() {
    let raw_path = unique_temp_path("tap_cod1", "raw");
    let out_path = unique_temp_path("tap_cod1", "rpf");
    write_two_winding_raw(
        &raw_path,
        "1.0,230.0,0.0,100.0,110.0,120.0,1,40,1.1,0.9,1.1,0.9,33",
    );

    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("COD=1 conversion");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("read");
    let xfmr = table_by_name(&tables, TABLE_TRANSFORMERS_2W);
    let unit = xfmr.column_by_name("tap_limit_unit").unwrap();
    assert_eq!(dict_utf8_at(unit.as_ref(), 0), "ratio");
    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}
