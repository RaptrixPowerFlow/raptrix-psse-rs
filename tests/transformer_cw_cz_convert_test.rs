// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! CW / CZ convert must land in RPF as system-base r/x and off-nominal tap.

use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::array::{Array, Float64Array, Int32Array};
use raptrix_cim_arrow::TABLE_TRANSFORMERS_2W;

fn unique_temp_path(stem: &str, ext: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be after epoch")
        .as_nanos();
    path.push(format!("raptrix_psse_rs_{stem}_{nanos}.{ext}"));
    path
}

fn write_two_winding_raw(path: &std::path::Path, rec1: &str, rec2: &str, rec3: &str, rec4: &str) {
    let raw = format!(
        r#"0, 100.0, 33, 0, 0, 60.0 / CW_CZ
CW CZ
CW CZ
4,'BUS4',138.0,3,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
7,'BUS7',18.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
{rec1}
{rec2}
{rec3}
{rec4}
0 / END OF TRANSFORMER DATA, BEGIN AREA INTERCHANGE DATA
0 / END OF AREA INTERCHANGE DATA, BEGIN TWO-TERMINAL DC DATA
"#
    );
    fs::write(path, raw).expect("write RAW");
}

fn convert_tx2(raw_path: &std::path::Path) -> arrow::record_batch::RecordBatch {
    let out_path = unique_temp_path("cw_cz_out", "rpf");
    raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().expect("utf-8"),
        None,
        out_path.to_str().expect("utf-8"),
    )
    .expect("convert");
    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("read rpf");
    let batch = tables
        .iter()
        .find(|(name, _)| name == TABLE_TRANSFORMERS_2W)
        .map(|(_, b)| b.clone())
        .expect("transformers_2w");
    let _ = fs::remove_file(out_path);
    batch
}

fn f64_col(batch: &arrow::record_batch::RecordBatch, name: &str) -> f64 {
    batch
        .column_by_name(name)
        .expect(name)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64")
        .value(0)
}

#[test]
fn cz1_cw1_writes_raw_x_and_windv_ratio() {
    let raw_path = unique_temp_path("cz1", "raw");
    write_two_winding_raw(
        &raw_path,
        "4,7,0,'1 ',1,1,1,0.0,0.0,1,'',1",
        "0.0,0.20912,100.0",
        "0.978,138.0,0.0,100.0,0.0,0.0",
        "1.0,18.0",
    );
    let batch = convert_tx2(&raw_path);
    assert!((f64_col(&batch, "x") - 0.20912).abs() < 1e-12);
    assert!((f64_col(&batch, "tap_ratio") - 0.978).abs() < 1e-12);
    let _ = fs::remove_file(raw_path);
}

#[test]
fn cz2_sbase50_doubles_x() {
    let raw_path = unique_temp_path("cz2", "raw");
    write_two_winding_raw(
        &raw_path,
        "4,7,0,'1 ',1,2,1,0.0,0.0,1,'',1",
        "0.0,0.20912,50.0",
        "1.0,138.0,0.0,100.0,0.0,0.0",
        "1.0,18.0",
    );
    let batch = convert_tx2(&raw_path);
    assert!((f64_col(&batch, "x") - 0.41824).abs() < 1e-12);
    let _ = fs::remove_file(raw_path);
}

#[test]
fn cw2_divides_by_baskv_not_nomv() {
    let raw_path = unique_temp_path("cw2", "raw");
    write_two_winding_raw(
        &raw_path,
        "4,7,0,'1 ',2,1,1,0.0,0.0,1,'',1",
        "0.0,0.10,100.0",
        "144.9,132.0,0.0,100.0,0.0,0.0",
        "18.0,16.5",
    );
    let batch = convert_tx2(&raw_path);
    let want = (144.9 / 138.0) / (18.0 / 18.0);
    assert!(
        (f64_col(&batch, "tap_ratio") - want).abs() < 1e-12,
        "tap={} want={want}",
        f64_col(&batch, "tap_ratio")
    );
    let _ = fs::remove_file(raw_path);
}

#[test]
fn cw9_fails_convert() {
    let raw_path = unique_temp_path("cw9", "raw");
    write_two_winding_raw(
        &raw_path,
        "4,7,0,'1 ',9,1,1,0.0,0.0,1,'',1",
        "0.0,0.10,100.0",
        "1.0,138.0,0.0,100.0,0.0,0.0",
        "1.0,18.0",
    );
    let out_path = unique_temp_path("cw9", "rpf");
    let err = raptrix_psse_rs::write_psse_to_rpf(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .unwrap_err();
    let full = format!("{err:#}");
    assert!(
        full.contains("CW=9") || full.contains("unsupported codes"),
        "error was: {full}"
    );
    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}

fn write_three_winding_cz2(path: &std::path::Path) {
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / 3W_CZ2
3W CZ2
3W CZ2
1,'H',230.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
2,'M',115.0,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
3,'L',13.8,1,1,1,1,0.0,0.0,1.00,0.0,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
1,2,3,'T3',1,2,1,0.0,0.0,1,'',1
0.01,0.10,50.0,0.02,0.20,100.0,0.03,0.30,50.0
1.0,230.0,0.0,100.0,110.0,120.0
1.0,115.0,0.0,90.0,100.0,110.0
1.0,13.8,0.0,80.0,90.0,100.0
0 / END OF TRANSFORMER DATA, BEGIN AREA INTERCHANGE DATA
0 / END OF AREA INTERCHANGE DATA, BEGIN TWO-TERMINAL DC DATA
"#;
    fs::write(path, raw).expect("write 3W RAW");
}

#[test]
fn three_winding_cz2_converts_each_pair_before_star() {
    let raw_path = unique_temp_path("3w_cz2", "raw");
    write_three_winding_cz2(&raw_path);
    let out_path = unique_temp_path("3w_cz2", "rpf");
    let opts = raptrix_psse_rs::ExportOptions {
        transformer_representation_mode: raptrix_psse_rs::TransformerRepresentationMode::Expanded,
        ..Default::default()
    };
    raptrix_psse_rs::write_psse_to_rpf_with_options(
        raw_path.to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
        &opts,
    )
    .expect("expanded 3W convert");

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("read");
    let tx2 = tables
        .iter()
        .find(|(n, _)| n == TABLE_TRANSFORMERS_2W)
        .map(|(_, b)| b)
        .expect("2w");
    let from = tx2
        .column_by_name("from_bus_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let x = tx2
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // System-base pairs: Z12=0.02+j0.20, Z23=0.02+j0.20, Z31=0.06+j0.60
    // za (H-star) = 0.5*(Z12+Z31-Z23) = 0.03+j0.30
    let mut found_h = false;
    for row in 0..tx2.num_rows() {
        if from.value(row) == 1 {
            assert!(
                (x.value(row) - 0.30).abs() < 1e-12,
                "H-star x={} want 0.30 (unconverted star would be 0.10)",
                x.value(row)
            );
            found_h = true;
        }
    }
    assert!(found_h, "missing H-star leg");
    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(out_path);
}
