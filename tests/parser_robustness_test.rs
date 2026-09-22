// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, StringArray};
use raptrix_psse_rs::models::{DcConverter, Network};
use raptrix_psse_rs::parser::parse_raw;
use std::io::Write;
use tempfile::NamedTempFile;

fn parse_snippet(raw_content: &str) -> anyhow::Result<Network> {
    let mut temp_file = NamedTempFile::new()?;
    temp_file.write_all(raw_content.as_bytes())?;
    temp_file.flush()?;
    parse_raw(temp_file.path())
}

fn raw_with_dc_rows(dc_rows: &str) -> String {
    format!(
        "0, 100.0, 33, 1, 60.0 / parser robustness\n\
         TEST CASE\n\
         TEST CASE 2\n\
         0 / END OF BUS DATA, BEGIN LOAD DATA\n\
         0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA\n\
         0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA\n\
         0 / END OF GENERATOR DATA, BEGIN BRANCH DATA\n\
         0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA\n\
         0 / END OF TRANSFORMER DATA, BEGIN AREA DATA\n\
         0 / END OF AREA DATA, BEGIN TWO-TERMINAL DC DATA\n\
         {dc_rows}\n\
         0 / END OF TWO-TERMINAL DC DATA, BEGIN ZONE DATA\n\
         0 / END OF ZONE DATA\n\
         Q\n"
    )
}

fn raw_with_msl_rows(msl_rows: &str) -> String {
    format!(
        "0, 100.0, 33, 1, 60.0 / parser robustness\n\
         TEST CASE\n\
         TEST CASE 2\n\
         0 / END OF BUS DATA, BEGIN LOAD DATA\n\
         0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA\n\
         0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA\n\
         0 / END OF GENERATOR DATA, BEGIN BRANCH DATA\n\
         0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA\n\
         0 / END OF TRANSFORMER DATA, BEGIN AREA DATA\n\
         0 / END OF AREA DATA, BEGIN TWO-TERMINAL DC DATA\n\
         0 / END OF TWO-TERMINAL DC DATA, BEGIN MULTI-SECTION LINE GROUPING DATA\n\
         {msl_rows}\n\
         0 / END OF MULTI-SECTION LINE GROUPING DATA, BEGIN ZONE DATA\n\
         0 / END OF ZONE DATA\n\
         Q\n"
    )
}

fn raw_with_dc_and_msl_rows(dc_rows: &str, msl_rows: &str) -> String {
    format!(
        "0, 100.0, 33, 1, 60.0 / parser robustness\n\
         TEST CASE\n\
         TEST CASE 2\n\
         0 / END OF BUS DATA, BEGIN LOAD DATA\n\
         0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA\n\
         0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA\n\
         0 / END OF GENERATOR DATA, BEGIN BRANCH DATA\n\
         0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA\n\
         0 / END OF TRANSFORMER DATA, BEGIN AREA DATA\n\
         0 / END OF AREA DATA, BEGIN TWO-TERMINAL DC DATA\n\
         {dc_rows}\n\
         0 / END OF TWO-TERMINAL DC DATA, BEGIN MULTI-SECTION LINE GROUPING DATA\n\
         {msl_rows}\n\
         0 / END OF MULTI-SECTION LINE GROUPING DATA, BEGIN ZONE DATA\n\
         0 / END OF ZONE DATA\n\
         Q\n"
    )
}

#[test]
fn psse_three_record_lcc_line() {
    let raw = raw_with_dc_rows(
        "'DC Line 1   ', 1, 10.0000, 550.00, 250.00, 0.00, 0.0000, 0.10000, I, 0.00, 0, 0.00000\n\
         58540, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0\n\
         55247, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0",
    );
    let network = parse_snippet(&raw).expect("Failed to parse PSS/E LCC triplet");

    assert_eq!(network.dc_lines_2w.len(), 1);
    let dc = &network.dc_lines_2w[0];
    assert_eq!(dc.from_bus_id, 58540);
    assert_eq!(dc.to_bus_id, 55247);
    assert_eq!(dc.converter_type.as_ref(), "lcc");
    assert_eq!(dc.control_mode.as_ref(), "power");
    assert_eq!(dc.name.as_deref(), Some("DC Line 1"));
    assert!((dc.r_ohm - 10.0).abs() < 1.0e-9);
    assert_eq!(dc.p_setpoint_mw, Some(550.0));
    assert_eq!(dc.v_setpoint_kv, Some(250.0));
    assert_eq!(dc.q_from_mvar, None);
    assert_eq!(dc.q_to_mvar, None);
    assert_eq!(network.dc_converters.len(), 2);
    let rect = converter_at(&network.dc_converters, 58540);
    let inv = converter_at(&network.dc_converters, 55247);
    assert_eq!(rect.role.as_ref(), "rectifier");
    assert_eq!(inv.role.as_ref(), "inverter");
    assert_eq!(rect.converter_kind.as_ref(), "lcc");
    assert_eq!(inv.converter_kind.as_ref(), "lcc");
    assert!(!rect.is_meter_end);
    assert!(inv.is_meter_end);
    for end in [rect, inv] {
        assert_eq!(end.n_bridges, Some(2));
        assert_eq!(end.ebas_kv, Some(345.0));
        assert_eq!(end.tr, Some(0.55784));
        assert_eq!(end.tap, Some(1.5));
        assert_eq!(end.tap_max, Some(1.5));
        assert_eq!(end.tap_min, Some(0.51));
        assert_eq!(end.xc_ohm, Some(10.0));
        assert_eq!(end.alpha_deg, None);
        assert_eq!(end.gamma_deg, None);
    }
}

fn converter_at(rows: &[DcConverter], bus: u32) -> &DcConverter {
    rows.iter()
        .find(|row| row.bus_id == bus)
        .unwrap_or_else(|| panic!("missing converter at bus {bus}"))
}

#[test]
fn psse_lcc_meter_r_stays_on_the_rectifier() {
    let raw = raw_with_dc_rows(
        "'DC Line 1   ', 1, 10.0000, 550.00, 250.00, 0.00, 0.0000, 0.10000, R, 0.00, 0, 0.00000\n\
         58540, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0\n\
         55247, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0",
    );
    let network = parse_snippet(&raw).expect("parse");
    assert!(converter_at(&network.dc_converters, 58540).is_meter_end);
    assert!(!converter_at(&network.dc_converters, 55247).is_meter_end);
    assert_eq!(network.dc_lines_2w[0].p_setpoint_mw, Some(550.0));
}

#[test]
fn psse_lcc_unknown_meter_leaves_both_ends_false() {
    let raw = raw_with_dc_rows(
        "'DC Line 1   ', 1, 10.0000, 550.00, 250.00, 0.00, 0.0000, 0.10000, X, 0.00, 0, 0.00000\n\
         58540, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0\n\
         55247, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0",
    );
    let network = parse_snippet(&raw).expect("parse");
    assert!(network.dc_converters.iter().all(|row| !row.is_meter_end));
    assert_eq!(network.dc_lines_2w[0].p_setpoint_mw, Some(550.0));
}

#[test]
fn psse_lcc_absent_ratio_and_zero_tap_are_not_filled() {
    let raw = raw_with_dc_rows(
        "'DC Line 1   ', 1, 10.0000, 550.00, 250.00, 0.00, 0.0000, 0.10000, I, 0.00, 0, 0.00000\n\
         58540, 2, 15, 15, 0, 10, 345, , 0, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0\n\
         55247, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0",
    );
    let network = parse_snippet(&raw).expect("parse");
    let rect = converter_at(&network.dc_converters, 58540);
    assert_eq!(rect.tr, None);
    assert_eq!(rect.tap, Some(0.0));
    assert_eq!(rect.alpha_deg, None);
    assert_eq!(rect.gamma_deg, None);
}

#[test]
fn dc_line_minimal_fields() {
    let raw = raw_with_dc_rows("10, 20, 'DC1', 'LCC'");
    let network = parse_snippet(&raw).expect("Failed to parse minimal DC line");

    assert_eq!(network.dc_lines_2w.len(), 1);
    let dc = &network.dc_lines_2w[0];
    assert_eq!(dc.from_bus_id, 10);
    assert_eq!(dc.to_bus_id, 20);
    assert_eq!(dc.ckt.as_ref(), "DC1");
    assert_eq!(dc.converter_type.as_ref(), "lcc");
    assert!(network.dc_converters.is_empty());
}

#[test]
fn dc_line_full_parameters() {
    let raw = raw_with_dc_rows("10, 20, 5.0, 0.1, 100.0, 50.0, 1.0, 'HVDC01', 'LCC'");
    let network = parse_snippet(&raw).expect("Failed to parse full DC line");

    assert_eq!(network.dc_lines_2w.len(), 1);
    let dc = &network.dc_lines_2w[0];
    assert_eq!(dc.from_bus_id, 10);
    assert_eq!(dc.to_bus_id, 20);
    assert_eq!(dc.ckt.as_ref(), "HVDC01");
    assert!((dc.r_ohm - 5.0).abs() < 1.0e-9);
    assert_eq!(dc.l_henry, Some(0.1));
    assert_eq!(dc.p_setpoint_mw, Some(100.0));
    assert_eq!(dc.i_setpoint_ka, Some(50.0));
    assert_eq!(dc.v_setpoint_kv, Some(1.0));
}

#[test]
fn vsc_dc_line_recognized() {
    let raw = "0, 100.0, 33, 1, 60.0 / parser robustness\n\
         TEST CASE\n\
         TEST CASE 2\n\
         0 / END OF BUS DATA, BEGIN LOAD DATA\n\
         0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA\n\
         0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA\n\
         0 / END OF GENERATOR DATA, BEGIN BRANCH DATA\n\
         0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA\n\
         0 / END OF TRANSFORMER DATA, BEGIN AREA DATA\n\
         0 / END OF AREA DATA, BEGIN TWO-TERMINAL DC DATA\n\
         0 / END OF TWO-TERMINAL DC DATA, BEGIN VSC DC DATA\n\
         5, 15, 'VSC_A', 'VSC', 2.0, 0.05, 150.0, 75.0, 1.2\n\
         0 / END OF VSC DC DATA, BEGIN ZONE DATA\n\
         0 / END OF ZONE DATA\n\
         Q\n"
    .to_string();

    let network = parse_snippet(&raw).expect("Failed to parse VSC DC line");

    assert_eq!(network.dc_lines_2w.len(), 1);
    let dc = &network.dc_lines_2w[0];
    assert_eq!(dc.from_bus_id, 5);
    assert_eq!(dc.to_bus_id, 15);
    assert_eq!(dc.converter_type.as_ref(), "vsc");
    assert_eq!(dc.ckt.as_ref(), "VSC_A");
    assert!(
        network.dc_converters.is_empty(),
        "a shorthand VSC line does not invent converter rows"
    );
}

#[test]
fn multi_section_line_minimal() {
    let raw = raw_with_msl_rows("100, 200, 0.01, 0.05, 'MSL1'");
    let network = parse_snippet(&raw).expect("Failed to parse minimal MSL");

    assert_eq!(network.multi_section_lines.len(), 1);
    let msl = &network.multi_section_lines[0];
    assert_eq!(msl.from_bus_id, 100);
    assert_eq!(msl.to_bus_id, 200);
    assert_eq!(msl.ckt.as_ref(), "MSL1");
    assert!((msl.total_r_pu - 0.01).abs() < 1.0e-9);
    assert!((msl.total_x_pu - 0.05).abs() < 1.0e-9);
}

#[test]
fn multi_section_line_full() {
    let raw = raw_with_msl_rows("50, 60, 0.02, 0.08, 0.03, 300.0, 250.0, 'MSL_FULL'");
    let network = parse_snippet(&raw).expect("Failed to parse full MSL");

    assert_eq!(network.multi_section_lines.len(), 1);
    let msl = &network.multi_section_lines[0];
    assert_eq!(msl.from_bus_id, 50);
    assert_eq!(msl.to_bus_id, 60);
    assert_eq!(msl.ckt.as_ref(), "MSL_FULL");
    assert!((msl.total_r_pu - 0.02).abs() < 1.0e-9);
    assert!((msl.total_x_pu - 0.08).abs() < 1.0e-9);
    assert!((msl.total_b_pu - 0.03).abs() < 1.0e-9);
    assert!((msl.rate_a_mva - 300.0).abs() < 1.0e-9);
    assert_eq!(msl.rate_b_mva, Some(250.0));
}

#[test]
fn multiple_dc_lines() {
    let raw = raw_with_dc_rows(
        "10, 20, 'DC1', 'LCC', 5.0, 0.1\n\
         30, 40, 'DC2', 'LCC', 3.0, 0.05\n\
         50, 60, 'DC3', 'LCC', 4.5, 0.08",
    );
    let network = parse_snippet(&raw).expect("Failed to parse multiple DC lines");

    assert_eq!(network.dc_lines_2w.len(), 3);
    assert_eq!(network.dc_lines_2w[0].from_bus_id, 10);
    assert_eq!(network.dc_lines_2w[1].from_bus_id, 30);
    assert_eq!(network.dc_lines_2w[2].from_bus_id, 50);
}

#[test]
fn multiple_multi_section_lines() {
    let raw = raw_with_msl_rows(
        "100, 200, 'MSL1', 0.01, 0.05, 0.02, 500.0\n\
         150, 250, 'MSL2', 0.015, 0.06, 0.025, 400.0",
    );
    let network = parse_snippet(&raw).expect("Failed to parse multiple MSL records");

    assert_eq!(network.multi_section_lines.len(), 2);
    assert_eq!(network.multi_section_lines[0].from_bus_id, 100);
    assert_eq!(network.multi_section_lines[1].from_bus_id, 150);
}

#[test]
fn malformed_dc_line_no_endpoints() {
    let raw = raw_with_dc_rows(
        ", , 'DC_BAD', 'LCC', 5.0, 0.1\n\
         10, 20, 'DC_GOOD', 'LCC', 3.0, 0.05",
    );
    let network = parse_snippet(&raw).expect("Failed to parse mixed malformed/valid DC rows");

    assert_eq!(network.dc_lines_2w.len(), 1);
    assert_eq!(network.dc_lines_2w[0].from_bus_id, 10);
    assert_eq!(network.dc_lines_2w[0].to_bus_id, 20);
}

#[test]
fn msl_same_endpoint_rejected() {
    let raw = raw_with_msl_rows(
        "100, 100, 'MSL_BAD', 0.01, 0.05, 0.02, 500.0\n\
         100, 200, 'MSL_GOOD', 0.015, 0.06, 0.025, 400.0",
    );
    let network = parse_snippet(&raw).expect("Failed to parse mixed MSL rows");

    assert_eq!(network.multi_section_lines.len(), 1);
    assert_eq!(network.multi_section_lines[0].from_bus_id, 100);
    assert_eq!(network.multi_section_lines[0].to_bus_id, 200);
}

#[test]
fn dc_line_garbage_parameters() {
    let raw = raw_with_dc_rows("10, 20, 'DC1', 'LCC', abc, xyz, NaN");
    let network = parse_snippet(&raw).expect("Parser should not crash on garbage parameters");

    assert_eq!(network.dc_lines_2w.len(), 1);
    let dc = &network.dc_lines_2w[0];
    assert!(dc.r_ohm.is_finite());
    assert!(dc.l_henry.is_none());
}

#[test]
fn msl_sparse_numeric_data() {
    let raw = raw_with_msl_rows("75, 85, 0.005, , 0.01, 250.0, 200.0, 'MSL_SPARSE'");
    let network = parse_snippet(&raw).expect("Failed to parse sparse MSL row");

    assert_eq!(network.multi_section_lines.len(), 1);
    let msl = &network.multi_section_lines[0];
    assert_eq!(msl.from_bus_id, 75);
    assert_eq!(msl.to_bus_id, 85);
    assert!((msl.total_r_pu - 0.005).abs() < 1.0e-9);
    assert_eq!(msl.total_x_pu, 0.01);
    assert_eq!(msl.total_b_pu, 250.0);
    assert_eq!(msl.rate_a_mva, 200.0);
    assert_eq!(msl.rate_b_mva, Some(0.0));
}

#[test]
fn mixed_dc_and_msl_records() {
    let raw = raw_with_dc_and_msl_rows(
        "10, 20, 'DC1', 'LCC', 5.0, 0.1\n\
         30, 40, 'DC2', 'LCC', 3.0, 0.05",
        "100, 200, 'MSL1', 0.01, 0.05, 0.02, 500.0, 400.0",
    );
    let network = parse_snippet(&raw).expect("Failed to parse mixed DC/MSL rows");

    assert_eq!(network.dc_lines_2w.len(), 2);
    assert_eq!(network.multi_section_lines.len(), 1);
}

#[test]
fn empty_dc_and_msl_sections() {
    let raw = raw_with_dc_and_msl_rows("", "");
    let network = parse_snippet(&raw).expect("Failed to parse empty DC/MSL sections");

    assert_eq!(network.dc_lines_2w.len(), 0);
    assert_eq!(network.multi_section_lines.len(), 0);
}

#[test]
fn dc_line_ckt_preservation() {
    let raw = raw_with_dc_rows(
        "10, 20, 'A', 'LCC', 5.0, 0.1\n\
         30, 40, 'DC12', 'LCC', 3.0, 0.05",
    );
    let network = parse_snippet(&raw).expect("Failed to parse DC circuit IDs");

    assert_eq!(network.dc_lines_2w.len(), 2);
    assert_eq!(network.dc_lines_2w[0].ckt.as_ref(), "A");
    assert_eq!(network.dc_lines_2w[1].ckt.as_ref(), "DC12");
}

#[test]
fn dc_line_quoted_strings() {
    let raw = raw_with_dc_rows("10, 20, \"HVDC_01\", \"LCC\", 5.0, 0.1, 100.0, 50.0, 1.0");
    let network = parse_snippet(&raw).expect("Failed to parse quoted-string DC row");

    assert_eq!(network.dc_lines_2w.len(), 1);
    assert_eq!(network.dc_lines_2w[0].ckt.as_ref(), "HVDC_01");
}

#[test]
fn large_bus_numbers() {
    let raw = raw_with_dc_and_msl_rows(
        "999997, 999998, 'DC_LARGE', 'LCC', 5.0, 0.1",
        "500000, 600000, 'MSL_LARGE', 0.01, 0.05, 0.02, 500.0",
    );
    let network = parse_snippet(&raw).expect("Failed to parse large bus IDs");

    assert_eq!(network.dc_lines_2w.len(), 1);
    assert_eq!(network.multi_section_lines.len(), 1);
    assert_eq!(network.dc_lines_2w[0].from_bus_id, 999997);
    assert_eq!(network.multi_section_lines[0].from_bus_id, 500000);
}

#[test]
fn negative_bus_numbers_rejected() {
    let raw = raw_with_dc_rows(
        "-10, -20, 'DC_NEG', 'LCC', 5.0, 0.1\n\
         10, 20, 'DC_GOOD', 'LCC', 3.0, 0.05",
    );
    let network = parse_snippet(&raw).expect("Failed to parse mixed negative/valid DC rows");

    assert_eq!(network.dc_lines_2w.len(), 1);
    assert_eq!(network.dc_lines_2w[0].from_bus_id, 10);
    assert_eq!(network.dc_lines_2w[0].to_bus_id, 20);
}

#[test]
fn msl_with_wmod_like_field() {
    let raw = raw_with_msl_rows("100, 200, 'MSL1', 0.01, 0.05, 0.02, 500.0, 400.0, 1");
    let network =
        parse_snippet(&raw).expect("Failed to parse MSL row with trailing WMOD-like field");

    assert_eq!(network.multi_section_lines.len(), 1);
    let msl = &network.multi_section_lines[0];
    assert_eq!(msl.from_bus_id, 100);
    assert_eq!(msl.to_bus_id, 200);
}

#[test]
fn worked_lcc_line_round_trips_as_v0144() {
    let raw = "0, 100.0, 33, 1, 60.0 / worked lcc\n\
         WORKED\n\
         WORKED\n\
         58540,'B58540',345.0,1,1,1,1,1.0,0.0,1.1,0.9,1.1,0.9\n\
         55247,'B55247',345.0,1,1,1,1,1.0,0.0,1.1,0.9,1.1,0.9\n\
         0 / END OF BUS DATA, BEGIN LOAD DATA\n\
         0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA\n\
         0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA\n\
         0 / END OF GENERATOR DATA, BEGIN BRANCH DATA\n\
         0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA\n\
         0 / END OF TRANSFORMER DATA, BEGIN AREA DATA\n\
         0 / END OF AREA DATA, BEGIN TWO-TERMINAL DC DATA\n\
         'DC Line 1   ', 1, 10.0000, 550.00, 250.00, 0.00, 0.0000, 0.10000, I, 0.00, 0, 0.00000\n\
         58540, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0\n\
         55247, 2, 15, 15, 0, 10, 345, 0.55784, 1.5, 1.5, 0.51, 0.00625, 0, 0, 0, 0, 0\n\
         0 / END OF TWO-TERMINAL DC DATA, BEGIN ZONE DATA\n\
         0 / END OF ZONE DATA\n\
         Q\n";
    let mut raw_file = NamedTempFile::new().expect("raw");
    raw_file.write_all(raw.as_bytes()).expect("write raw");
    raw_file.flush().expect("flush");
    let out = NamedTempFile::new().expect("rpf");
    let out_path = out.path().to_path_buf();
    raptrix_psse_rs::write_psse_to_rpf(
        raw_file.path().to_str().unwrap(),
        None,
        out_path.to_str().unwrap(),
    )
    .expect("write rpf");

    let meta = raptrix_cim_arrow::rpf_file_metadata(&out_path).expect("metadata");
    assert_eq!(meta.get("rpf_version").map(String::as_str), Some("v0.14.4"));
    assert_eq!(
        meta.get("raptrix.features.dc_converters")
            .map(String::as_str),
        Some("true")
    );

    let tables = raptrix_cim_arrow::read_rpf_tables(&out_path).expect("read");
    let line = tables
        .iter()
        .find(|(name, _)| name == "dc_lines_2w")
        .expect("dc_lines_2w")
        .1
        .clone();
    assert_eq!(line.num_columns(), 15);
    let p = line
        .column_by_name("p_setpoint_mw")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(p.value(0), 550.0);
    assert!(line.column_by_name("q_from_mvar").unwrap().is_null(0));
    assert!(line.column_by_name("q_to_mvar").unwrap().is_null(0));
    let name = line
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "DC Line 1");

    let ends = tables
        .iter()
        .find(|(name, _)| name == "dc_converters")
        .expect("dc_converters")
        .1
        .clone();
    assert_eq!(ends.num_rows(), 2);
    let buses = ends
        .column_by_name("bus_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let meter = ends
        .column_by_name("is_meter_end")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let tr = ends
        .column_by_name("tr")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let alpha = ends.column_by_name("alpha_deg").unwrap();
    let gamma = ends.column_by_name("gamma_deg").unwrap();
    for i in 0..2 {
        assert!(alpha.is_null(i) && gamma.is_null(i));
        for vsc in [
            "ac_control",
            "ac_setpoint",
            "q_min_mvar",
            "q_max_mvar",
            "s_max_mva",
            "i_max_ka",
            "loss_const_mw",
            "loss_i_mw_per_ka",
        ] {
            assert!(
                ends.column_by_name(vsc).unwrap().is_null(i),
                "{vsc} must be null on an LCC row"
            );
        }
        assert!((tr.value(i) - 0.55784).abs() < 1e-12);
        if buses.value(i) == 55247 {
            assert!(meter.value(i));
        } else {
            assert_eq!(buses.value(i), 58540);
            assert!(!meter.value(i));
        }
    }

    let buses_tbl = tables
        .iter()
        .find(|(name, _)| name == "buses")
        .expect("buses")
        .1
        .clone();
    let p_sched = buses_tbl
        .column_by_name("p_sched")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 0..p_sched.len() {
        assert!(
            p_sched.value(i).abs() < 1e-12,
            "DC SETVL must not be copied into bus p_sched"
        );
    }
}
