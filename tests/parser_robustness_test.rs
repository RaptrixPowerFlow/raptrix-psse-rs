// raptrix-psse-rs
// Copyright (c) 2026 Raptrix Power
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, StringArray};
use raptrix_psse_rs::models::{BusType, DcConverter, Network};
use raptrix_psse_rs::parser::parse_raw;
use raptrix_psse_rs::validation::{run_mmwg_checks, Severity};
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
    assert_eq!(meta.get("rpf_version").map(String::as_str), Some("v0.14.6"));
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

/// REV 29/30 keep inline bus shunts and a short switched-shunt record.
/// A negative branch J is the metered end, on every revision.
#[test]
fn pre31_columns_and_metered_end_j() {
    let rev29 = "\
0, 100.0, 29, 0, 0, 60.0 / synthetic
S
S
101, 'ONE', 115.0, 1, 0.0, -2.5, 2, 3, 1.02, -15.5, 7
102, 'TWO', 230.0, 2, 1.5, 0.0, 2, 3, 1.01, 4.0, 8
301, 69.0, 1, 0.0, 0.0, 1, 1, 1.0, 0.0, 1
401, '12 34567', 69.0, 1, 0.0, 0.0, 1, 1, 1.04, -3.0, 2
0 / END OF BUS DATA, BEGIN BRANCH DATA
101, -102, '1', 0.01, 0.05, 0.02, 100, 110, 120, 0, 0, 0, 0, 1, 0, 0, 1
-101, 102, '9', 0.01, 0.05, 0.0, 10, 10, 10, 0, 0, 0, 0, 1
101, 102, '2', 0.02, 0.06, 0.0, 50, 50, 50, 0, 0, 0, 0, 0, 1.0, 0, 1
0 / END OF BRANCH DATA, BEGIN SWITCHED SHUNT DATA
101, 1, 1.05, 0.95, 0, 12.5, 1, -6.0
102, 0, 1.04, 0.98, 101, 'RMT', 3.0, 2, 5.0
103, 1, 1.05, 0.95, 0,, 12.5, 1.0, -6.0
0 / END OF SWITCHED SHUNT DATA
Q
";
    let n29 = parse_snippet(rev29).expect("rev 29");
    assert_eq!(n29.case_id.rev, 29);
    let b101 = n29.buses.iter().find(|b| b.i == 101).unwrap();
    assert_eq!(b101.ide, BusType::LoadBus);
    assert!((b101.bl + 2.5).abs() < 1e-9);
    assert_eq!(b101.area, 2);
    assert_eq!(b101.zone, 3);
    assert!((b101.vm - 1.02).abs() < 1e-9);
    assert!((b101.va + 15.5).abs() < 1e-9);
    assert_eq!(b101.owner, 7);
    let b102 = n29.buses.iter().find(|b| b.i == 102).unwrap();
    assert_eq!(b102.ide, BusType::GeneratorPV);
    assert!((b102.gl - 1.5).abs() < 1e-9);
    assert!((b102.vm - 1.01).abs() < 1e-9);
    let b301 = n29.buses.iter().find(|b| b.i == 301).unwrap();
    assert!((b301.baskv - 69.0).abs() < 1e-9);
    assert!((b301.vm - 1.0).abs() < 1e-9);
    assert_eq!(b301.owner, 1);
    let b401 = n29.buses.iter().find(|b| b.i == 401).unwrap();
    assert!(b401.name.trim().starts_with("12 34567"));
    assert!((b401.baskv - 69.0).abs() < 1e-9);
    assert!((b401.vm - 1.04).abs() < 1e-9);
    assert!((b401.va + 3.0).abs() < 1e-9);

    assert_eq!(n29.branches.len(), 2);
    let metered = n29.branches.iter().find(|b| b.ckt.as_ref() == "1").unwrap();
    assert_eq!(
        (metered.i, metered.j, metered.met, metered.st),
        (101, 102, 2, 1)
    );
    let open = n29.branches.iter().find(|b| b.ckt.as_ref() == "2").unwrap();
    assert_eq!((open.i, open.j, open.st), (101, 102, 0));

    assert_eq!(n29.switched_shunts.len(), 3);
    let s101 = n29.switched_shunts.iter().find(|s| s.i == 101).unwrap();
    assert_eq!(s101.modsw, 1);
    assert!((s101.vswhi - 1.05).abs() < 1e-9);
    assert!((s101.binit - 12.5).abs() < 1e-9);
    assert_eq!(s101.steps, vec![-6.0]);
    assert!((s101.rmpct - 100.0).abs() < 1e-9);
    let s102 = n29.switched_shunts.iter().find(|s| s.i == 102).unwrap();
    assert_eq!(s102.rmidnt.as_ref(), "RMT");
    assert!((s102.binit - 3.0).abs() < 1e-9);
    assert_eq!(s102.steps, vec![5.0, 5.0]);
    assert_eq!(s102.swrem, 101);
    let s103 = n29.switched_shunts.iter().find(|s| s.i == 103).unwrap();
    assert!(s103.rmidnt.is_empty());
    assert!((s103.binit - 12.5).abs() < 1e-9);
    assert_eq!(s103.steps, vec![-6.0]);

    let rev30 = "\
0, 100.0, 30, 0, 0, 60.0 / synthetic
S
S
101, 'ONE', 115.0, 1, 0.0, 0.0, 1, 1, 0.99, 2.5, 4
0 / END OF BUS DATA, BEGIN SWITCHED SHUNT DATA
101, 1, 1.06, 0.97, 102, 75.0, 'S1', -4.0, 1, 8.0
0 / END OF SWITCHED SHUNT DATA
Q
";
    let n30 = parse_snippet(rev30).expect("rev 30");
    assert_eq!(n30.case_id.rev, 30);
    let bus = &n30.buses[0];
    assert!((bus.vm - 0.99).abs() < 1e-9);
    assert!((bus.va - 2.5).abs() < 1e-9);
    assert_eq!(bus.owner, 4);
    let shunt = &n30.switched_shunts[0];
    assert_eq!(shunt.swrem, 102);
    assert!((shunt.rmpct - 75.0).abs() < 1e-9);
    assert_eq!(shunt.rmidnt.as_ref(), "S1");
    assert!((shunt.binit + 4.0).abs() < 1e-9);
    assert_eq!(shunt.steps, vec![8.0]);
    assert_eq!(shunt.stat, 1);

    let rev33 = "\
0, 100.0, 33, 0, 0, 60.0 / synthetic
S
S
201, 'MODERN', 138.0, 2, 4, 5, 6, 1.03, -8.0
0 / END OF BUS DATA, BEGIN BRANCH DATA
201, -202, '1', 0.01, 0.04, 0.0, 80, 80, 80, 0, 0, 0, 0, 1, 0
0 / END OF BRANCH DATA, BEGIN SWITCHED SHUNT DATA
201, 1, 0, 1, 1.05, 0.95, 0, 100.0, 'ID', 8.0, 2, 4.0
0 / END OF SWITCHED SHUNT DATA
Q
";
    let n33 = parse_snippet(rev33).expect("rev 33");
    assert_eq!(n33.case_id.rev, 33);
    let modern = &n33.buses[0];
    assert_eq!(modern.ide, BusType::GeneratorPV);
    assert_eq!((modern.area, modern.zone, modern.owner), (4, 5, 6));
    assert!((modern.vm - 1.03).abs() < 1e-9);
    assert!((modern.va + 8.0).abs() < 1e-9);
    assert!(modern.gl.abs() < 1e-12 && modern.bl.abs() < 1e-12);
    assert_eq!(
        (n33.branches[0].i, n33.branches[0].j, n33.branches[0].met),
        (201, 202, 2)
    );
    let modern_shunt = &n33.switched_shunts[0];
    assert!((modern_shunt.vswhi - 1.05).abs() < 1e-9);
    assert!((modern_shunt.vswlo - 0.95).abs() < 1e-9);
    assert!((modern_shunt.binit - 8.0).abs() < 1e-9);
    assert_eq!(modern_shunt.rmidnt.as_ref(), "ID");
    assert_eq!(modern_shunt.steps, vec![4.0, 4.0]);
    assert_eq!(modern_shunt.stat, 1);
}

#[test]
fn rev29_numeric_name_commentless_sections_and_transformer_comment() {
    let commented = "\
0, 100.0, 29, 0, 0, 60.0 / synthetic
S
S
1,'SLACK',138.0,3,0.0,0.0,1,1,1.01,0.0,1
3,'12',138.0,2,0.0,0.0,1,1,1.02,3.0,1
0 / END OF BUS DATA, BEGIN TRANSFORMER DATA
1,3,0,'1',1,1,1,0.0,0.0,2,'T',1,1,1.0,0,0,0,0,0,0
0.001, 0.05, 100.00
1.0, 0.0, 0.0, 100, 100, 100, 1, 99, 1.1, 0.9, 1.1, 0.9, 0, 0, 0.0, 0.0, /* a,b,c,d,e,f,g,h,i,j */
1.05, 0.0, /* winding two */
0 / END OF TRANSFORMER DATA
Q
";
    let net = parse_snippet(commented).expect("commented transformer");
    let named = net.buses.iter().find(|bus| bus.i == 3).unwrap();
    assert!(named.name.trim().starts_with('1'));
    assert!((named.baskv - 138.0).abs() < 1e-9);
    assert!((named.vm - 1.02).abs() < 1e-9);
    assert_eq!(named.ide, BusType::GeneratorPV);
    let tx = &net.transformers[0];
    assert!((tx.windv1 - 1.0).abs() < 1e-9);
    assert!((tx.windv2 - 1.05).abs() < 1e-9);
    assert!((tx.nomv2).abs() < 1e-9);
    assert_eq!(tx.cod1, 1);
    assert_eq!(tx.cont1, 99);

    let commentless = "\
0, 100.0, 29, 0, 0, 60.0 / synthetic
S
S
10,'A',138.0,1,0.0,0.0,1,1,1.03,-1.0,1
0
0
0
10,11,'1',0.01,0.05,0.0,50,50,50,0,0,0,0,1
0
0
0
0
0
10,0,1.10,0.90,0,,4.0,1,8.0
0
";
    let bare = parse_snippet(commentless).expect("commentless rev 29");
    assert_eq!(bare.buses.len(), 1);
    assert!((bare.buses[0].vm - 1.03).abs() < 1e-9);
    assert_eq!(bare.branches.len(), 1);
    assert_eq!(bare.switched_shunts.len(), 1);
    assert!((bare.switched_shunts[0].binit - 4.0).abs() < 1e-9);
    assert_eq!(bare.switched_shunts[0].steps, vec![8.0]);
}

#[test]
fn published_flow_and_ide4_are_named() {
    let raw = "\
0, 100.0, 29, 0, 0, 60.0 / synthetic
S
S
1,'SLACK',138.0,3,0.0,0.0,1,1,1.00,0.0,1
2,'FAR',138.0,1,0.0,0.0,1,1,1.00,25.2,1
4,'OFF',138.0,4,0.0,0.0,1,1,1.00,0.0,1
0 / END OF BUS DATA, BEGIN GENERATOR DATA
1,'1',10.0,0.0,100.0,-100.0,1.00,4,100.0,0,0,0,0,1,1,100.0,20.0,0.0,1,1,0,0,0,0,0
4,'1',5.0,0.0,10.0,-10.0,1.00,0,100.0,0,0,0,0,1,1,100.0,10.0,0.0,1,1,0,0,0,0,0
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1',0.0001,0.0012,0.0,800,800,800,0,0,0,0,1
0 / END OF BRANCH DATA
Q
";
    let net = parse_snippet(raw).expect("infeasible");
    assert!(net.buses.iter().any(|bus| bus.i == 4 && bus.disconnected));
    let report = run_mmwg_checks(&net);
    let checks: Vec<&str> = report.issues.iter().map(|issue| issue.check).collect();
    assert!(
        checks.contains(&"MMWG-7.3.3/infeasible-published-flow"),
        "missing flow screen: {checks:?}"
    );
    assert!(checks.contains(&"MMWG-7.3.5/gen-on-isolated-bus"));
    assert!(checks.contains(&"MMWG-7.3.5/ireg-target-isolated"));
    assert!(report
        .issues
        .iter()
        .any(|issue| issue.check == "MMWG-7.3.3/infeasible-published-flow"
            && issue.severity == Severity::Error
            && issue.message.contains("1-2")));
}
