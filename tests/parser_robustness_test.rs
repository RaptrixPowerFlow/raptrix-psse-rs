// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

use raptrix_psse_rs::models::Network;
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
fn dc_line_minimal_fields() {
    let raw = raw_with_dc_rows("10, 20, 'DC1', 'LCC'");
    let network = parse_snippet(&raw).expect("Failed to parse minimal DC line");

    assert_eq!(network.dc_lines_2w.len(), 1);
    let dc = &network.dc_lines_2w[0];
    assert_eq!(dc.from_bus_id, 10);
    assert_eq!(dc.to_bus_id, 20);
    assert_eq!(dc.ckt.as_ref(), "DC1");
    assert_eq!(dc.converter_type.as_ref(), "lcc");
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
    let raw = format!(
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
         0 / END OF TWO-TERMINAL DC DATA, BEGIN VSC DC DATA\n\
         5, 15, 'VSC_A', 'VSC', 2.0, 0.05, 150.0, 75.0, 1.2\n\
         0 / END OF VSC DC DATA, BEGIN ZONE DATA\n\
         0 / END OF ZONE DATA\n\
         Q\n"
    );

    let network = parse_snippet(&raw).expect("Failed to parse VSC DC line");

    assert_eq!(network.dc_lines_2w.len(), 1);
    let dc = &network.dc_lines_2w[0];
    assert_eq!(dc.from_bus_id, 5);
    assert_eq!(dc.to_bus_id, 15);
    assert_eq!(dc.converter_type.as_ref(), "vsc");
    assert_eq!(dc.ckt.as_ref(), "VSC_A");
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
    let network = parse_snippet(&raw).expect("Failed to parse MSL row with trailing WMOD-like field");

    assert_eq!(network.multi_section_lines.len(), 1);
    let msl = &network.multi_section_lines[0];
    assert_eq!(msl.from_bus_id, 100);
    assert_eq!(msl.to_bus_id, 200);
}
