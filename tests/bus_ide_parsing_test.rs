// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! Bus `IDE` / RPF `type` alignment per the PSS®E Program Operation Manual:
//!   IDE=1 → load (PQ), IDE=2 → PV generator, IDE=3 → swing/slack, IDE=4 → disconnected (folded into PQ).
//! Also covers the v35 optional substation-name column inserted after BASKV.

use std::io::Write;

use raptrix_psse_rs::models::BusType;
use raptrix_psse_rs::parser::parse_raw;
use tempfile::NamedTempFile;

fn write_parse(raw: &str) -> raptrix_psse_rs::models::Network {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(raw.as_bytes()).expect("write raw");
    f.flush().expect("flush");
    parse_raw(f.path()).expect("parse_raw")
}

#[test]
fn v33_maps_psse_ide_2_to_pv_3_to_slack_and_4_to_load() {
    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / bus ide v33
L2
L3
1,'PQLOAD    ',138.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'PVTWO     ',138.0,2,1,1,1,1.02,0.00,1.10,0.90,1.10,0.90
3,'SWING     ',138.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
4,'ISOLATED  ',138.0,4,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
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
    let n = write_parse(raw);
    let by_id: std::collections::HashMap<u32, BusType> =
        n.buses.iter().map(|b| (b.i, b.ide)).collect();
    assert_eq!(
        by_id[&1],
        BusType::LoadBus,
        "PSS/E IDE=1 is a load (PQ) bus"
    );
    assert_eq!(
        by_id[&2],
        BusType::GeneratorPV,
        "PSS/E IDE=2 is a voltage-regulating generator (PV)"
    );
    assert_eq!(
        by_id[&3],
        BusType::Slack,
        "PSS/E IDE=3 is the swing/slack bus"
    );
    assert_eq!(
        by_id[&4],
        BusType::LoadBus,
        "PSS/E IDE=4 is a disconnected/isolated bus, folded into PQ on import"
    );
}

#[test]
fn v35_optional_field_after_baskv_keeps_ide_and_vm_aligned() {
    let raw = r#"0, 100.0, 35, 0, 0, 60.0 / bus ide v35
L2
L3
0 / END OF SYSTEM-WIDE DATA, BEGIN BUS DATA
1,'WITHSUB   ',138.0,'SUB_A      ',2,7,26,1,1.03125,0.50,1.10,0.90,1.10,0.90
2,'NOSUB     ',138.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
3,'ISO       ',138.0,4,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
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
    let n = write_parse(raw);
    let b1 = n.buses.iter().find(|b| b.i == 1).expect("bus 1");
    assert_eq!(b1.ide, BusType::GeneratorPV);
    assert!(
        (b1.vm - 1.03125).abs() < 1e-9,
        "VM must follow OWNER column, not substation token"
    );
    assert!((b1.va - 0.50).abs() < 1e-9);
    assert_eq!(b1.area, 7);
    assert_eq!(b1.zone, 26);

    let b2 = n.buses.iter().find(|b| b.i == 2).expect("bus 2");
    assert_eq!(b2.ide, BusType::Slack, "PSS/E IDE=3 is the slack bus");

    let b3 = n.buses.iter().find(|b| b.i == 3).expect("bus 3");
    assert_eq!(
        b3.ide,
        BusType::LoadBus,
        "PSS/E IDE=4 is disconnected/isolated, folded into PQ"
    );
}
