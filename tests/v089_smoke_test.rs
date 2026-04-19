// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

use std::{fs, path::PathBuf, time::{SystemTime, UNIX_EPOCH}};

use arrow::array::{Array, BooleanArray, Int32Array, StringArray};
use raptrix_cim_arrow::{TABLE_BRANCHES, TABLE_BUSES, TABLE_GENERATORS, TABLE_OWNERS};

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
fn v089_generators_hierarchy_and_ownership_smoke() {
    let raw_path = unique_temp_path("v089_smoke", "raw");
    let dyr_path = unique_temp_path("v089_smoke", "dyr");
    let out_path = unique_temp_path("v089_smoke", "rpf");

    let raw = r#"0, 100.0, 33, 0, 0, 60.0 / V089_SMOKE
V089 SMOKE
V089 SMOKE
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

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(dyr_path);
    let _ = fs::remove_file(out_path);
}
