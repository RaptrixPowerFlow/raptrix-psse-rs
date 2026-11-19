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

use arrow::ipc::reader::FileReader;
use raptrix_cim_arrow::{TABLE_CONTINGENCIES, read_contingencies_batch};
use raptrix_psse_rs::ExportOptions;

fn unique_temp_path(stem: &str, ext: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must be after epoch")
        .as_nanos();
    path.push(format!("raptrix_psse_rs_{stem}_{nanos}.{ext}"));
    path
}

const RAW: &str = r#"0, 100.0, 33, 0, 0, 60.0 / SYNTHETIC
SYNTHETIC
SYNTHETIC
1,'BUS1',230.0,3,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'BUS2',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
3,'BUS3',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
4,'BUS4',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
5,'BUS5',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
6,'BUS6',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
7,'BUS7',115.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
8,'BUS8',13.8,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
2,'1',1,1,1,40.0,10.0,0,0,0,0,1,1,0
2,'2',1,1,1,0.0,0.0,0,0,0,0,1,1,0
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
1,'1',75.0,10.0,40.0,-20.0,1.02,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0,1,1,1.0
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
2,3,'1',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
2,3,'B',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
4,5,0,'1',1,1,1,0.0,0.0,2,'',1
0.01,0.10,100.0
1.0,230.0,0.0,100.0,110.0,120.0
1.0,230.0
6,7,8,'1',1,1,1,0.0,0.0,2,'',1
0.01,0.10,100.0,0.02,0.20,100.0,0.03,0.30,100.0
1.0,230.0,0.0,100.0,110.0,120.0
1.0,115.0,0.0,90.0,100.0,110.0
1.0,13.8,0.0,80.0,90.0,100.0
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

const CON: &str = r#"
CONTINGENCY 'LINE'
 DISCONNECT BRANCH FROM BUS 3 TO BUS 2 CKT 1
END
CONTINGENCY 'XF'
 OPEN BRANCH FROM BUS 5 TO BUS 4 CKT '1'
END
CONTINGENCY 'T3'
 DISCONNECT BRANCH FROM BUS 8 TO BUS 6 CKT 1
END
CONTINGENCY 'GEN'
 REMOVE MACHINE 1 FROM BUS 1
END
CONTINGENCY 'LD'
 REMOVE LOAD '1' FROM BUS 2
END
CONTINGENCY 'SHED'
 SET BUS 2 LOAD TO 0 MW
END
CONTINGENCY 'CLOSE'
 CLOSE BRANCH FROM BUS 2 TO BUS 3 CKT 1
END
CONTINGENCY 'BAD'
 DISCONNECT BRANCH FROM BUS 2 TO BUS 3 CKT 1
 REMOVE MACHINE 9 FROM BUS 1
END
CONTINGENCY 'EMPTY'
END
"#;

#[test]
fn con_file_fills_contingencies_and_omits_the_stub_flag() {
    let raw_path = unique_temp_path("con_import", "raw");
    let con_path = unique_temp_path("con_import", "con");
    let out_path = unique_temp_path("con_import", "rpf");
    fs::write(&raw_path, RAW).expect("write raw");
    fs::write(&con_path, CON).expect("write con");

    raptrix_psse_rs::write_psse_to_rpf_with_options(
        raw_path.to_str().expect("utf-8"),
        None,
        out_path.to_str().expect("utf-8"),
        &ExportOptions {
            contingency_path: Some(con_path.clone()),
            ..ExportOptions::default()
        },
    )
    .expect("convert");

    let file = fs::File::open(&out_path).expect("open rpf");
    let reader = FileReader::try_new(file, None).expect("ipc reader");
    assert!(
        reader
            .schema()
            .metadata()
            .get("raptrix.features.contingencies_stub")
            .is_none(),
        "a file with contingency rows is not a stub"
    );

    let tables = raptrix_psse_rs::read_rpf_tables(&out_path).expect("read tables");
    let batch = tables
        .iter()
        .find(|(name, _)| name == TABLE_CONTINGENCIES)
        .map(|(_, batch)| batch)
        .expect("contingencies table");
    let rows = read_contingencies_batch(batch).expect("decode contingencies");
    let ids: Vec<_> = rows.iter().map(|row| row.contingency_id.as_str()).collect();
    assert_eq!(ids, vec!["LINE", "XF", "T3", "GEN", "LD", "SHED", "CLOSE"]);
    assert!(ids.iter().all(|id| *id != "BAD" && *id != "EMPTY"));
    assert_eq!(rows[0].elements[0].branch_id, Some(1));
    assert_eq!(
        rows[0].elements[0].equipment_kind.as_deref(),
        Some("psse_branch")
    );
    assert_eq!(rows[0].elements[0].equipment_id.as_deref(), Some("3|2|1"));
    assert_eq!(rows[1].elements[0].branch_id, Some(-1));
    assert_eq!(rows[2].elements[0].element_type, "branch_outage");
    assert_eq!(rows[3].elements[0].element_type, "gen_trip");
    assert_eq!(rows[3].elements[0].gen_id.as_deref(), Some("1"));
    assert_eq!(rows[4].elements[0].amount_mw, Some(40.0));
    assert_eq!(rows[5].elements[0].amount_mw, Some(0.0));
    assert!(!rows[6].elements[0].status_change);
    assert!(rows.iter().all(|row| row.tpl_category.is_none()));

    let _ = fs::remove_file(raw_path);
    let _ = fs::remove_file(con_path);
    let _ = fs::remove_file(out_path);
}
