// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! PSS/E contingency file (`.con`) → `contingencies` rows.
//!
//! A row is emitted only when every action resolves to one in-service record.
//! Labels and bus numbers are not logged.

use std::collections::{BTreeMap, HashSet};

use raptrix_cim_arrow::{ContingencyElementRow, ContingencyRow};

use crate::models::{Network, ThreeWindingTransformer};

const PSSE_BRANCH_KIND: &str = "psse_branch";
/// Not a `branches.branch_id`. The solver opens the endpoint key instead.
const TRANSFORMER_BRANCH_SENTINEL: i32 = -1;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContingencyImportStats {
    pub read: u64,
    pub emitted: u64,
    pub empty: u64,
    pub duplicate: u64,
    pub unresolved: u64,
    pub unknown_verb: u64,
    pub bad_action: u64,
    pub no_endpoint: u64,
    pub ambiguous: u64,
    pub no_load: u64,
    pub no_machine: u64,
    pub load_setpoint: u64,
}

impl ContingencyImportStats {
    pub fn reason_summary(&self) -> String {
        format!(
            "unknown_verb:{} bad_action:{} no_endpoint:{} ambiguous:{} no_load:{} no_machine:{} load_setpoint:{}",
            self.unknown_verb,
            self.bad_action,
            self.no_endpoint,
            self.ambiguous,
            self.no_load,
            self.no_machine,
            self.load_setpoint
        )
    }
}

#[derive(Debug)]
enum Action {
    Branch {
        from: u32,
        to: u32,
        ckt: String,
        open: bool,
    },
    Machine {
        bus: u32,
        id: String,
    },
    Load {
        bus: u32,
        id: String,
    },
    ShedBus {
        bus: u32,
    },
}

#[derive(Debug, Clone, Copy)]
enum ActionFault {
    UnknownVerb,
    BadAction,
    NoEndpoint,
    Ambiguous,
    NoLoad,
    NoMachine,
    LoadSetpoint,
}

enum ActionOutcome {
    Keep(ContingencyElementRow),
    /// In-service record exists and has no constant-power effect.
    Drop,
    Fault(ActionFault),
}

#[derive(Clone, Copy)]
enum Equip {
    Line { branch_id: i32 },
    Transformer,
}

struct EquipmentIndex {
    /// Unordered bus pair plus trimmed circuit → matching records.
    by_endpoint: BTreeMap<(u32, u32, String), Vec<Equip>>,
}

impl EquipmentIndex {
    fn from_network(network: &Network) -> Self {
        let mut by_endpoint: BTreeMap<(u32, u32, String), Vec<Equip>> = BTreeMap::new();
        for (idx, branch) in network.branches.iter().enumerate() {
            let Some(branch_id) = i32::try_from(idx + 1).ok() else {
                continue;
            };
            by_endpoint
                .entry(endpoint_key(branch.i, branch.j, branch.ckt.as_ref()))
                .or_default()
                .push(Equip::Line { branch_id });
        }
        for tx in network.transformers.iter().filter(|tx| tx.stat != 0) {
            push_transformer(&mut by_endpoint, tx.i, tx.j, tx.ckt.as_ref());
        }
        for tx in network.transformers_3w.iter().filter(|tx| tx.stat != 0) {
            push_winding_pairs(&mut by_endpoint, tx);
        }
        Self { by_endpoint }
    }

    fn lookup(&self, from: u32, to: u32, ckt: &str) -> Result<Equip, ActionFault> {
        let hits = self
            .by_endpoint
            .get(&endpoint_key(from, to, ckt))
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        match hits {
            [one] => Ok(*one),
            [] => Err(ActionFault::NoEndpoint),
            _ => Err(ActionFault::Ambiguous),
        }
    }
}

fn push_transformer(
    index: &mut BTreeMap<(u32, u32, String), Vec<Equip>>,
    from: u32,
    to: u32,
    ckt: &str,
) {
    index
        .entry(endpoint_key(from, to, ckt))
        .or_default()
        .push(Equip::Transformer);
}

fn push_winding_pairs(
    index: &mut BTreeMap<(u32, u32, String), Vec<Equip>>,
    tx: &ThreeWindingTransformer,
) {
    let pairs = [
        (tx.bus_h, tx.bus_m),
        (tx.bus_h, tx.bus_l),
        (tx.bus_m, tx.bus_l),
    ];
    for (from, to) in pairs {
        push_transformer(index, from, to, tx.ckt.as_ref());
    }
}

fn endpoint_key(from: u32, to: u32, ckt: &str) -> (u32, u32, String) {
    let (lo, hi) = if from <= to { (from, to) } else { (to, from) };
    (lo, hi, ckt.trim().to_string())
}

fn escape_ckt(ckt: &str) -> String {
    ckt.replace('\\', "\\\\").replace('|', "\\|")
}

fn equipment_id(from: u32, to: u32, ckt: &str) -> String {
    format!("{from}|{to}|{}", escape_ckt(ckt.trim()))
}

/// Parse a contingency file and resolve it against the parsed RAW network.
pub fn import_contingencies(
    text: &str,
    network: &Network,
) -> (Vec<ContingencyRow>, ContingencyImportStats) {
    let index = EquipmentIndex::from_network(network);
    let mut stats = ContingencyImportStats::default();
    let mut rows = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let mut current_name: Option<String> = None;
    let mut actions: Vec<Result<Action, ActionFault>> = Vec::new();
    let mut started = false;

    let flush = |name: &str,
                 actions: &[Result<Action, ActionFault>],
                 stats: &mut ContingencyImportStats,
                 rows: &mut Vec<ContingencyRow>,
                 seen: &mut HashSet<String>| {
        stats.read += 1;
        if name.is_empty() {
            stats.empty += 1;
            return;
        }
        if !seen.insert(name.to_string()) {
            stats.duplicate += 1;
            return;
        }
        match resolve_actions(actions, network, &index) {
            Resolved::Empty => stats.empty += 1,
            Resolved::Fault(fault) => {
                stats.unresolved += 1;
                match fault {
                    ActionFault::UnknownVerb => stats.unknown_verb += 1,
                    ActionFault::BadAction => stats.bad_action += 1,
                    ActionFault::NoEndpoint => stats.no_endpoint += 1,
                    ActionFault::Ambiguous => stats.ambiguous += 1,
                    ActionFault::NoLoad => stats.no_load += 1,
                    ActionFault::NoMachine => stats.no_machine += 1,
                    ActionFault::LoadSetpoint => stats.load_setpoint += 1,
                }
            }
            Resolved::Rows(elements) => {
                stats.emitted += 1;
                rows.push(ContingencyRow {
                    contingency_id: name.to_string(),
                    elements,
                    ..ContingencyRow::default()
                });
            }
        }
    };

    for line in text.lines() {
        let tokens = tokenize(line);
        if tokens.is_empty() {
            continue;
        }
        if tokens[0].eq_ignore_ascii_case("CONTINGENCY") {
            if started {
                if let Some(name) = current_name.take() {
                    flush(&name, &actions, &mut stats, &mut rows, &mut seen);
                }
                actions.clear();
            }
            started = true;
            current_name = Some(tokens.get(1).cloned().unwrap_or_default());
            actions.clear();
            continue;
        }
        if tokens[0].eq_ignore_ascii_case("END") {
            if started {
                if let Some(name) = current_name.take() {
                    flush(&name, &actions, &mut stats, &mut rows, &mut seen);
                }
                actions.clear();
                started = false;
            }
            continue;
        }
        if !started {
            continue;
        }
        actions.push(parse_action(&tokens));
    }
    if started && let Some(name) = current_name.take() {
        flush(&name, &actions, &mut stats, &mut rows, &mut seen);
    }
    (rows, stats)
}

enum Resolved {
    Empty,
    Fault(ActionFault),
    Rows(Vec<ContingencyElementRow>),
}

fn resolve_actions(
    actions: &[Result<Action, ActionFault>],
    network: &Network,
    index: &EquipmentIndex,
) -> Resolved {
    if actions.is_empty() {
        return Resolved::Empty;
    }
    let mut elements = Vec::with_capacity(actions.len());
    for action in actions {
        let action = match action {
            Ok(action) => action,
            Err(fault) => return Resolved::Fault(*fault),
        };
        match resolve_action(action, network, index) {
            ActionOutcome::Keep(element) => elements.push(element),
            ActionOutcome::Drop => {}
            ActionOutcome::Fault(fault) => return Resolved::Fault(fault),
        }
    }
    if elements.is_empty() {
        Resolved::Empty
    } else {
        Resolved::Rows(elements)
    }
}

fn resolve_action(action: &Action, network: &Network, index: &EquipmentIndex) -> ActionOutcome {
    match action {
        Action::Branch {
            from,
            to,
            ckt,
            open,
        } => match index.lookup(*from, *to, ckt) {
            Ok(Equip::Line { branch_id }) => {
                ActionOutcome::Keep(branch_element(*from, *to, ckt, Some(branch_id), *open))
            }
            Ok(Equip::Transformer) => ActionOutcome::Keep(branch_element(
                *from,
                *to,
                ckt,
                Some(TRANSFORMER_BRANCH_SENTINEL),
                *open,
            )),
            Err(fault) => ActionOutcome::Fault(fault),
        },
        Action::Machine { bus, id } => {
            let matches = network
                .generators
                .iter()
                .filter(|machine| {
                    machine.stat != 0 && machine.i == *bus && machine.id.as_ref() == id
                })
                .count();
            match matches {
                1 => ActionOutcome::Keep(ContingencyElementRow {
                    element_type: "gen_trip".to_string(),
                    bus_id: i32::try_from(*bus).ok(),
                    gen_id: Some(id.clone()),
                    status_change: true,
                    ..ContingencyElementRow::default()
                }),
                0 => ActionOutcome::Fault(ActionFault::NoMachine),
                _ => ActionOutcome::Fault(ActionFault::Ambiguous),
            }
        }
        Action::Load { bus, id } => {
            let loads: Vec<_> = network
                .loads
                .iter()
                .filter(|load| load.status != 0 && load.i == *bus && load.id.as_ref() == id)
                .collect();
            match loads.as_slice() {
                [load] if load.pl.abs() < 1.0e-6 => ActionOutcome::Drop,
                [load] => ActionOutcome::Keep(ContingencyElementRow {
                    element_type: "load_shed".to_string(),
                    bus_id: i32::try_from(*bus).ok(),
                    load_id: Some(load.id.to_string()),
                    amount_mw: Some(load.pl),
                    status_change: true,
                    ..ContingencyElementRow::default()
                }),
                [] => ActionOutcome::Fault(ActionFault::NoLoad),
                _ => ActionOutcome::Fault(ActionFault::Ambiguous),
            }
        }
        Action::ShedBus { bus } => {
            let load_id = network
                .loads
                .iter()
                .find(|load| load.status != 0 && load.i == *bus && load.pl.abs() >= 1.0e-6);
            match load_id {
                Some(load) => ActionOutcome::Keep(ContingencyElementRow {
                    element_type: "load_shed".to_string(),
                    bus_id: i32::try_from(*bus).ok(),
                    load_id: Some(load.id.to_string()),
                    amount_mw: Some(0.0),
                    status_change: true,
                    ..ContingencyElementRow::default()
                }),
                None => ActionOutcome::Fault(ActionFault::NoLoad),
            }
        }
    }
}

fn branch_element(
    from: u32,
    to: u32,
    ckt: &str,
    branch_id: Option<i32>,
    open: bool,
) -> ContingencyElementRow {
    ContingencyElementRow {
        element_type: "branch_outage".to_string(),
        branch_id,
        status_change: open,
        equipment_kind: Some(PSSE_BRANCH_KIND.to_string()),
        equipment_id: Some(equipment_id(from, to, ckt)),
        ..ContingencyElementRow::default()
    }
}

fn parse_action(tokens: &[String]) -> Result<Action, ActionFault> {
    if tokens.is_empty() {
        return Err(ActionFault::BadAction);
    }
    let verb = tokens[0].to_ascii_uppercase();
    let second = tokens.get(1).map(|t| t.to_ascii_uppercase());
    match (verb.as_str(), second.as_deref()) {
        ("DISCONNECT" | "OPEN" | "CLOSE", Some("BRANCH")) => {
            let (from, to, ckt) = branch_endpoints(tokens).ok_or(ActionFault::BadAction)?;
            Ok(Action::Branch {
                from,
                to,
                ckt,
                open: !verb.eq_ignore_ascii_case("CLOSE"),
            })
        }
        ("REMOVE", Some("MACHINE" | "UNIT")) => {
            let (id, bus) = id_from_bus(tokens).ok_or(ActionFault::BadAction)?;
            Ok(Action::Machine { bus, id })
        }
        ("REMOVE", Some("LOAD")) => {
            let (id, bus) = id_from_bus(tokens).ok_or(ActionFault::BadAction)?;
            Ok(Action::Load { bus, id })
        }
        ("SET", Some("BUS")) => parse_shed_bus(tokens),
        _ => Err(ActionFault::UnknownVerb),
    }
}

fn parse_shed_bus(tokens: &[String]) -> Result<Action, ActionFault> {
    // SET BUS <n> LOAD TO <mw> MW
    let bus = tokens
        .get(2)
        .and_then(|t| t.parse::<u32>().ok())
        .filter(|bus| *bus > 0)
        .ok_or(ActionFault::BadAction)?;
    let load = tokens.get(3).map(|t| t.to_ascii_uppercase());
    let to = tokens.get(4).map(|t| t.to_ascii_uppercase());
    if load.as_deref() != Some("LOAD") || to.as_deref() != Some("TO") {
        return Err(ActionFault::UnknownVerb);
    }
    let mw = tokens
        .get(5)
        .and_then(|t| t.parse::<f64>().ok())
        .ok_or(ActionFault::BadAction)?;
    if mw.abs() > 1.0e-6 {
        return Err(ActionFault::LoadSetpoint);
    }
    Ok(Action::ShedBus { bus })
}

fn branch_endpoints(tokens: &[String]) -> Option<(u32, u32, String)> {
    let from = number_after(tokens, "FROM")?;
    let to = number_after(tokens, "TO")?;
    let ckt = token_after_keyword_str(tokens, "CKT")?;
    if ckt.is_empty() {
        return None;
    }
    Some((from, to, ckt))
}

fn number_after(tokens: &[String], keyword: &str) -> Option<u32> {
    let pos = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case(keyword))?;
    for token in tokens.iter().skip(pos + 1) {
        if token.eq_ignore_ascii_case("BUS") {
            continue;
        }
        return token.parse::<u32>().ok().filter(|value| *value > 0);
    }
    None
}

fn id_from_bus(tokens: &[String]) -> Option<(String, u32)> {
    // REMOVE MACHINE|UNIT|LOAD <id> FROM BUS <n>
    let id = tokens.get(2)?.trim().to_string();
    if id.is_empty() || id.eq_ignore_ascii_case("FROM") {
        return None;
    }
    let bus = number_after(tokens, "BUS")?;
    if bus == 0 {
        return None;
    }
    Some((id, bus))
}

fn token_after_keyword_str(tokens: &[String], keyword: &str) -> Option<String> {
    let pos = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case(keyword))?;
    let value = tokens.get(pos + 1)?.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn tokenize(line: &str) -> Vec<String> {
    let bytes = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] == b'/' {
            break;
        }
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            let quote = bytes[i];
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i] != quote {
                i += 1;
            }
            out.push(line[start..i].trim().to_string());
            if i < bytes.len() {
                i += 1;
            }
            continue;
        }
        let start = i;
        while i < bytes.len() && !bytes[i].is_ascii_whitespace() && bytes[i] != b'/' {
            i += 1;
        }
        out.push(line[start..i].to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Branch, Generator, Load, TwoWindingTransformer};

    fn net() -> Network {
        Network {
            branches: vec![
                Branch {
                    i: 2,
                    j: 3,
                    ckt: "1".into(),
                    st: 1,
                    ..Branch::default()
                },
                Branch {
                    i: 2,
                    j: 3,
                    ckt: "B".into(),
                    st: 1,
                    ..Branch::default()
                },
            ],
            transformers: vec![TwoWindingTransformer {
                i: 4,
                j: 5,
                ckt: "1".into(),
                stat: 1,
                ..TwoWindingTransformer::default()
            }],
            transformers_3w: vec![ThreeWindingTransformer {
                bus_h: 6,
                bus_m: 7,
                bus_l: 8,
                ckt: "1".into(),
                stat: 1,
                ..ThreeWindingTransformer::default()
            }],
            generators: vec![
                Generator {
                    i: 1,
                    id: "1".into(),
                    stat: 1,
                    ..Generator::default()
                },
                Generator {
                    i: 9,
                    id: "1".into(),
                    stat: 1,
                    ..Generator::default()
                },
            ],
            loads: vec![
                Load {
                    i: 2,
                    id: "1".into(),
                    status: 1,
                    pl: 40.0,
                    ..Load::default()
                },
                Load {
                    i: 2,
                    id: "2".into(),
                    status: 1,
                    pl: 0.0,
                    ..Load::default()
                },
            ],
            ..empty_network()
        }
    }

    fn empty_network() -> Network {
        Network {
            case_id: crate::models::CaseId::default(),
            buses: Vec::new(),
            loads: Vec::new(),
            fixed_shunts: Vec::new(),
            generators: Vec::new(),
            branches: Vec::new(),
            transformers: Vec::new(),
            transformers_3w: Vec::new(),
            areas: Vec::new(),
            multi_section_lines: Vec::new(),
            dc_lines_2w: Vec::new(),
            dc_converters: Vec::new(),
            zones: Vec::new(),
            owners: Vec::new(),
            facts_devices: Vec::new(),
            switched_shunts: Vec::new(),
            switched_shunt_banks: Vec::new(),
            has_multi_terminal_dc: false,
            dyr_models: Vec::new(),
            dyr_generators: Vec::new(),
        }
    }

    #[test]
    fn resolves_both_dialects_and_drops_incomplete_rows() {
        let text = r#"
CONTINGENCY 'LINE'
 DISCONNECT BRANCH FROM BUS 3 TO BUS 2 CKT 1
END
CONTINGENCY 'XF'
 OPEN BRANCH FROM BUS 5 TO BUS 4 CKT '1'
END
CONTINGENCY 'T3'
 DISCONNECT BRANCH FROM BUS 8 TO BUS 6 CKT 1
END
CONTINGENCY 'LETTER'
 DISCONNECT BRANCH FROM BUS 2 TO BUS 3 CKT B
END
CONTINGENCY 'GEN'
 REMOVE UNIT '1' FROM BUS 1
END
CONTINGENCY 'LD'
 REMOVE LOAD '1' FROM BUS 2
END
CONTINGENCY 'ZERO'
 REMOVE LOAD 2 FROM BUS 2
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
CONTINGENCY 'LINE'
 DISCONNECT BRANCH FROM BUS 2 TO BUS 3 CKT 1
END
CONTINGENCY 'MISS'
 OPEN BRANCH FROM BUS 2 TO BUS 99 CKT 1
END
CONTINGENCY 'SETPOINT'
 SET BUS 2 LOAD TO 5 MW
END
"#;
        let (rows, stats) = import_contingencies(text, &net());
        let ids: Vec<_> = rows.iter().map(|row| row.contingency_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["LINE", "XF", "T3", "LETTER", "GEN", "LD", "SHED", "CLOSE"]
        );
        assert_eq!(stats.empty, 2);
        assert_eq!(stats.duplicate, 1);
        assert_eq!(stats.unresolved, 3);
        assert_eq!(stats.no_machine, 1);
        assert_eq!(stats.no_endpoint, 1);
        assert_eq!(stats.load_setpoint, 1);

        assert_eq!(rows[0].elements[0].branch_id, Some(1));
        assert!(rows[0].elements[0].status_change);
        assert_eq!(rows[0].elements[0].equipment_id.as_deref(), Some("3|2|1"));
        assert_eq!(rows[1].elements[0].branch_id, Some(-1));
        assert_eq!(rows[1].elements[0].equipment_id.as_deref(), Some("5|4|1"));
        assert_eq!(rows[2].elements[0].branch_id, Some(-1));
        assert_eq!(rows[2].elements[0].equipment_id.as_deref(), Some("8|6|1"));
        assert_eq!(rows[3].elements[0].branch_id, Some(2));
        assert_eq!(rows[4].elements[0].element_type, "gen_trip");
        assert_eq!(rows[4].elements[0].bus_id, Some(1));
        assert_eq!(rows[4].elements[0].gen_id.as_deref(), Some("1"));
        assert_eq!(rows[5].elements[0].amount_mw, Some(40.0));
        assert_eq!(rows[5].elements[0].load_id.as_deref(), Some("1"));
        assert_eq!(rows[6].elements[0].amount_mw, Some(0.0));
        assert!(!rows[7].elements[0].status_change);
        assert!(rows.iter().all(|row| row.tpl_category.is_none()));
        assert!(rows.iter().all(|row| row.reserved.is_none()));
    }
}
