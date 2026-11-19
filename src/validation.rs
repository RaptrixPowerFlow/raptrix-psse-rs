// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! MMWG §7.3 conformant power flow data quality checks.
//!
//! This module is **completely opt-in** — it is never called from the default
//! `write_psse_to_rpf()` convert path (speed is paramount in real-time
//! SCADA/EMS environments).  Call [`run_mmwg_checks`] explicitly from the
//! `validate` CLI subcommand or from application code where model quality
//! matters more than throughput.
//!
//! ## Reference
//! Checks are aligned with the MMWG Procedural Manual v4.3, Section 7.3
//! "Power Flow Data Quality Checks".  Each issue is tagged with the
//! applicable subsection identifier (e.g. `"MMWG-7.3.3/zero-x"`).

use std::collections::{HashMap, HashSet};

use crate::models::{BusType, Network};
use crate::transformer_convert::{
    convert_z_to_system, normalize_code, series_z_on_from_tap, winding_taps_from_cw, TransformerId,
};

// ---------------------------------------------------------------------------
// Public API types
// ---------------------------------------------------------------------------

/// Severity of a [`ValidationIssue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Informational — not a conformance problem, but worth knowing.
    Info,
    /// Warning — potential modelling issue; review recommended.
    Warning,
    /// Error — definite conformance violation; model may produce wrong results.
    Error,
}

impl Severity {
    fn label(self) -> &'static str {
        match self {
            Severity::Info => "INFO ",
            Severity::Warning => "WARN ",
            Severity::Error => "ERROR",
        }
    }
}

/// A single issue found during validation.
#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub severity: Severity,
    /// MMWG §7.3.x subsection identifier, e.g. `"MMWG-7.3.3/zero-x"`.
    pub check: &'static str,
    /// Human-readable description.
    pub message: String,
}

/// Aggregated result returned by [`run_mmwg_checks`].
#[derive(Debug, Default)]
pub struct ValidationReport {
    pub issues: Vec<ValidationIssue>,
}

impl ValidationReport {
    pub fn error_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .count()
    }
    pub fn warning_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Warning)
            .count()
    }
    pub fn info_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == Severity::Info)
            .count()
    }
    pub fn is_clean(&self) -> bool {
        self.error_count() == 0
    }

    /// Print a human-readable report to stderr.
    pub fn print_summary(&self) {
        let errors = self.error_count();
        let warnings = self.warning_count();
        let infos = self.info_count();

        eprintln!("\n=== MMWG §7.3 Validation Report ===");
        eprintln!(
            "  {} issue(s): {} error(s)  {} warning(s)  {} info",
            self.issues.len(),
            errors,
            warnings,
            infos
        );

        if self.issues.is_empty() {
            eprintln!("  All checks passed.");
        } else {
            eprintln!();
            for issue in &self.issues {
                eprintln!(
                    "  [{}] {:35}  {}",
                    issue.severity.label(),
                    issue.check,
                    issue.message
                );
            }
        }
        eprintln!();
    }

    /// One JSON object. Messages are escaped. Suitable for a batch screen.
    pub fn to_json(&self) -> String {
        let mut out = format!(
            "{{\"errors\":{},\"warnings\":{},\"infos\":{},\"issues\":[",
            self.error_count(),
            self.warning_count(),
            self.info_count()
        );
        for (index, issue) in self.issues.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let severity = match issue.severity {
                Severity::Error => "error",
                Severity::Warning => "warning",
                Severity::Info => "info",
            };
            out.push_str(&format!(
                "{{\"severity\":\"{severity}\",\"check\":\"{}\",\"message\":\"{}\"}}",
                json_escape(issue.check),
                json_escape(&issue.message)
            ));
        }
        out.push_str("]}");
        out
    }
}

fn json_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run all MMWG §7.3 checks against a parsed [`Network`] and return the
/// aggregated [`ValidationReport`].
///
/// This function has no side effects and does not write any output files.
/// The caller owns all printing/reporting decisions.
pub fn run_mmwg_checks(network: &Network) -> ValidationReport {
    let mut report = ValidationReport::default();
    let r = &mut report;

    check_system(r, network);
    check_buses(r, network);
    check_branches(r, network);
    check_transformers(r, network);
    check_generators(r, network);
    check_loads(r, network);
    check_fixed_shunts(r, network);
    check_switched_shunts(r, network);
    check_areas(r, network);
    check_system_balance(r, network);
    check_published_operating_point(r, network);

    report
}

// ---------------------------------------------------------------------------
// §7.3.1 — System-level checks
// ---------------------------------------------------------------------------

fn check_system(r: &mut ValidationReport, n: &Network) {
    // SBASE must be positive
    if n.case_id.sbase <= 0.0 {
        push(
            r,
            Severity::Error,
            "MMWG-7.3.1/sbase-nonpositive",
            format!(
                "SBASE = {} is non-positive; a positive MVA base is required",
                n.case_id.sbase
            ),
        );
    } else if n.case_id.sbase < 1.0 || n.case_id.sbase > 100_000.0 {
        push(
            r,
            Severity::Warning,
            "MMWG-7.3.1/sbase-unusual",
            format!(
                "SBASE = {} MVA is outside the typical range [1, 100000]; verify case identification record",
                n.case_id.sbase
            ),
        );
    }

    // BASFRQ must be 50 or 60 Hz
    if n.case_id.basfrq != 50.0 && n.case_id.basfrq != 60.0 {
        push(
            r,
            Severity::Warning,
            "MMWG-7.3.1/basfrq-unusual",
            format!(
                "BASFRQ = {} Hz is not 50 or 60 Hz; verify frequency base",
                n.case_id.basfrq
            ),
        );
    }

    // At least one slack bus required
    let slack_count = n.buses.iter().filter(|b| b.ide == BusType::Slack).count();
    if slack_count == 0 {
        push(
            r,
            Severity::Error,
            "MMWG-7.3.1/no-slack",
            "No slack (swing) bus found; at least one bus with IDE=3 is required".to_string(),
        );
    } else if slack_count > 1 {
        push(
            r,
            Severity::Info,
            "MMWG-7.3.1/multi-slack",
            format!(
                "{slack_count} slack buses found; verify that multiple swing buses are intentional"
            ),
        );
    }

    // Case must have at least some buses
    if n.buses.is_empty() {
        push(
            r,
            Severity::Error,
            "MMWG-7.3.1/no-buses",
            "Network contains no buses; RAW file may be empty or malformed".to_string(),
        );
    }
}

// ---------------------------------------------------------------------------
// §7.3.2 — Bus checks
// ---------------------------------------------------------------------------

fn check_buses(r: &mut ValidationReport, n: &Network) {
    let mut seen_ids: HashSet<u32> = HashSet::with_capacity(n.buses.len());
    let owner_ids: HashSet<u32> = n.owners.iter().map(|o| o.i).collect();

    for bus in &n.buses {
        // Duplicate bus number
        if !seen_ids.insert(bus.i) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.2/duplicate-bus-id",
                format!("Duplicate bus number {}", bus.i),
            );
        }

        // BASKV must be positive
        if bus.baskv <= 0.0 {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.2/baskv-nonpositive",
                format!(
                    "Bus {} '{}': BASKV = {} kV is non-positive",
                    bus.i, bus.name, bus.baskv
                ),
            );
        }

        // Voltage magnitude out of realistic range [0.5, 1.5] pu
        if bus.vm < 0.5 || bus.vm > 1.5 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.2/vm-out-of-range",
                format!(
                    "Bus {} '{}': VM = {:.4} pu is outside [0.50, 1.50]; solved case may have diverged",
                    bus.i, bus.name, bus.vm
                ),
            );
        }

        // Voltage limits sanity
        if bus.nvhi > 0.0 && bus.nvlo > 0.0 && bus.nvhi <= bus.nvlo {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.2/vhi-le-vlo",
                format!(
                    "Bus {} '{}': NVHI ({:.3}) ≤ NVLO ({:.3}); voltage limits are inverted",
                    bus.i, bus.name, bus.nvhi, bus.nvlo
                ),
            );
        }
        if bus.evhi > 0.0 && bus.evlo > 0.0 && bus.evhi <= bus.evlo {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.2/evhi-le-evlo",
                format!(
                    "Bus {} '{}': EVHI ({:.3}) ≤ EVLO ({:.3}); emergency voltage limits are inverted",
                    bus.i, bus.name, bus.evhi, bus.evlo
                ),
            );
        }

        if bus.owner != 0 && !owner_ids.contains(&bus.owner) {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.2/undefined-owner",
                format!(
                    "Bus {} '{}': OWNER={} not present in owner table",
                    bus.i, bus.name, bus.owner
                ),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// §7.3.3 — Non-transformer branch checks
// ---------------------------------------------------------------------------

fn check_branches(r: &mut ValidationReport, n: &Network) {
    let bus_ids: HashSet<u32> = n.buses.iter().map(|b| b.i).collect();
    let owner_ids: HashSet<u32> = n.owners.iter().map(|o| o.i).collect();
    let mut zero_x_count = 0usize;
    let mut zero_x_examples: Vec<String> = Vec::new();

    for br in &n.branches {
        // Terminal buses must exist
        if !bus_ids.contains(&br.i) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.3/orphan-branch-from",
                format!(
                    "Branch {}-{} ckt '{}': from-bus {} not in bus table",
                    br.i, br.j, br.ckt, br.i
                ),
            );
        }
        if !bus_ids.contains(&br.j) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.3/orphan-branch-to",
                format!(
                    "Branch {}-{} ckt '{}': to-bus {} not in bus table",
                    br.i, br.j, br.ckt, br.j
                ),
            );
        }

        // Zero reactance — potential numerical singularity
        if br.x == 0.0 {
            zero_x_count += 1;
            if zero_x_examples.len() < 5 {
                zero_x_examples.push(format!("{}-{} ckt '{}'", br.i, br.j, br.ckt));
            }
        }

        // Very high R/X ratio (> 5) is unusual for transmission lines
        if br.x.abs() > 1e-9 && (br.r / br.x).abs() > 5.0 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.3/high-rx-ratio",
                format!(
                    "Branch {}-{} ckt '{}': R/X = {:.2}; unusually high for transmission",
                    br.i,
                    br.j,
                    br.ckt,
                    br.r / br.x
                ),
            );
        }

        // Negative ratings
        if br.ratea < 0.0 || br.rateb < 0.0 || br.ratec < 0.0 {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.3/negative-rating",
                format!(
                    "Branch {}-{} ckt '{}': negative thermal rating (RATEA={}, RATEB={}, RATEC={})",
                    br.i, br.j, br.ckt, br.ratea, br.rateb, br.ratec
                ),
            );
        }

        if br.o1 != 0 && !owner_ids.contains(&br.o1) {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.3/undefined-owner",
                format!(
                    "Branch {}-{} ckt '{}': O1={} not present in owner table",
                    br.i, br.j, br.ckt, br.o1
                ),
            );
        }
    }

    // Summarize zero-X rather than flooding large EI cases
    if zero_x_count > 0 {
        let examples = zero_x_examples.join(", ");
        let tail = if zero_x_count > 5 {
            format!(", … ({} total)", zero_x_count)
        } else {
            String::new()
        };
        push(
            r,
            Severity::Warning,
            "MMWG-7.3.3/zero-x",
            format!(
                "Branch(es) with X=0: {}{} — may cause numerical issues in full Newton-Raphson",
                examples, tail
            ),
        );
    }
}

// ---------------------------------------------------------------------------
// §7.3.4 — Transformer checks
// ---------------------------------------------------------------------------

fn check_transformers(r: &mut ValidationReport, n: &Network) {
    let bus_ids: HashSet<u32> = n.buses.iter().map(|b| b.i).collect();

    for tx in &n.transformers {
        // Terminal buses must exist
        if !bus_ids.contains(&tx.i) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.4/orphan-tx-from",
                format!(
                    "Transformer {}-{} ckt '{}': from-bus {} not in bus table",
                    tx.i, tx.j, tx.ckt, tx.i
                ),
            );
        }
        if !bus_ids.contains(&tx.j) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.4/orphan-tx-to",
                format!(
                    "Transformer {}-{} ckt '{}': to-bus {} not in bus table",
                    tx.i, tx.j, tx.ckt, tx.j
                ),
            );
        }

        // Zero leakage reactance
        if tx.x12 == 0.0 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.4/zero-x12",
                format!(
                    "Transformer {}-{} ckt '{}': X1-2 = 0; potential numerical singularity",
                    tx.i, tx.j, tx.ckt
                ),
            );
        }

        // Non-positive SBASE1-2
        if tx.sbase12 <= 0.0 {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.4/sbase12-nonpositive",
                format!(
                    "Transformer {}-{} ckt '{}': SBASE1-2 = {} is non-positive",
                    tx.i, tx.j, tx.ckt, tx.sbase12
                ),
            );
        }

        // Off-nominal turns ratio out of reasonable range (CW=1: WINDV is pu of BASKV)
        if normalize_code(tx.cw) == 1 && (tx.windv1 < 0.5 || tx.windv1 > 2.0) {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.4/windv1-range",
                format!(
                    "Transformer {}-{} ckt '{}': WINDV1 = {:.4} is outside [0.5, 2.0]",
                    tx.i, tx.j, tx.ckt, tx.windv1
                ),
            );
        }
        if normalize_code(tx.cw) == 1 && (tx.windv2 < 0.5 || tx.windv2 > 2.0) {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.4/windv2-range",
                format!(
                    "Transformer {}-{} ckt '{}': WINDV2 = {:.4} is outside [0.5, 2.0]",
                    tx.i, tx.j, tx.ckt, tx.windv2
                ),
            );
        }

        // Phase shift > ±30° is highly unusual
        if tx.ang1.abs() > 30.0 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.4/phase-shift-large",
                format!(
                    "Transformer {}-{} ckt '{}': ANG1 = {:.2}° exceeds ±30°; verify PST settings",
                    tx.i, tx.j, tx.ckt, tx.ang1
                ),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// §7.3.5 — Generator checks
// ---------------------------------------------------------------------------

fn check_generators(r: &mut ValidationReport, n: &Network) {
    let bus_ids: HashSet<u32> = n.buses.iter().map(|b| b.i).collect();
    let owner_ids: HashSet<u32> = n.owners.iter().map(|o| o.i).collect();
    let mut seen: HashSet<(u32, &str)> = HashSet::new();

    for machine in &n.generators {
        // Connected bus must exist
        if !bus_ids.contains(&machine.i) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.5/orphan-gen",
                format!(
                    "Generator bus {} id '{}': bus not in bus table",
                    machine.i, machine.id
                ),
            );
        }

        // Duplicate I/ID
        if !seen.insert((machine.i, machine.id.as_ref())) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.5/duplicate-gen-id",
                format!("Duplicate generator: bus {} id '{}'", machine.i, machine.id),
            );
        }

        // MBASE must be positive
        if machine.mbase <= 0.0 {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.5/mbase-nonpositive",
                format!(
                    "Generator bus {} id '{}': MBASE = {} is non-positive",
                    machine.i, machine.id, machine.mbase
                ),
            );
        }

        // PT must be ≥ PB
        if machine.pt < machine.pb {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.5/pt-lt-pb",
                format!(
                    "Generator bus {} id '{}': PT ({}) < PB ({}); active power limits are inverted",
                    machine.i, machine.id, machine.pt, machine.pb
                ),
            );
        }

        // QT must be ≥ QB
        if machine.qt < machine.qb {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.5/qt-lt-qb",
                format!(
                    "Generator bus {} id '{}': QT ({}) < QB ({}); reactive power limits are inverted",
                    machine.i, machine.id, machine.qt, machine.qb
                ),
            );
        }

        // Voltage setpoint out of range [0.5, 1.5]
        if machine.vs < 0.5 || machine.vs > 1.5 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.5/vs-out-of-range",
                format!(
                    "Generator bus {} id '{}': VS = {:.4} pu is outside [0.50, 1.50]",
                    machine.i, machine.id, machine.vs
                ),
            );
        }

        // PG outside [PB, PT] when in service
        if machine.stat == 1 && machine.pt > machine.pb {
            if machine.pg > machine.pt + 1.0 {
                push(
                    r,
                    Severity::Warning,
                    "MMWG-7.3.5/pg-above-pt",
                    format!(
                        "Generator bus {} id '{}': PG ({:.1}) > PT ({:.1}); dispatch exceeds max MW",
                        machine.i, machine.id, machine.pg, machine.pt
                    ),
                );
            }
            if machine.pg < machine.pb - 1.0 {
                push(
                    r,
                    Severity::Warning,
                    "MMWG-7.3.5/pg-below-pb",
                    format!(
                        "Generator bus {} id '{}': PG ({:.1}) < PB ({:.1}); dispatch below min MW",
                        machine.i, machine.id, machine.pg, machine.pb
                    ),
                );
            }
        }

        if machine.o1 != 0 && !owner_ids.contains(&machine.o1) {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.5/undefined-owner",
                format!(
                    "Generator bus {} id '{}': O1={} not present in owner table",
                    machine.i, machine.id, machine.o1
                ),
            );
        }

        if machine.wmod != 0 && machine.stat == 1 && machine.pt <= 0.0 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.5/ibr-without-pmax",
                format!(
                    "Generator bus {} id '{}': WMOD={} but PT={} MW; IBR rows should have nonzero p_max",
                    machine.i, machine.id, machine.wmod, machine.pt
                ),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// §7.3.6 — Load checks
// ---------------------------------------------------------------------------

fn check_loads(r: &mut ValidationReport, n: &Network) {
    let bus_ids: HashSet<u32> = n.buses.iter().map(|b| b.i).collect();
    let in_service_loads = n.loads.iter().filter(|l| l.status == 1).count();

    for load in &n.loads {
        if !bus_ids.contains(&load.i) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.6/orphan-load",
                format!("Load bus {} id '{}': bus not in bus table", load.i, load.id),
            );
        }

        // Constant-power component — negative MW is unusual (generation via load record)
        let total_p = load.pl + load.ip + load.yp;
        if load.status == 1 && total_p < -1.0 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.6/negative-load-mw",
                format!(
                    "Load bus {} id '{}': total P = {:.1} MW is negative; intentional if modeling a generator as load",
                    load.i, load.id, total_p
                ),
            );
        }
    }

    if in_service_loads == 0 && !n.buses.is_empty() {
        push(
            r,
            Severity::Warning,
            "MMWG-7.3.6/no-in-service-loads",
            "No in-service loads found; model may represent a generation-only network".to_string(),
        );
    }
}

// ---------------------------------------------------------------------------
// §7.3.7 — Fixed shunt checks
// ---------------------------------------------------------------------------

fn check_fixed_shunts(r: &mut ValidationReport, n: &Network) {
    let bus_ids: HashSet<u32> = n.buses.iter().map(|b| b.i).collect();

    for sh in &n.fixed_shunts {
        if !bus_ids.contains(&sh.i) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.7/orphan-fixed-shunt",
                format!(
                    "Fixed shunt bus {} id '{}': bus not in bus table",
                    sh.i, sh.id
                ),
            );
        }

        // Very large shunt values (> 10 000 MVAr) are likely data entry errors
        if sh.bl.abs() > 10_000.0 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.7/large-bl",
                format!(
                    "Fixed shunt bus {} id '{}': |BL| = {:.1} MVAr is extremely large; verify scaling",
                    sh.i, sh.id, sh.bl
                ),
            );
        }
        if sh.gl.abs() > 10_000.0 {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.7/large-gl",
                format!(
                    "Fixed shunt bus {} id '{}': |GL| = {:.1} MW is extremely large; verify scaling",
                    sh.i, sh.id, sh.gl
                ),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// §7.3.8 — Switched shunt checks
// ---------------------------------------------------------------------------

fn check_switched_shunts(r: &mut ValidationReport, n: &Network) {
    let bus_ids: HashSet<u32> = n.buses.iter().map(|b| b.i).collect();

    for ss in &n.switched_shunts {
        if !bus_ids.contains(&ss.i) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.8/orphan-switched-shunt",
                format!("Switched shunt bus {}: bus not in bus table", ss.i),
            );
        }

        // Voltage window inverted
        if ss.stat == 1 && ss.vswhi > 0.0 && ss.vswlo > 0.0 && ss.vswhi <= ss.vswlo {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.8/vswhi-le-vswlo",
                format!(
                    "Switched shunt bus {}: VSWHI ({:.3}) ≤ VSWLO ({:.3}); control window is inverted",
                    ss.i, ss.vswhi, ss.vswlo
                ),
            );
        }

        // Empty step list for a non-locked shunt
        if ss.modsw != 0 && ss.stat == 1 && ss.steps.is_empty() {
            push(
                r,
                Severity::Warning,
                "MMWG-7.3.8/no-steps",
                format!(
                    "Switched shunt bus {}: MODSW={} but step list is empty; shunt cannot switch",
                    ss.i, ss.modsw
                ),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// §7.3.9 — Area checks
// ---------------------------------------------------------------------------

fn check_areas(r: &mut ValidationReport, n: &Network) {
    let bus_ids: HashSet<u32> = n.buses.iter().map(|b| b.i).collect();
    let mut seen_areas: HashSet<u32> = HashSet::new();

    // Collect the set of area numbers actually referenced by buses
    let bus_areas: HashSet<u32> = n.buses.iter().map(|b| b.area).collect();

    for area in &n.areas {
        if !seen_areas.insert(area.i) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.9/duplicate-area",
                format!("Duplicate area number {}", area.i),
            );
        }

        // Swing bus must exist (ISW = 0 means uncontrolled, which is allowed)
        if area.isw != 0 && !bus_ids.contains(&area.isw) {
            push(
                r,
                Severity::Error,
                "MMWG-7.3.9/area-swing-missing",
                format!(
                    "Area {} '{}': swing bus {} not in bus table",
                    area.i, area.arnam, area.isw
                ),
            );
        }
    }

    // Buses reference area numbers that don't appear in the area table
    let defined_areas: HashSet<u32> = n.areas.iter().map(|a| a.i).collect();
    let orphan_areas: Vec<u32> = bus_areas
        .iter()
        .filter(|&&a| a != 0 && !defined_areas.contains(&a))
        .copied()
        .collect();
    if !orphan_areas.is_empty() {
        let mut sorted = orphan_areas;
        sorted.sort_unstable();
        push(
            r,
            Severity::Warning,
            "MMWG-7.3.9/undefined-area",
            format!("Bus(es) reference area(s) not in area table: {:?}", sorted),
        );
    }
}

// ---------------------------------------------------------------------------
// §7.3.10 — System-level balance check
// ---------------------------------------------------------------------------

fn check_system_balance(r: &mut ValidationReport, n: &Network) {
    let total_gen_mw: f64 = n
        .generators
        .iter()
        .filter(|g| g.stat == 1)
        .map(|g| g.pg)
        .sum();

    let total_load_mw: f64 = n
        .loads
        .iter()
        .filter(|l| l.status == 1)
        .map(|l| l.pl + l.ip + l.yp)
        .sum();

    let total_shunt_mw: f64 = n
        .fixed_shunts
        .iter()
        .filter(|s| s.status == 1)
        .map(|s| s.gl)
        .sum::<f64>()
        + n.buses.iter().map(|b| b.gl).sum::<f64>();

    let net_imbalance = (total_gen_mw - total_load_mw - total_shunt_mw).abs();

    // For a solved case with losses the imbalance will be the I²R losses
    // (typically < 5% of load).  Flag if > 10% of total load (likely unsolved
    // flat-start or bad data).
    let threshold_mw = (total_load_mw.abs() * 0.10).max(100.0);
    if total_gen_mw > 0.0 && total_load_mw > 0.0 && net_imbalance > threshold_mw {
        push(
            r,
            Severity::Warning,
            "MMWG-7.3.10/gen-load-imbalance",
            format!(
                "Gen ({:.0} MW) − Load ({:.0} MW) − Shunt ({:.0} MW) = {:.0} MW net imbalance \
                 exceeds 10% of load; model may be unsolved or missing swing-bus contribution",
                total_gen_mw, total_load_mw, total_shunt_mw, net_imbalance
            ),
        );
    }

    // Total generation in a real network must be > 0
    if total_gen_mw <= 0.0 && !n.generators.is_empty() {
        push(
            r,
            Severity::Warning,
            "MMWG-7.3.10/zero-total-gen",
            format!(
                "Total in-service generation = {:.1} MW; all generators may be offline or PG=0",
                total_gen_mw
            ),
        );
    }

    // System summary info
    push(
        r,
        Severity::Info,
        "MMWG-7.3.10/summary",
        format!(
            "buses={} branches={} transformers={} generators={} loads={} \
             gen_mw={:.0} load_mw={:.0} areas={} zones={} owners={}",
            n.buses.len(),
            n.branches.len(),
            n.transformers.len(),
            n.generators.len(),
            n.loads.len(),
            total_gen_mw,
            total_load_mw,
            n.areas.len(),
            n.zones.len(),
            n.owners.len()
        ),
    );
}

// ---------------------------------------------------------------------------
// Published operating point — not an MMWG subsection. A correct reader still
// cannot turn a 25° angle across X = 0.0012 pu into an 800 MVA flow.
// ---------------------------------------------------------------------------

struct FlowHit {
    angle_deg: f64,
    implied_mw: f64,
    message: String,
}

/// Counts and the worst sample. `validate` records the issues. `convert`
/// prints one line and does not change the RPF.
#[derive(Debug, Clone)]
pub struct PublishedStateScreen {
    pub infeasible_branches: usize,
    pub infeasible_transformers: usize,
    pub isolated_generators: usize,
    pub isolated_ireg_targets: usize,
    pub max_implied_mw: f64,
    pub max_angle_deg: f64,
    pub worst: String,
}

impl PublishedStateScreen {
    pub fn has_infeasible_flow(&self) -> bool {
        self.infeasible_branches + self.infeasible_transformers > 0
    }
}

pub fn screen_published_state(network: &Network) -> PublishedStateScreen {
    let mut screen = PublishedStateScreen {
        infeasible_branches: 0,
        infeasible_transformers: 0,
        isolated_generators: 0,
        isolated_ireg_targets: 0,
        max_implied_mw: 0.0,
        max_angle_deg: 0.0,
        worst: String::new(),
    };
    let buses = bus_index(network);
    if !deck_looks_solved(network) {
        count_isolated_controls(network, &buses, &mut screen);
        return screen;
    }
    let star_buses = star_bus_set(network);
    let sbase = if network.case_id.sbase.abs() > 1.0e-9 {
        network.case_id.sbase
    } else {
        100.0
    };
    let mut worst_mw = 0.0;
    for branch in &network.branches {
        if branch.st == 0 {
            continue;
        }
        let Some(hit) = branch_flow_hit(network, &buses, branch, sbase) else {
            continue;
        };
        screen.infeasible_branches += 1;
        note_worst(&mut screen, &mut worst_mw, &hit);
    }
    for tx in &network.transformers {
        if tx.stat == 0 || is_star_leg(tx, &star_buses) {
            continue;
        }
        let Some(hit) = transformer_flow_hit(network, &buses, tx, sbase) else {
            continue;
        };
        screen.infeasible_transformers += 1;
        note_worst(&mut screen, &mut worst_mw, &hit);
    }
    for tx in &network.transformers_3w {
        if tx.stat == 0 {
            continue;
        }
        for (from, to, x, shift) in [
            (tx.bus_h, tx.bus_m, tx.x_hm, tx.phase_shift_deg),
            (tx.bus_h, tx.bus_l, tx.x_hl, tx.phase_shift_deg),
            (tx.bus_m, tx.bus_l, tx.x_ml, 0.0),
        ] {
            let Some(hit) = pair_flow_hit(
                &buses,
                sbase,
                from,
                to,
                x,
                1.0,
                shift,
                tx.rate_a_mva,
                &format!("3w {}-{} ckt '{}'", from, to, tx.ckt),
                network,
            ) else {
                continue;
            };
            screen.infeasible_transformers += 1;
            note_worst(&mut screen, &mut worst_mw, &hit);
        }
    }
    count_isolated_controls(network, &buses, &mut screen);
    screen
}

fn check_published_operating_point(r: &mut ValidationReport, network: &Network) {
    let buses = bus_index(network);
    if !deck_looks_solved(network) {
        check_isolated_only(r, network, &buses);
        return;
    }
    let star_buses = star_bus_set(network);
    let sbase = if network.case_id.sbase.abs() > 1.0e-9 {
        network.case_id.sbase
    } else {
        100.0
    };
    let mut branch_hits = 0usize;
    let mut tx_hits = 0usize;
    let mut shown = 0usize;
    let mut push_hit = |kind: &'static str, hit: FlowHit, hits: &mut usize| {
        *hits += 1;
        if shown >= 16 {
            return;
        }
        shown += 1;
        push(r, Severity::Error, kind, hit.message);
    };
    for branch in &network.branches {
        if branch.st == 0 {
            continue;
        }
        if let Some(hit) = branch_flow_hit(network, &buses, branch, sbase) {
            push_hit("MMWG-7.3.3/infeasible-published-flow", hit, &mut branch_hits);
        }
    }
    for tx in &network.transformers {
        if tx.stat == 0 || is_star_leg(tx, &star_buses) {
            continue;
        }
        if let Some(hit) = transformer_flow_hit(network, &buses, tx, sbase) {
            push_hit("MMWG-7.3.4/infeasible-published-tx-flow", hit, &mut tx_hits);
        }
    }
    for tx in &network.transformers_3w {
        if tx.stat == 0 {
            continue;
        }
        for (from, to, x, shift) in [
            (tx.bus_h, tx.bus_m, tx.x_hm, tx.phase_shift_deg),
            (tx.bus_h, tx.bus_l, tx.x_hl, tx.phase_shift_deg),
            (tx.bus_m, tx.bus_l, tx.x_ml, 0.0),
        ] {
            if let Some(hit) = pair_flow_hit(
                &buses,
                sbase,
                from,
                to,
                x,
                1.0,
                shift,
                tx.rate_a_mva,
                &format!("3w {}-{} ckt '{}'", from, to, tx.ckt),
                network,
            ) {
                push_hit("MMWG-7.3.4/infeasible-published-tx-flow", hit, &mut tx_hits);
            }
        }
    }
    let total = branch_hits + tx_hits;
    if total > shown {
        push(
            r,
            Severity::Error,
            "MMWG-7.3.3/infeasible-published-flow",
            format!(
                "{total} in-service branches or transformers publish an infeasible flow; {shown} listed"
            ),
        );
    }
    check_isolated_only(r, network, &buses);
}

fn bus_is_disconnected(buses: &HashMap<u32, usize>, network: &Network, id: u32) -> bool {
    buses
        .get(&id)
        .and_then(|&idx| network.buses.get(idx))
        .is_some_and(|bus| bus.disconnected)
}

fn check_isolated_only(
    r: &mut ValidationReport,
    network: &Network,
    buses: &HashMap<u32, usize>,
) {
    let mut gens = 0usize;
    let mut ireg = 0usize;
    let mut shown = 0usize;
    for generator in &network.generators {
        if generator.stat == 0 {
            continue;
        }
        if bus_is_disconnected(buses, network, generator.i) {
            gens += 1;
            if shown < 8 {
                shown += 1;
                push(
                    r,
                    Severity::Warning,
                    "MMWG-7.3.5/gen-on-isolated-bus",
                    format!(
                        "In-service generator bus {} id '{}' sits on IDE=4 (disconnected)",
                        generator.i, generator.id
                    ),
                );
            }
        }
        if generator.ireg != 0
            && generator.ireg != generator.i
            && bus_is_disconnected(buses, network, generator.ireg)
        {
            ireg += 1;
            if shown < 8 {
                shown += 1;
                push(
                    r,
                    Severity::Warning,
                    "MMWG-7.3.5/ireg-target-isolated",
                    format!(
                        "Generator bus {} regulates disconnected bus {}",
                        generator.i, generator.ireg
                    ),
                );
            }
        }
    }
    if gens + ireg > shown {
        push(
            r,
            Severity::Warning,
            "MMWG-7.3.5/gen-on-isolated-bus",
            format!(
                "{gens} in-service generators sit on IDE=4 buses and {ireg} regulate an IDE=4 bus"
            ),
        );
    }
}

fn count_isolated_controls(
    network: &Network,
    buses: &HashMap<u32, usize>,
    screen: &mut PublishedStateScreen,
) {
    for generator in &network.generators {
        if generator.stat == 0 {
            continue;
        }
        if bus_is_disconnected(buses, network, generator.i) {
            screen.isolated_generators += 1;
        }
        if generator.ireg != 0
            && generator.ireg != generator.i
            && bus_is_disconnected(buses, network, generator.ireg)
        {
            screen.isolated_ireg_targets += 1;
        }
    }
}

fn deck_looks_solved(network: &Network) -> bool {
    let n = network.buses.len().max(1) as f64;
    let angled = network
        .buses
        .iter()
        .filter(|bus| bus.va.abs() > 1.0e-4)
        .count() as f64;
    let nonunity = network
        .buses
        .iter()
        .filter(|bus| (bus.vm - 1.0).abs() > 1.0e-3)
        .count() as f64;
    angled / n > 0.25 || nonunity / n > 0.25
}

fn bus_index(network: &Network) -> HashMap<u32, usize> {
    network
        .buses
        .iter()
        .enumerate()
        .map(|(index, bus)| (bus.i, index))
        .collect()
}

fn energized<'a>(buses: &'a HashMap<u32, usize>, network: &'a Network, id: u32) -> Option<&'a crate::models::Bus> {
    let index = *buses.get(&id)?;
    let bus = network.buses.get(index)?;
    if bus.disconnected || !(bus.vm > 0.0) || !bus.vm.is_finite() || !bus.va.is_finite() {
        return None;
    }
    Some(bus)
}

fn wrap_deg(mut degrees: f64) -> f64 {
    while degrees > 180.0 {
        degrees -= 360.0;
    }
    while degrees < -180.0 {
        degrees += 360.0;
    }
    degrees.abs()
}

fn flow_is_infeasible(rate_mva: f64, implied_mw: f64, angle_deg: f64) -> bool {
    if angle_deg > 80.0 {
        return true;
    }
    if rate_mva > 0.0 {
        implied_mw > (5.0 * rate_mva).max(2500.0)
    } else {
        implied_mw > 5000.0
    }
}

#[allow(clippy::too_many_arguments)]
fn pair_flow_hit(
    buses: &HashMap<u32, usize>,
    network_sbase: f64,
    from: u32,
    to: u32,
    x_pu: f64,
    tap_ratio: f64,
    phase_deg: f64,
    rate_mva: f64,
    label: &str,
    network: &Network,
) -> Option<FlowHit> {
    if x_pu.abs() <= 1.0e-9 {
        return None;
    }
    let from_bus = energized(buses, network, from)?;
    let to_bus = energized(buses, network, to)?;
    let angle = wrap_deg(from_bus.va - to_bus.va - phase_deg);
    let tap = if tap_ratio.is_finite() && tap_ratio.abs() > 1.0e-6 {
        tap_ratio.abs()
    } else {
        1.0
    };
    let implied =
        network_sbase * (from_bus.vm / tap) * to_bus.vm * angle.to_radians().sin() / x_pu.abs();
    if !implied.is_finite() || !flow_is_infeasible(rate_mva, implied, angle) {
        return None;
    }
    Some(FlowHit {
        angle_deg: angle,
        implied_mw: implied,
        message: format!(
            "{label}: X={x_pu:.6} pu, |dθ|={angle:.2} deg, implied {implied:.0} MW, RATEA={rate_mva:.0} MVA. Published voltage is not a feasible flow."
        ),
    })
}

fn branch_flow_hit(
    network: &Network,
    buses: &HashMap<u32, usize>,
    branch: &crate::models::Branch,
    sbase: f64,
) -> Option<FlowHit> {
    pair_flow_hit(
        buses,
        sbase,
        branch.i,
        branch.j,
        branch.x,
        1.0,
        0.0,
        branch.ratea,
        &format!("Branch {}-{} ckt '{}'", branch.i, branch.j, branch.ckt),
        network,
    )
}

fn transformer_flow_hit(
    network: &Network,
    buses: &HashMap<u32, usize>,
    tx: &crate::models::TwoWindingTransformer,
    sbase: f64,
) -> Option<FlowHit> {
    let from_bus = energized(buses, network, tx.i)?;
    let to_bus = energized(buses, network, tx.j)?;
    let id = TransformerId {
        i: tx.i,
        j: tx.j,
        k: 0,
    };
    let (r_sys, x_sys) = convert_z_to_system(id, tx.cz, tx.r12, tx.x12, tx.sbase12, sbase).ok()?;
    let (x_eff, tap) = match winding_taps_from_cw(
        id,
        tx.cw,
        tx.windv1,
        tx.windv2,
        tx.nomv1,
        tx.nomv2,
        from_bus.baskv,
        to_bus.baskv,
    ) {
        Ok(taps) => {
            let (_r_eff, x_eff) = series_z_on_from_tap(r_sys, x_sys, taps.t2);
            (x_eff, taps.from_tap)
        }
        Err(_) => (x_sys, 1.0),
    };
    pair_flow_hit(
        buses,
        sbase,
        tx.i,
        tx.j,
        x_eff,
        tap,
        tx.ang1,
        tx.rata1,
        &format!("Transformer {}-{} ckt '{}'", tx.i, tx.j, tx.ckt),
        network,
    )
}

fn note_worst(screen: &mut PublishedStateScreen, worst_mw: &mut f64, hit: &FlowHit) {
    if hit.angle_deg > screen.max_angle_deg {
        screen.max_angle_deg = hit.angle_deg;
    }
    if hit.implied_mw > *worst_mw {
        *worst_mw = hit.implied_mw;
        screen.max_implied_mw = hit.implied_mw;
        screen.worst = hit.message.clone();
    }
}

fn star_bus_set(network: &Network) -> HashSet<u32> {
    network
        .transformers_3w
        .iter()
        .map(|tx| tx.star_bus_id)
        .collect()
}

fn is_star_leg(tx: &crate::models::TwoWindingTransformer, star_buses: &HashSet<u32>) -> bool {
    // Star legs are the expanded 3W rows. Their pairwise X is also on
    // transformers_3w, so counting both double-reports the same winding.
    star_buses.contains(&tx.i)
        || star_buses.contains(&tx.j)
        || (tx.ckt.starts_with('S') && (tx.i >= 10_000_000 || tx.j >= 10_000_000))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn push(r: &mut ValidationReport, severity: Severity, check: &'static str, message: String) {
    r.issues.push(ValidationIssue {
        severity,
        check,
        message,
    });
}
