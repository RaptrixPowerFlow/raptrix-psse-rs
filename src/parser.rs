// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! PSS/E `.raw` and `.dyr` parser — versions v23 through v35+.
//!
//! # Design
//! * **State-machine driven**: a single pass over the file tracks which PSS/E
//!   data section is active.  Section transitions are detected from the
//!   `0 / END OF X DATA, BEGIN Y DATA` comment hints, with a version-aware
//!   default ordering as a fallback.
//! * **Version-aware field offsets**: PSS/E v35 inserts extra fields in
//!   several records (BRANCH NAME, GENERATOR NREG, SWITCHED SHUNT NAME/NREG).
//!   A `VersionOffsets` struct captures all affected indices.
//! * **Fortran double parsing**: handles `D`-exponent notation (`1.5D-3`)
//!   and bare implicit-exponent (`1.5-3 → 1.5e-3`) used by some exporters.
//! * **Quote-aware tokeniser**: bus names may contain spaces; quoted strings
//!   are not split at internal commas or spaces.
//! * **3-winding transformer star expansion**: creates a fictitious star bus
//!   and three 2-winding legs, matching the C++ solver approach.
//! * **DYR parser**: preserves all numeric dynamic model records and extracts
//!   synchronous-machine parameters used by the generator table.

use std::{
    fs,
    io::{self, BufRead},
    path::Path,
};

use anyhow::{Context, Result};

use crate::models::{
    Area, Branch, Bus, BusType, CaseId, DcLine2W, DyrGeneratorData, DyrModelData, FactsDeviceRaw,
    FixedShunt, Generator, Load, MultiSectionLine, Network, Owner, SwitchedShunt,
    ThreeWindingTransformer, TwoWindingTransformer, Zone,
};

// ---------------------------------------------------------------------------
// Parse state machine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseState {
    Header,
    SystemWide, // v35 SYSTEM-WIDE DATA section (skipped)
    Bus,
    Load,
    FixedShunt,
    Generator,
    Branch,
    SystemSwitchingDevice, // v35 SYSTEM SWITCHING DEVICE section (skipped)
    Transformer,
    Area,
    TwoTerminalDc,
    VscDc,
    ImpedanceCorrection,
    MultiTerminalDc,
    MultiSectionLine,
    Zone,
    InterAreaTransfer,
    Owner,
    Facts,
    SwitchedShunt,
    GneDevice,
    InductionMachine,
    Done,
}

/// Version-aware field index offsets for records whose layout differs between
/// PSS/E v33 and v35.
struct VersionOffsets {
    // ---- BRANCH ----
    /// Index of STATUS in a BRANCH record.
    pub branch_status_idx: usize,
    /// Index of RATEA in a BRANCH record.
    pub branch_ratea_idx: usize,
    /// Index of GI in a BRANCH record.
    pub branch_gi_idx: usize,
    // ---- GENERATOR ----
    /// Index of MBASE in a GENERATOR record.
    pub gen_mbase_idx: usize,
    /// Index of ZR in a GENERATOR record.
    pub gen_zr_idx: usize,
    /// Index of STAT in a GENERATOR record.
    pub gen_stat_idx: usize,
    /// Index of RMPCT in a GENERATOR record.
    pub gen_rmpct_idx: usize,
    /// Index of PT in a GENERATOR record.
    pub gen_pt_idx: usize,
    /// Index of PB in a GENERATOR record.
    pub gen_pb_idx: usize,
    /// Index of O1 in a GENERATOR record.
    pub gen_o1_idx: usize,
    // ---- SWITCHED SHUNT ----
    /// Index of MODSW in a SWITCHED SHUNT record.
    pub sw_modsw_idx: usize,
    /// Index of ADJM in a SWITCHED SHUNT record.
    pub sw_adjm_idx: usize,
    /// Index of STAT in a SWITCHED SHUNT record.
    pub sw_stat_idx: usize,
    /// Index of VSWHI in a SWITCHED SHUNT record.
    pub sw_vswhi_idx: usize,
    /// Index of VSWLO in a SWITCHED SHUNT record.
    pub sw_vswlo_idx: usize,
    /// Index of SWREM/SWREG in a SWITCHED SHUNT record.
    pub sw_swreg_idx: usize,
    /// Index of RMPCT in a SWITCHED SHUNT record.
    pub sw_rmpct_idx: usize,
    /// Index of RMIDNT in a SWITCHED SHUNT record.
    pub sw_rmidnt_idx: usize,
    /// Index of BINIT in a SWITCHED SHUNT record.
    pub sw_binit_idx: usize,
    /// Index of the first N/B pair in a SWITCHED SHUNT record.
    pub sw_pairs_start: usize,
}

fn version_offsets(psse_version: u32) -> VersionOffsets {
    if psse_version >= 35 {
        // v35 BRANCH: NAME inserted at idx 6 → RATEA at 7, STATUS at 23
        // v35 GENERATOR: NREG inserted at idx 8 → MBASE shifts to 9, STAT→15, PT→17, PB→18
        // v35 SWITCHED SHUNT: NAME at 1 → MODSW→2, ADJM→3, STAT→4, VSWHI→5, VSWLO→6,
        //   SWREG→7, NREG at 8, RMPCT→9, RMIDNT→10, BINIT→11, extra flag at 12, pairs start 13
        VersionOffsets {
            branch_status_idx: 23,
            branch_ratea_idx: 7,
            branch_gi_idx: 19,
            gen_mbase_idx: 9,
            gen_zr_idx: 10,
            gen_stat_idx: 15,
            gen_rmpct_idx: 16,
            gen_pt_idx: 17,
            gen_pb_idx: 18,
            gen_o1_idx: 19,
            sw_modsw_idx: 2,
            sw_adjm_idx: 3,
            sw_stat_idx: 4,
            sw_vswhi_idx: 5,
            sw_vswlo_idx: 6,
            sw_swreg_idx: 7,
            sw_rmpct_idx: 9,
            sw_rmidnt_idx: 10,
            sw_binit_idx: 11,
            sw_pairs_start: 13,
        }
    } else {
        // v23–v34 (v33 is the most common)
        VersionOffsets {
            branch_status_idx: 13,
            branch_ratea_idx: 6,
            branch_gi_idx: 9,
            gen_mbase_idx: 8,
            gen_zr_idx: 9,
            gen_stat_idx: 14,
            gen_rmpct_idx: 15,
            gen_pt_idx: 16,
            gen_pb_idx: 17,
            gen_o1_idx: 18,
            sw_modsw_idx: 1,
            sw_adjm_idx: 2,
            sw_stat_idx: 3,
            sw_vswhi_idx: 4,
            sw_vswlo_idx: 5,
            sw_swreg_idx: 6,
            sw_rmpct_idx: 7,
            sw_rmidnt_idx: 8,
            sw_binit_idx: 9,
            sw_pairs_start: 10,
        }
    }
}

/// Default section ordering, used when the line comment provides no hint.
fn default_next_state(state: ParseState, version: u32) -> ParseState {
    match state {
        ParseState::SystemWide => ParseState::Bus,
        ParseState::Bus => ParseState::Load,
        ParseState::Load => ParseState::FixedShunt,
        ParseState::FixedShunt => ParseState::Generator,
        ParseState::Generator => ParseState::Branch,
        ParseState::Branch => {
            if version >= 35 {
                ParseState::SystemSwitchingDevice
            } else {
                ParseState::Transformer
            }
        }
        ParseState::SystemSwitchingDevice => ParseState::Transformer,
        ParseState::Transformer => ParseState::Area,
        ParseState::Area => ParseState::TwoTerminalDc,
        ParseState::TwoTerminalDc => ParseState::VscDc,
        ParseState::VscDc => ParseState::ImpedanceCorrection,
        ParseState::ImpedanceCorrection => ParseState::MultiTerminalDc,
        ParseState::MultiTerminalDc => ParseState::MultiSectionLine,
        ParseState::MultiSectionLine => ParseState::Zone,
        ParseState::Zone => ParseState::InterAreaTransfer,
        ParseState::InterAreaTransfer => ParseState::Owner,
        ParseState::Owner => ParseState::Facts,
        ParseState::Facts => ParseState::SwitchedShunt,
        ParseState::SwitchedShunt => ParseState::GneDevice,
        ParseState::GneDevice => ParseState::InductionMachine,
        ParseState::InductionMachine => ParseState::Done,
        _ => ParseState::Done,
    }
}

// ---------------------------------------------------------------------------
// Low-level parsing helpers
// ---------------------------------------------------------------------------

/// Advance the iterator, strip a trailing `\r`, and return the line.
fn next_line(lines: &mut io::Lines<io::BufReader<fs::File>>) -> Result<Option<String>> {
    match lines.next() {
        None => Ok(None),
        Some(Ok(l)) => Ok(Some(l.trim_end_matches('\r').to_string())),
        Some(Err(e)) => Err(anyhow::Error::from(e).context("I/O error reading file")),
    }
}

/// Advance the iterator; return `Err` if the file ends unexpectedly.
/// Parse a Fortran-style floating-point token into an `f64`.
///
/// Handles:
/// * Bare value: `"3.14"` → `3.14`
/// * Fortran D-exponent: `"1.5D-3"` → `1.5e-3`
/// * Implicit exponent (no 'E'): `"1.5-3"` → `1.5e-3`
/// * Quoted strings: `"'1.0'"` → `1.0`
/// * Missing / empty: `""` → `0.0`
pub fn parse_fortran_double(raw: &str) -> f64 {
    let s = raw.trim().trim_matches('\'');
    if s.is_empty() {
        return 0.0;
    }

    // Fast path: try direct parse first (avoids allocation for the common case)
    if let Ok(v) = s.parse::<f64>() {
        return v;
    }

    // Replace Fortran 'D' exponent with 'e'
    let mut s = if s.contains('D') || s.contains('d') {
        s.replace(['D', 'd'], "e")
    } else {
        s.to_owned()
    };

    // Insert 'e' before a bare sign not already preceded by 'e'
    // e.g. "1.5-3" → "1.5e-3", "1.5+3" → "1.5e+3"
    let bytes = s.as_bytes().to_vec();
    let mut result = String::with_capacity(bytes.len() + 1);
    for (i, &b) in bytes.iter().enumerate() {
        let ch = b as char;
        if i > 0 && (ch == '+' || ch == '-') {
            let prev = bytes[i - 1] as char;
            if prev != 'e' && prev != 'E' && (prev.is_ascii_digit() || prev == '.') {
                result.push('e');
            }
        }
        result.push(ch);
    }
    s = result;

    s.parse::<f64>().unwrap_or(0.0)
}

/// Quote-aware comma tokeniser.  Strips surrounding single quotes from each
/// token and trims leading/trailing whitespace.  Does NOT split at commas
/// that appear inside a quoted string.
fn tokenize(line: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut token = String::new();
    let mut in_quotes = false;

    for ch in line.chars() {
        match ch {
            '\'' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                tokens.push(token.trim().to_string());
                token = String::new();
            }
            _ => token.push(ch),
        }
    }
    // Push the last token (may be empty for trailing comma)
    let t = token.trim().to_string();
    if !t.is_empty() || !tokens.is_empty() {
        tokens.push(t);
    }
    tokens
}

/// Split a line at the first `/` into `(data, hint)`.
/// The `hint` may contain a section-transition marker like
/// `"END OF BUS DATA, BEGIN LOAD DATA"`.
fn split_comment(line: &str) -> (&str, &str) {
    match line.find('/') {
        Some(pos) => (&line[..pos], &line[pos + 1..]),
        None => (line, ""),
    }
}

/// Return `true` if `data` (the portion before `/`) marks a section
/// terminator (`0` or `Q`).
fn is_section_end(data: &str) -> bool {
    let t = data.trim();
    t == "0" || t == "Q"
}

/// Extract the next [`ParseState`] from a section comment hint.
///
/// Searches only in the `BEGIN X DATA` portion to avoid false matches on
/// `"END OF BUS DATA, BEGIN LOAD DATA"` matching `BUS` in the END part.
fn hint_to_state(hint: &str, psse_version: u32) -> Option<ParseState> {
    let upper = hint.to_ascii_uppercase();
    let begin_pos = upper.find("BEGIN")?;
    let after = &upper[begin_pos + 5..];

    // Test most-specific patterns first
    if after.contains("SYSTEM SWITCHING") || after.contains("SYSTEM-SWITCHING") {
        return Some(ParseState::SystemSwitchingDevice);
    }
    if after.contains("SYSTEM-WIDE") || after.contains("SYSTEM WIDE") {
        return Some(ParseState::SystemWide);
    }
    if after.contains("SWITCHED SHUNT") || after.contains("SWITCHED-SHUNT") {
        // Both v33 and v35 land on the same state; version-aware field offsets handle the rest.
        let _ = psse_version;
        return Some(ParseState::SwitchedShunt);
    }
    if after.contains("FIXED SHUNT") || after.contains("FIXED-SHUNT") {
        return Some(ParseState::FixedShunt);
    }
    if after.contains("MULTI-TERMINAL") || after.contains("MULTI TERMINAL") {
        return Some(ParseState::MultiTerminalDc);
    }
    if after.contains("MULTI-SECTION") || after.contains("MULTI SECTION") {
        return Some(ParseState::MultiSectionLine);
    }
    if after.contains("TWO-TERMINAL") || after.contains("TWO TERMINAL") {
        return Some(ParseState::TwoTerminalDc);
    }
    if after.contains("VOLTAGE SOURCE") || after.contains("VSC") {
        return Some(ParseState::VscDc);
    }
    if after.contains("INTER") && after.contains("AREA") {
        return Some(ParseState::InterAreaTransfer);
    }
    if after.contains("IMPEDANCE") {
        return Some(ParseState::ImpedanceCorrection);
    }
    if after.contains("INDUCTION") {
        return Some(ParseState::InductionMachine);
    }
    if after.contains("GNE") {
        return Some(ParseState::GneDevice);
    }
    if after.contains("BUS") {
        return Some(ParseState::Bus);
    }
    if after.contains("LOAD") {
        return Some(ParseState::Load);
    }
    if after.contains("GENERATOR") {
        return Some(ParseState::Generator);
    }
    if after.contains("BRANCH") {
        return Some(ParseState::Branch);
    }
    if after.contains("TRANSFORMER") {
        return Some(ParseState::Transformer);
    }
    if after.contains("AREA") {
        return Some(ParseState::Area);
    }
    if after.contains("ZONE") {
        return Some(ParseState::Zone);
    }
    if after.contains("OWNER") {
        return Some(ParseState::Owner);
    }
    if after.contains("FACTS") {
        return Some(ParseState::Facts);
    }
    None
}

// ---------------------------------------------------------------------------
// Field accessor helpers
// ---------------------------------------------------------------------------

fn field_str(fields: &[String], idx: usize) -> String {
    fields
        .get(idx)
        .map(|s| s.trim_matches('\'').trim().to_string())
        .unwrap_or_default()
}

fn field_f64(fields: &[String], idx: usize) -> f64 {
    fields
        .get(idx)
        .map(|s| parse_fortran_double(s))
        .unwrap_or(0.0)
}

fn field_u32(fields: &[String], idx: usize) -> u32 {
    fields
        .get(idx)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn field_u32_default(fields: &[String], idx: usize, default: u32) -> u32 {
    fields
        .get(idx)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default)
}

fn field_i32(fields: &[String], idx: usize) -> i32 {
    fields
        .get(idx)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn field_u8(fields: &[String], idx: usize) -> u8 {
    fields
        .get(idx)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn field_u8_default(fields: &[String], idx: usize, default: u8) -> u8 {
    fields
        .get(idx)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default)
}

fn token_looks_float(token: &str) -> bool {
    let t = token.trim();
    t.contains('.') || t.contains('e') || t.contains('E') || t.contains('d') || t.contains('D')
}

fn token_looks_alpha(token: &str) -> bool {
    token.trim().chars().any(|c| c.is_ascii_alphabetic())
}

fn token_to_positive_u32(token: &str) -> Option<u32> {
    let t = token.trim();
    if t.is_empty() {
        return None;
    }
    t.parse::<i64>()
        .ok()
        .and_then(|v| (v > 0).then_some(v as u32))
}

fn token_to_f64(token: &str) -> Option<f64> {
    let t = token.trim();
    if t.is_empty() {
        return None;
    }
    let v = parse_fortran_double(t);
    if v.is_finite() { Some(v) } else { None }
}

// ---------------------------------------------------------------------------
// Per-record parsers (single-line sections)
// ---------------------------------------------------------------------------

/// Parse one BUS record.
///
/// Handles the optional bus NAME field: very old PSS/E formats (pre-v29) omit
/// it.  The heuristic: if `parts[1]` is empty or `''`, treat name as absent
/// and shift all subsequent indices down by one.
fn parse_bus_record(f: &[String]) -> Option<Bus> {
    if f.is_empty() {
        return None;
    }

    let i = field_u32(f, 0);
    if i == 0 {
        return None;
    }

    // Detect presence of the optional bus NAME field (indices shift by 1 when present)
    let has_name = f
        .get(1)
        .map(|s| !s.is_empty() && s != "''")
        .unwrap_or(false);

    let baskv_idx = if has_name { 2 } else { 1 };
    let ide_idx = baskv_idx + 1;
    let (gl_idx, bl_idx, area_idx, zone_idx, owner_idx, vm_idx, va_idx) = {
        let modern_area_idx = baskv_idx + 2;
        let modern_zone_idx = baskv_idx + 3;
        let modern_owner_idx = baskv_idx + 4;
        let modern_vm_idx = baskv_idx + 5;
        let modern_va_idx = baskv_idx + 6;

        // Some legacy/variant RAW exports include inline GL/BL in BUS records:
        // I, NAME, BASKV, IDE, GL, BL, AREA, ZONE, OWNER, VM, VA, ...
        // Use a conservative heuristic so v33/v35 layouts remain unchanged.
        let has_inline_shunt = if f.len() > modern_va_idx + 2 {
            let old_area_idx = baskv_idx + 4;
            let old_zone_idx = baskv_idx + 5;
            let old_owner_idx = baskv_idx + 6;
            let old_vm_idx = baskv_idx + 7;

            let modern_area = field_u32(f, modern_area_idx);
            let modern_zone = field_u32(f, modern_zone_idx);
            let modern_owner = field_u32(f, modern_owner_idx);
            let modern_vm = field_f64(f, modern_vm_idx);

            let old_area = field_u32(f, old_area_idx);
            let old_zone = field_u32(f, old_zone_idx);
            let old_owner = field_u32(f, old_owner_idx);
            let old_vm = field_f64(f, old_vm_idx);

            let modern_score = (modern_area > 0) as u8
                + (modern_zone > 0) as u8
                + (modern_owner > 0) as u8
                + ((0.2..=2.0).contains(&modern_vm)) as u8;
            let old_score = (old_area > 0) as u8
                + (old_zone > 0) as u8
                + (old_owner > 0) as u8
                + ((0.2..=2.0).contains(&old_vm)) as u8;

            let gl_token = f.get(baskv_idx + 2).map(|s| s.as_str()).unwrap_or("");
            let bl_token = f.get(baskv_idx + 3).map(|s| s.as_str()).unwrap_or("");
            let shunt_tokens_floaty = token_looks_float(gl_token) || token_looks_float(bl_token);

            old_score > modern_score || (old_score == modern_score && shunt_tokens_floaty)
        } else {
            false
        };

        if has_inline_shunt {
            (
                Some(baskv_idx + 2),
                Some(baskv_idx + 3),
                baskv_idx + 4,
                baskv_idx + 5,
                baskv_idx + 6,
                baskv_idx + 7,
                baskv_idx + 8,
            )
        } else {
            (
                None,
                None,
                modern_area_idx,
                modern_zone_idx,
                modern_owner_idx,
                modern_vm_idx,
                modern_va_idx,
            )
        }
    };

    let ide_raw = field_u8(f, ide_idx);
    let ide = match ide_raw {
        2 => BusType::GeneratorPQ,
        3 => BusType::GeneratorPV,
        4 => BusType::Slack,
        _ => BusType::LoadBus,
    };

    let vm_raw = field_f64(f, vm_idx);

    let nvhi_raw = field_f64(f, va_idx + 1);
    let nvlo_raw = field_f64(f, va_idx + 2);
    let nvhi = if nvhi_raw > 0.5 && nvhi_raw <= 2.0 {
        nvhi_raw
    } else {
        1.1
    };
    let nvlo = if nvlo_raw > 0.0 && nvlo_raw <= 2.0 {
        nvlo_raw
    } else {
        0.9
    };

    let evhi_raw = field_f64(f, va_idx + 3);
    let evlo_raw = field_f64(f, va_idx + 4);
    let evhi = if evhi_raw > 0.5 && evhi_raw <= 2.0 {
        evhi_raw
    } else {
        1.1
    };
    let evlo = if evlo_raw > 0.0 && evlo_raw <= 2.0 {
        evlo_raw
    } else {
        0.9
    };

    Some(Bus {
        i,
        name: if has_name {
            {
                let n = field_str(f, 1);
                // PSS/E bus name is exactly 12 chars; pad or truncate
                let mut s = n;
                if s.len() < 12 {
                    s.push_str(&" ".repeat(12 - s.len()));
                } else {
                    s.truncate(12);
                }
                s.into_boxed_str()
            }
        } else {
            "????????????".into()
        },
        baskv: field_f64(f, baskv_idx),
        ide,
        area: field_u32_default(f, area_idx, 1),
        zone: field_u32_default(f, zone_idx, 1),
        owner: field_u32_default(f, owner_idx, 1),
        gl: gl_idx.map(|idx| field_f64(f, idx)).unwrap_or(0.0),
        bl: bl_idx.map(|idx| field_f64(f, idx)).unwrap_or(0.0),
        vm: vm_raw,
        va: field_f64(f, va_idx),
        nvhi,
        nvlo,
        evhi,
        evlo,
    })
}

/// Parse one LOAD record.
fn parse_load_record(f: &[String]) -> Option<Load> {
    if f.len() < 6 {
        return None;
    }
    let i = field_u32(f, 0);
    if i == 0 {
        return None;
    }
    Some(Load {
        i,
        id: field_str(f, 1).into_boxed_str(),
        status: field_u8_default(f, 2, 1),
        area: field_u32_default(f, 3, 1),
        zone: field_u32_default(f, 4, 1),
        pl: field_f64(f, 5),
        ql: field_f64(f, 6),
        ip: field_f64(f, 7),
        iq: field_f64(f, 8),
        yp: field_f64(f, 9),
        yq: field_f64(f, 10),
        owner: field_u32_default(f, 11, 1),
        scale: field_u8(f, 12),
        intrpt: field_u8(f, 13),
    })
}

/// Parse one FIXED SHUNT record.
fn parse_fixed_shunt_record(f: &[String]) -> Option<FixedShunt> {
    if f.len() < 4 {
        return None;
    }
    let i = field_u32(f, 0);
    if i == 0 {
        return None;
    }
    Some(FixedShunt {
        i,
        id: field_str(f, 1).into_boxed_str(),
        status: field_u8_default(f, 2, 1),
        gl: field_f64(f, 3),
        bl: field_f64(f, 4),
    })
}

/// Parse one GENERATOR record (version-aware field offsets).
///
/// PSS/E v35 inserts `NREG` at index 8, shifting all subsequent fields by 1.
fn parse_generator_record(f: &[String], off: &VersionOffsets) -> Option<Generator> {
    if f.len() < 10 {
        return None;
    }
    let i = field_u32(f, 0);
    if i == 0 {
        return None;
    }

    let mbase = field_f64(f, off.gen_mbase_idx);
    let mbase = if mbase <= 0.0 { 100.0 } else { mbase };

    let pt = {
        let raw = field_f64(f, off.gen_pt_idx);
        if raw <= 0.0 { mbase } else { raw } // PSS/E fallback: Pmax = Mbase
    };
    let pb = {
        let raw = field_f64(f, off.gen_pb_idx);
        if raw < 0.0 || raw > pt { 0.0 } else { raw }
    };

    Some(Generator {
        i,
        id: field_str(f, 1).into_boxed_str(),
        pg: field_f64(f, 2),
        qg: field_f64(f, 3),
        qt: field_f64(f, 4),
        qb: field_f64(f, 5),
        vs: field_f64(f, 6),
        ireg: field_u32(f, 7),
        mbase,
        zr: field_f64(f, off.gen_zr_idx),
        zx: field_f64(f, off.gen_zr_idx + 1),
        rt: field_f64(f, off.gen_zr_idx + 2),
        xt: field_f64(f, off.gen_zr_idx + 3),
        gtap: field_f64(f, off.gen_zr_idx + 4),
        stat: field_u8_default(f, off.gen_stat_idx, 1),
        rmpct: field_f64(f, off.gen_rmpct_idx),
        pt,
        pb,
        o1: field_u32(f, off.gen_o1_idx),
        wmod: field_u8(f, off.gen_o1_idx + 1),
        wpf: field_f64(f, off.gen_o1_idx + 2),
    })
}

/// Parse one BRANCH record (version-aware field offsets).
///
/// PSS/E v35 inserts a `NAME` field at index 6 and expands to 12 rate fields,
/// pushing `STATUS` to index 23.  v33 has 3 rate fields; `STATUS` is at 13.
fn parse_branch_record(f: &[String], off: &VersionOffsets) -> Option<Branch> {
    if f.len() < 7 {
        return None; // minimum: I, J, CKT, R, X, B, RATEA
    }
    let i = field_u32(f, 0);
    let j = field_u32(f, 1);
    if i == 0 || j == 0 {
        return None;
    }

    // Status defaults to 1 when field is missing or malformed.
    let st = if f.len() > off.branch_status_idx {
        let v = field_i32(f, off.branch_status_idx);
        if v == 0 { 0u8 } else { 1u8 }
    } else {
        1u8
    };

    let ra = off.branch_ratea_idx;

    Some(Branch {
        i,
        j,
        ckt: field_str(f, 2).into_boxed_str(),
        r: field_f64(f, 3),
        // Preserve RAW branch reactance exactly; solver-side handling owns singularity policy.
        x: field_f64(f, 4),
        b: field_f64(f, 5),
        ratea: field_f64(f, ra),
        rateb: field_f64(f, ra + 1),
        ratec: field_f64(f, ra + 2),
        gi: field_f64(f, off.branch_gi_idx),
        bi: field_f64(f, off.branch_gi_idx + 1),
        gj: field_f64(f, off.branch_gi_idx + 2),
        bj: field_f64(f, off.branch_gi_idx + 3),
        st,
        met: field_u8(f, off.branch_status_idx + 1),
        len: field_f64(f, off.branch_status_idx + 2),
        o1: field_u32(f, off.branch_status_idx + 3),
    })
}

/// Parse one AREA INTERCHANGE record.
fn parse_area_record(f: &[String]) -> Area {
    Area {
        i: field_u32(f, 0),
        isw: field_u32(f, 1),
        pdes: field_f64(f, 2),
        ptol: field_f64(f, 3),
        arnam: field_str(f, 4).into_boxed_str(),
    }
}

/// Parse one ZONE record.
fn parse_zone_record(f: &[String]) -> Zone {
    Zone {
        i: field_u32(f, 0),
        zonam: field_str(f, 1).into_boxed_str(),
    }
}

/// Parse one OWNER record.
fn parse_owner_record(f: &[String]) -> Owner {
    Owner {
        i: field_u32(f, 0),
        ownam: field_str(f, 1).into_boxed_str(),
    }
}

/// Parse one Section 18 FACTS record into a normalized branch-oriented payload.
///
/// Section 18 has multiple formats in the wild. This parser intentionally keeps
/// the extraction conservative:
/// * requires at least two positive integer bus numbers in the record,
/// * captures a model/device token when present,
/// * preserves all remaining numeric tokens as `p1..pN`.
fn parse_facts_record(f: &[String]) -> Option<FactsDeviceRaw> {
    if f.len() < 3 {
        return None;
    }

    let mut bus_indices: Vec<usize> = Vec::new();
    let mut buses: Vec<u32> = Vec::new();
    for (idx, token) in f.iter().enumerate() {
        if let Ok(v) = token.trim().parse::<i64>() {
            if v > 0 {
                bus_indices.push(idx);
                buses.push(v as u32);
                if buses.len() == 2 {
                    break;
                }
            }
        }
    }
    if buses.len() < 2 {
        return None;
    }

    let mut device_type = "facts".to_string();
    if let Some(model_tok) = f
        .iter()
        .find(|tok| tok.chars().any(|c| c.is_ascii_alphabetic()))
    {
        let normalized = model_tok
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_ascii_lowercase();
        if !normalized.is_empty() {
            device_type = normalized;
        }
    }

    let mut params: Vec<(Box<str>, f64)> = Vec::new();
    let mut p_idx = 1usize;
    for (idx, token) in f.iter().enumerate() {
        if bus_indices.contains(&idx) {
            continue;
        }
        if let Ok(value) = token.trim().parse::<f64>() {
            params.push((format!("p{p_idx}").into_boxed_str(), value));
            p_idx += 1;
        }
    }

    Some(FactsDeviceRaw {
        bus_i: buses[0],
        bus_j: buses[1],
        device_type: device_type.into_boxed_str(),
        control_mode: None,
        target_flow_mw: None,
        x_min_pu: None,
        x_max_pu: None,
        injected_voltage_mag_pu: None,
        injected_voltage_angle_deg: None,
        params,
    })
}

/// Parse one SWITCHED SHUNT record (version-aware).
///
/// Expands `N₁/B₁ … N₈/B₈` pairs into `steps`: a flat list where each step
/// value (B in MVAr) is repeated `N` times.  Both capacitive (B > 0) and
/// inductive (B < 0) steps are stored; the consumer decides how to use them.
fn parse_switched_shunt_record(f: &[String], off: &VersionOffsets) -> Option<SwitchedShunt> {
    let i = field_u32(f, 0);
    if i == 0 {
        return None;
    }
    let mut steps = Vec::new();
    let mut bank_pairs: Vec<(u32, f64)> = Vec::new();
    let ps = off.sw_pairs_start;
    let mut idx = ps;
    while idx + 1 < f.len() {
        let n = field_u32(f, idx) as usize;
        let b = field_f64(f, idx + 1);
        if n == 0 {
            break;
        }
        for _ in 0..n {
            steps.push(b);
        }
        bank_pairs.push((n as u32, b));
        idx += 2;
    }
    Some(SwitchedShunt {
        i,
        modsw: field_u8(f, off.sw_modsw_idx),
        adjm: field_u8(f, off.sw_adjm_idx),
        stat: field_u8_default(f, off.sw_stat_idx, 1),
        vswhi: field_f64(f, off.sw_vswhi_idx),
        vswlo: field_f64(f, off.sw_vswlo_idx),
        swrem: field_u32(f, off.sw_swreg_idx),
        rmpct: field_f64(f, off.sw_rmpct_idx),
        rmidnt: field_str(f, off.sw_rmidnt_idx).into_boxed_str(),
        binit: field_f64(f, off.sw_binit_idx),
        steps,
        bank_pairs,
    })
}

fn first_plausible_bus_pair_with_indices(f: &[String]) -> Option<(usize, usize, u32, u32)> {
    // Prefer adjacent positive integers to reduce false positives from IDs + controls.
    for i in 0..f.len().saturating_sub(1) {
        let a = token_to_positive_u32(&f[i])?;
        let b = token_to_positive_u32(&f[i + 1])?;
        if a != b {
            return Some((i, i + 1, a, b));
        }
    }

    // Fallback: first two positive integer tokens anywhere in row.
    let mut seen: Vec<(usize, u32)> = Vec::new();
    for (idx, tok) in f.iter().enumerate() {
        if let Some(v) = token_to_positive_u32(tok) {
            seen.push((idx, v));
            if seen.len() == 2 {
                break;
            }
        }
    }
    if seen.len() == 2 {
        Some((seen[0].0, seen[1].0, seen[0].1, seen[1].1))
    } else {
        None
    }
}

fn nearest_non_numeric_label(f: &[String], bus_b_idx: usize) -> Option<String> {
    f.iter()
        .enumerate()
        .skip(bus_b_idx + 1)
        .find(|(_, t)| token_looks_alpha(t) && t.len() <= 12)
        .map(|(_, t)| t.trim().trim_matches('"').trim_matches('\'').to_string())
}

fn collect_numeric_after(f: &[String], start_idx: usize) -> Vec<f64> {
    let mut out = Vec::new();
    for tok in f.iter().skip(start_idx) {
        if let Some(v) = token_to_f64(tok) {
            out.push(v);
        }
    }
    out
}

fn parse_dc_line_record(f: &[String], dc_line_id: i32, converter_type: &str) -> Option<DcLine2W> {
    let (a_idx, b_idx, from_bus_id, to_bus_id) = first_plausible_bus_pair_with_indices(f)?;

    let ckt = nearest_non_numeric_label(f, b_idx).unwrap_or_else(|| field_str(f, 2));
    let ckt = if ckt.is_empty() {
        format!("DC{}", dc_line_id)
    } else {
        ckt
    };

    let numeric_tail = collect_numeric_after(f, b_idx + 1);
    let r_ohm = numeric_tail.first().copied().unwrap_or(0.0);
    let l_henry = numeric_tail
        .get(1)
        .copied()
        .and_then(|v| (v.abs() > 0.0).then_some(v));

    let control_mode_token = f
        .iter()
        .find(|t| t.chars().any(|c| c.is_ascii_alphabetic()))
        .map(|s| {
            s.trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_ascii_lowercase()
        })
        .unwrap_or_else(|| "power".to_string());

    let p_setpoint_mw = numeric_tail.get(2).copied();
    let i_setpoint_ka = numeric_tail.get(3).copied();
    let v_setpoint_kv = numeric_tail.get(4).copied();

    if a_idx == b_idx || from_bus_id == to_bus_id {
        return None;
    }

    Some(DcLine2W {
        dc_line_id,
        from_bus_id,
        to_bus_id,
        ckt: ckt.into_boxed_str(),
        r_ohm,
        l_henry,
        control_mode: control_mode_token.into_boxed_str(),
        p_setpoint_mw,
        i_setpoint_ka,
        v_setpoint_kv,
        q_from_mvar: None,
        q_to_mvar: None,
        status: true,
        name: None,
        converter_type: converter_type.to_string().into_boxed_str(),
    })
}

fn parse_multi_section_line_record(f: &[String], line_id: i32) -> Option<MultiSectionLine> {
    let (_a_idx, b_idx, from_bus_id, to_bus_id) = first_plausible_bus_pair_with_indices(f)?;
    if from_bus_id == to_bus_id {
        return None;
    }
    let ckt = nearest_non_numeric_label(f, b_idx).unwrap_or_else(|| field_str(f, 2));
    let ckt = if ckt.is_empty() {
        format!("MSL{}", line_id)
    } else {
        ckt
    };

    let nums = collect_numeric_after(f, b_idx + 1);

    Some(MultiSectionLine {
        line_id,
        from_bus_id,
        to_bus_id,
        ckt: ckt.into_boxed_str(),
        section_branch_ids: Vec::new(),
        total_r_pu: nums.first().copied().unwrap_or(0.0),
        total_x_pu: nums.get(1).copied().unwrap_or(0.0),
        total_b_pu: nums.get(2).copied().unwrap_or(0.0),
        rate_a_mva: nums.get(3).copied().unwrap_or(0.0),
        rate_b_mva: nums.get(4).copied(),
        status: true,
        name: None,
    })
}

// ---------------------------------------------------------------------------
// Header parsing
// ---------------------------------------------------------------------------

/// Parse header line 1 into `CaseId` and return the detected PSS/E version.
///
/// Accepts several common header variants:
/// * Standard: `IC, SBASE, REV, XFRRAT, NXFRAT, BASFRQ / title`
/// * Legacy (pre-v29): version may not be in the third position
/// * Expert-fallback: scan all tokens for a plausible version in 20..=40
fn parse_header_line(line: &str) -> (CaseId, u32) {
    let (data, hint) = split_comment(line);
    let title = hint.trim().to_string();
    let f = tokenize(data);

    // Try position 2 first (standard layout)
    let mut psse_version: i32 = f
        .get(2)
        .and_then(|s| s.trim().parse::<i32>().ok())
        .filter(|&v| (20..=40).contains(&v))
        .unwrap_or(-1);

    // Fallback: scan all tokens
    if psse_version < 0 {
        for tok in &f {
            if let Ok(v) = tok.trim().parse::<i32>() {
                if (20..=40).contains(&v) {
                    psse_version = v;
                    break;
                }
            }
        }
    }

    // Last resort: infer v33 from base MVA
    if psse_version < 0 {
        if let Some(mva) = f.get(1).and_then(|s| s.parse::<f64>().ok()) {
            if mva > 1.0 && mva < 1.0e6 {
                psse_version = 33;
            }
        }
    }
    let psse_version = psse_version.max(33) as u32;

    let basfrq = f.get(5).and_then(|s| s.parse::<f64>().ok()).unwrap_or(60.0);
    let basfrq = if basfrq <= 0.0 { 60.0 } else { basfrq };

    let sbase = f
        .get(1)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(100.0);

    let case_id = CaseId {
        sbase,
        rev: psse_version,
        xfrrat: field_u8(&f, 3),
        basfrq,
        title: title.into_boxed_str(),
    };

    (case_id, psse_version)
}

// ---------------------------------------------------------------------------
// Transformer star-equivalent helpers
// ---------------------------------------------------------------------------

/// Build a star-equivalent [`TwoWindingTransformer`] leg for a 3-winding
/// transformer.  `to_bus` is the fictitious star bus.
#[allow(clippy::too_many_arguments)]
fn star_leg_transformer(
    from_bus: u32,
    to_bus: u32,
    ckt_suffix: u8,
    r_star: f64,
    x_star: f64,
    windv: f64,
    ang_deg: f64,
    rate_mva: f64,
    sbase: f64,
    stat: u8,
) -> TwoWindingTransformer {
    TwoWindingTransformer {
        i: from_bus,
        j: to_bus,
        ckt: format!("S{ckt_suffix}").into_boxed_str(),
        cw: 0,
        cz: 0,
        stat,
        mag1: 0.0,
        mag2: 0.0,
        r12: r_star,
        x12: x_star,
        sbase12: sbase,
        windv1: windv,
        nomv1: 0.0,
        ang1: ang_deg,
        rata1: rate_mva,
        ratb1: 0.0,
        ratc1: 0.0,
        windv2: 1.0,
        nomv2: 0.0,
    }
}

/// Build a fictitious star bus for the 3W star expansion.
fn fictitious_star_bus(id: u32, area: u32, zone: u32, owner: u32) -> Bus {
    Bus {
        i: id,
        name: "STAR        ".into(),
        baskv: 0.0,
        ide: BusType::LoadBus,
        area,
        zone,
        owner,
        gl: 0.0,
        bl: 0.0,
        vm: 1.0,
        va: 0.0,
        nvhi: 1.5,
        nvlo: 0.5,
        evhi: 1.5,
        evlo: 0.5,
    }
}

// ---------------------------------------------------------------------------
// Public entry point: parse_raw
// ---------------------------------------------------------------------------

/// Parse a PSS/E RAW file (v23–v35+) into a [`Network`].
///
/// Sections are detected by the `0 / END OF X DATA, BEGIN Y DATA` comment
/// hints, falling back to the version-appropriate default ordering.  The
/// parser tolerates empty sections, out-of-place section terminators, and
/// most encoding quirks found in real-world PSS/E export files.
///
/// # 3-winding transformers
/// 3-winding records (K ≠ 0) are converted to a star-equivalent: a fictitious
/// bus (ID > 10 000 000) plus three [`TwoWindingTransformer`] legs. The
/// fictitious buses are used only as an internal normalization aid and are
/// removed before final RPF emission.
pub fn parse_raw(path: &Path) -> Result<Network> {
    let file = fs::File::open(path)
        .with_context(|| format!("cannot open RAW file: {}", path.display()))?;
    let reader = io::BufReader::new(file);
    let mut lines_iter = reader.lines();

    let mut state = ParseState::Header;
    let mut psse_version: u32 = 33;
    let mut off = version_offsets(psse_version);

    let mut result = Network::default();
    // Counter for fictitious star bus IDs generated by 3W expansion.
    let mut next_star_id: u32 = 10_000_001;
    let mut next_dc_line_id: i32 = 1;
    let mut next_multi_section_line_id: i32 = 1;
    let mut dc_rows_rejected: usize = 0;
    let mut multi_section_rows_rejected: usize = 0;

    loop {
        let raw_line = match next_line(&mut lines_iter)? {
            None => break,
            Some(l) => l,
        };

        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Skip PSS/E directive / comment lines
        if trimmed.starts_with("@!") || trimmed.starts_with("@") {
            continue;
        }

        let (data_part, hint_part) = split_comment(trimmed);
        let data = data_part.trim();

        // ---- Section terminator ----
        if is_section_end(data) {
            let next = hint_to_state(hint_part, psse_version)
                .unwrap_or_else(|| default_next_state(state, psse_version));
            state = next;
            if state == ParseState::Done {
                break;
            }
            continue;
        }

        if data.is_empty() {
            continue;
        }

        // ---- Record dispatch ----
        match state {
            // ================================================================
            // HEADER — exactly 3 lines, no section terminator
            // ================================================================
            ParseState::Header => {
                let (case_id, ver) = parse_header_line(trimmed);
                psse_version = ver;
                off = version_offsets(psse_version);
                result.case_id = case_id;

                // Consume the two case-description text lines (lines 2-3)
                let _ = next_line(&mut lines_iter)?;
                let _ = next_line(&mut lines_iter)?;

                state = if psse_version >= 35 {
                    ParseState::SystemWide
                } else {
                    ParseState::Bus
                };
            }

            // ================================================================
            // BUS DATA
            // ================================================================
            ParseState::Bus => {
                let f = tokenize(data);
                if let Some(bus) = parse_bus_record(&f) {
                    result.buses.push(bus);
                }
            }

            // ================================================================
            // LOAD DATA
            // ================================================================
            ParseState::Load => {
                let f = tokenize(data);
                if let Some(load) = parse_load_record(&f) {
                    result.loads.push(load);
                }
            }

            // ================================================================
            // FIXED SHUNT DATA
            // ================================================================
            ParseState::FixedShunt => {
                let f = tokenize(data);
                if let Some(shunt) = parse_fixed_shunt_record(&f) {
                    result.fixed_shunts.push(shunt);
                }
            }

            // ================================================================
            // GENERATOR DATA
            // ================================================================
            ParseState::Generator => {
                let f = tokenize(data);
                if let Some(generator) = parse_generator_record(&f, &off) {
                    result.generators.push(generator);
                }
            }

            // ================================================================
            // BRANCH DATA
            // ================================================================
            ParseState::Branch => {
                let f = tokenize(data);
                if let Some(branch) = parse_branch_record(&f, &off) {
                    result.branches.push(branch);
                }
            }

            // ================================================================
            // TRANSFORMER DATA — multi-line records (4 lines for 2W, 5 for 3W)
            // ================================================================
            ParseState::Transformer => {
                let f1 = tokenize(data);
                if f1.len() < 3 {
                    continue;
                }

                let i_bus = field_u32(&f1, 0);
                let j_bus = field_u32(&f1, 1);
                let k_bus = field_u32(&f1, 2); // 0 = 2-winding, else 3-winding
                if i_bus == 0 || j_bus == 0 {
                    continue;
                }

                // Record 1: I, J, K, CKT, CW, CZ, CM, MAG1, MAG2, NMETR, NAME, STAT, ...
                let ckt = field_str(&f1, 3);
                let cw = field_u8(&f1, 4);
                let cz = field_u8(&f1, 5);
                let mag1 = field_f64(&f1, 7);
                let mag2 = field_f64(&f1, 8);
                let stat = field_u8_default(&f1, 11, 1);

                // Always read lines 2, 3, 4 (and 5 for 3W) regardless of status,
                // so the line iterator stays synchronised with the file.
                let l2 = match next_line(&mut lines_iter)? {
                    None => break,
                    Some(l) => l,
                };
                let l3 = match next_line(&mut lines_iter)? {
                    None => break,
                    Some(l) => l,
                };
                let l4 = match next_line(&mut lines_iter)? {
                    None => break,
                    Some(l) => l,
                };
                let l5 = if k_bus != 0 {
                    match next_line(&mut lines_iter)? {
                        None => break,
                        Some(l) => Some(l),
                    }
                } else {
                    None
                };

                let f2 = tokenize(l2.trim());
                let f3 = tokenize(l3.trim());
                let f4 = tokenize(l4.trim());

                // Record 2: R1-2, X1-2, SBASE1-2[, R2-3, X2-3, SBASE2-3, R3-1, X3-1, SBASE3-1]
                let r12 = field_f64(&f2, 0);
                let x12 = field_f64(&f2, 1);
                let sbase12 = field_f64(&f2, 2);

                // Record 3: WINDV1, NOMV1, ANG1, RATA1, RATB1, RATC1, …
                let windv1 = field_f64(&f3, 0);
                let nomv1 = field_f64(&f3, 1);
                let ang1 = field_f64(&f3, 2);
                let rata1 = field_f64(&f3, 3);
                let ratb1 = field_f64(&f3, 4);
                let ratc1 = field_f64(&f3, 5);

                // Record 4: WINDV2, NOMV2[, ANG2, RATA2, …]
                let windv2 = field_f64(&f4, 0);
                let nomv2 = field_f64(&f4, 1);

                if k_bus == 0 {
                    // ---- 2-winding transformer ----
                    result.transformers.push(TwoWindingTransformer {
                        i: i_bus,
                        j: j_bus,
                        ckt: ckt.into_boxed_str(),
                        cw,
                        cz,
                        stat,
                        mag1,
                        mag2,
                        r12,
                        x12,
                        sbase12,
                        windv1,
                        nomv1,
                        ang1,
                        rata1,
                        ratb1,
                        ratc1,
                        windv2,
                        nomv2,
                    });
                } else {
                    // ---- 3-winding transformer → star equivalent ----
                    let f5 = tokenize(l5.unwrap().trim());

                    // Record 2 (3W): R1-2, X1-2, SBASE1-2, R2-3, X2-3, SBASE2-3, R3-1, X3-1, SBASE3-1
                    let r23 = field_f64(&f2, 3);
                    let x23 = field_f64(&f2, 4);
                    let r31 = field_f64(&f2, 6);
                    let x31 = field_f64(&f2, 7);

                    // Record 4 for winding 2: WINDV2, NOMV2, ANG2, RATA2, …
                    let ang2 = field_f64(&f4, 2);
                    let rata2 = field_f64(&f4, 3);
                    let ratb2 = field_f64(&f4, 4);
                    let ratc2 = field_f64(&f4, 5);

                    // Record 5 for winding 3: WINDV3, NOMV3, ANG3, RATA3, …
                    let windv3 = field_f64(&f5, 0);
                    let nomv3 = field_f64(&f5, 1);
                    let ang3 = field_f64(&f5, 2);
                    let rata3 = field_f64(&f5, 3);
                    let ratb3 = field_f64(&f5, 4);
                    let ratc3 = field_f64(&f5, 5);

                    // Star-delta impedance decomposition
                    let za_r = 0.5 * (r12 + r31 - r23);
                    let za_x = 0.5 * (x12 + x31 - x23);
                    let zb_r = 0.5 * (r12 + r23 - r31);
                    let zb_x = 0.5 * (x12 + x23 - x31);
                    let zc_r = 0.5 * (r23 + r31 - r12);
                    let zc_x = 0.5 * (x23 + x31 - x12);

                    // Minimum MVA rating across the three windings
                    let rate = rata1.min(rata2).min(rata3);
                    let rate_b = ratb1.min(ratb2).min(ratb3);
                    let rate_c = ratc1.min(ratc2).min(ratc3);

                    // Fictitious star bus
                    let star_id = next_star_id;
                    next_star_id += 1;

                    result.transformers_3w.push(ThreeWindingTransformer {
                        bus_h: i_bus,
                        bus_m: j_bus,
                        bus_l: k_bus,
                        star_bus_id: star_id,
                        ckt: ckt.clone().into_boxed_str(),
                        stat,
                        r_hm: r12,
                        x_hm: x12,
                        r_hl: r31,
                        x_hl: x31,
                        r_ml: r23,
                        x_ml: x23,
                        tap_h: windv1,
                        tap_m: windv2,
                        tap_l: windv3,
                        phase_shift_deg: ang1,
                        rate_a_mva: rate,
                        rate_b_mva: rate_b,
                        rate_c_mva: rate_c,
                        nominal_kv_h: nomv1,
                        nominal_kv_m: nomv2,
                        nominal_kv_l: nomv3,
                    });

                    // Determine area/zone/owner from bus i (must be in bus list already
                    // because buses are parsed before transformers in PSS/E ordering)
                    let (star_area, star_zone, star_owner) = result
                        .buses
                        .iter()
                        .find(|b| b.i == i_bus)
                        .map_or((1u32, 1u32, 1u32), |b| (b.area, b.zone, b.owner));

                    result.buses.push(fictitious_star_bus(
                        star_id, star_area, star_zone, star_owner,
                    ));

                    result.transformers.push(star_leg_transformer(
                        i_bus, star_id, 1, za_r, za_x, windv1, ang1, rate, sbase12, stat,
                    ));
                    result.transformers.push(star_leg_transformer(
                        j_bus, star_id, 2, zb_r, zb_x, windv2, ang2, rate, sbase12, stat,
                    ));
                    result.transformers.push(star_leg_transformer(
                        k_bus, star_id, 3, zc_r, zc_x, windv3, ang3, rate, sbase12, stat,
                    ));
                }
            }

            // ================================================================
            // AREA INTERCHANGE DATA
            // ================================================================
            ParseState::Area => {
                let f = tokenize(data);
                if field_u32(&f, 0) > 0 {
                    result.areas.push(parse_area_record(&f));
                }
            }

            // ================================================================
            // TWO-TERMINAL DC DATA
            // ================================================================
            ParseState::TwoTerminalDc => {
                let f = tokenize(data);
                if let Some(row) = parse_dc_line_record(&f, next_dc_line_id, "lcc") {
                    result.dc_lines_2w.push(row);
                    next_dc_line_id += 1;
                } else {
                    dc_rows_rejected += 1;
                }
            }

            // ================================================================
            // VSC DC DATA
            // ================================================================
            ParseState::VscDc => {
                let f = tokenize(data);
                if let Some(row) = parse_dc_line_record(&f, next_dc_line_id, "vsc") {
                    result.dc_lines_2w.push(row);
                    next_dc_line_id += 1;
                } else {
                    dc_rows_rejected += 1;
                }
            }

            // ================================================================
            // MULTI-TERMINAL DC DATA (presence signal only in this converter)
            // ================================================================
            ParseState::MultiTerminalDc => {
                let f = tokenize(data);
                if !f.is_empty() && field_u32(&f, 0) > 0 {
                    result.has_multi_terminal_dc = true;
                }
            }

            // ================================================================
            // MULTI-SECTION LINE DATA
            // ================================================================
            ParseState::MultiSectionLine => {
                let f = tokenize(data);
                if let Some(row) = parse_multi_section_line_record(&f, next_multi_section_line_id) {
                    result.multi_section_lines.push(row);
                    next_multi_section_line_id += 1;
                } else {
                    multi_section_rows_rejected += 1;
                }
            }

            // ================================================================
            // ZONE DATA
            // ================================================================
            ParseState::Zone => {
                let f = tokenize(data);
                if field_u32(&f, 0) > 0 {
                    result.zones.push(parse_zone_record(&f));
                }
            }

            // ================================================================
            // OWNER DATA
            // ================================================================
            ParseState::Owner => {
                let f = tokenize(data);
                if field_u32(&f, 0) > 0 {
                    result.owners.push(parse_owner_record(&f));
                }
            }

            // ================================================================
            // FACTS DATA (section 18)
            // ================================================================
            ParseState::Facts => {
                let f = tokenize(data);
                if let Some(facts) = parse_facts_record(&f) {
                    result.facts_devices.push(facts);
                }
            }

            // ================================================================
            // SWITCHED SHUNT DATA
            // ================================================================
            ParseState::SwitchedShunt => {
                let f = tokenize(data);
                if let Some(ss) = parse_switched_shunt_record(&f, &off) {
                    result.switched_shunts.push(ss);
                }
            }

            // ================================================================
            // Sections we intentionally skip (no data consumed)
            // ================================================================
            ParseState::SystemWide
            | ParseState::SystemSwitchingDevice
            | ParseState::ImpedanceCorrection
            | ParseState::InterAreaTransfer
            | ParseState::GneDevice
            | ParseState::InductionMachine => { /* skip */ }

            ParseState::Done => break,
        }
    }

    eprintln!(
        "[parser v{}] buses={} loads={} fixed_shunts={} generators={} \
         branches={} transformers_2w={} areas={} zones={} owners={} \
         switched_shunts={} facts_devices={} dc_lines_2w={} multi_section_lines={} has_mtdc={} \
         dc_rows_rejected={} msl_rows_rejected={}",
        psse_version,
        result.buses.len(),
        result.loads.len(),
        result.fixed_shunts.len(),
        result.generators.len(),
        result.branches.len(),
        result.transformers.len(),
        result.areas.len(),
        result.zones.len(),
        result.owners.len(),
        result.switched_shunts.len(),
        result.facts_devices.len(),
        result.dc_lines_2w.len(),
        result.multi_section_lines.len(),
        result.has_multi_terminal_dc,
        dc_rows_rejected,
        multi_section_rows_rejected,
    );

    if dc_rows_rejected > 0 {
        eprintln!("[parser] skipped {dc_rows_rejected} malformed/unsupported DC section row(s)");
    }
    if multi_section_rows_rejected > 0 {
        eprintln!(
            "[parser] skipped {multi_section_rows_rejected} malformed/unsupported multi-section line row(s)"
        );
    }

    Ok(result)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::parse_facts_record;

    #[test]
    fn parse_facts_record_extracts_bus_pair_and_params() {
        let fields = vec![
            "1001".to_string(),
            "2002".to_string(),
            "TCSC".to_string(),
            "1.5".to_string(),
            "-0.2".to_string(),
            "0.3".to_string(),
        ];
        let rec = parse_facts_record(&fields).expect("should parse FACTS row");

        assert_eq!(rec.bus_i, 1001);
        assert_eq!(rec.bus_j, 2002);
        assert_eq!(rec.device_type.as_ref(), "tcsc");
        assert_eq!(rec.params.len(), 3);
        assert_eq!(rec.params[0].0.as_ref(), "p1");
    }

    #[test]
    fn parse_facts_record_rejects_without_two_bus_numbers() {
        let fields = vec!["SVC".to_string(), "alpha".to_string(), "beta".to_string()];
        assert!(parse_facts_record(&fields).is_none());
    }
}

// ---------------------------------------------------------------------------
// Public entry point: parse_dyr
// ---------------------------------------------------------------------------

/// Parse a PSS/E DYR dynamic data file and return all recognised synchronous
/// machine records.
///
/// # Supported models
/// | Model name | H index | D index | Xd′ index |
/// |-----------|---------|---------|-----------|
/// | GENROU / GENROE | 7  | 8  | 11 |
/// | GENSAL / GENSAE | 5  | 6  | 9  |
/// | GENCLS          | 3  | 4  | — |
///
/// Each record is terminated by a `/` character.  Records may span multiple
/// lines.  Comment lines that start with `@` are skipped.
pub fn parse_dyr(path: &Path) -> Result<Vec<DyrGeneratorData>> {
    let records = parse_dyr_records(path)?;
    Ok(extract_dyr_generators(&records))
}

/// Parse all numeric DYR records from `path`.
pub fn parse_dyr_records(path: &Path) -> Result<Vec<DyrModelData>> {
    let file = fs::File::open(path)
        .with_context(|| format!("cannot open DYR file: {}", path.display()))?;
    let reader = io::BufReader::new(file);

    let mut records: Vec<DyrModelData> = Vec::new();
    let mut pending = String::new();

    for line_result in reader.lines() {
        let line = line_result.context("I/O error reading DYR file")?;
        let mut remaining = line.as_str();

        loop {
            let slash_pos = remaining.find('/');
            let segment = match slash_pos {
                Some(pos) => remaining[..pos].trim(),
                None => remaining.trim(),
            };

            if !segment.is_empty() && !segment.starts_with('@') {
                if !pending.is_empty() {
                    pending.push(' ');
                }
                pending.push_str(segment);
            }

            if let Some(pos) = slash_pos {
                // Slash terminates a record; process whatever was accumulated
                if !pending.is_empty() {
                    if let Some(rec) = try_parse_dyr_record(&pending) {
                        records.push(rec);
                    }
                    pending.clear();
                }
                remaining = &remaining[pos + 1..];
            } else {
                break;
            }
        }
    }

    // Handle any unterminated trailing record
    if !pending.is_empty() {
        if let Some(rec) = try_parse_dyr_record(&pending) {
            records.push(rec);
        }
    }

    let machine_count = extract_dyr_generators(&records).len();

    eprintln!(
        "[parser] {} DYR records parsed ({} supported machine models) from {}",
        records.len(),
        machine_count,
        path.display()
    );

    Ok(records)
}

/// Extract the supported synchronous-machine subset from raw DYR records.
pub fn extract_dyr_generators(records: &[DyrModelData]) -> Vec<DyrGeneratorData> {
    records.iter().filter_map(try_extract_dyr_machine).collect()
}

/// Attempt to parse one DYR record from a `/`-terminated accumulation.
///
/// Expected token layout: `BUS_ID  'MODEL_NAME'  MACHINE_ID  ... parameters ...`
fn try_parse_dyr_record(record: &str) -> Option<DyrModelData> {
    // Tokenise by whitespace and commas, stripping quotes
    let parts: Vec<String> = {
        let mut toks: Vec<String> = Vec::new();
        let mut tok = String::new();
        let mut in_q = false;
        for ch in record.chars() {
            match ch {
                '\'' => in_q = !in_q,
                ' ' | '\t' | ',' if !in_q => {
                    if !tok.is_empty() {
                        toks.push(tok.trim().to_string());
                        tok = String::new();
                    }
                }
                _ => tok.push(ch),
            }
        }
        if !tok.trim().is_empty() {
            toks.push(tok.trim().to_string());
        }
        toks
    };

    if parts.len() < 3 {
        return None;
    }

    let bus_id: u32 = parts[0].parse().ok().filter(|&v| v > 0)?;
    let model = parts[1].to_ascii_uppercase();
    let machine_id = normalize_machine_id(&parts[2]);

    let params = parts[3..]
        .iter()
        .enumerate()
        .map(|(idx, token)| {
            (
                format!("p{}", idx + 1).into_boxed_str(),
                parse_fortran_double(token),
            )
        })
        .collect();

    Some(DyrModelData {
        bus_id,
        id: machine_id.into_boxed_str(),
        model: model.into_boxed_str(),
        params,
    })
}

fn try_extract_dyr_machine(record: &DyrModelData) -> Option<DyrGeneratorData> {
    let mut data = DyrGeneratorData {
        bus_id: record.bus_id,
        id: record.id.clone(),
        model: record.model.clone(),
        h: 0.0,
        d: 0.0,
        xd_prime: 0.0,
    };

    let set = |v: &mut f64, params: &[(Box<str>, f64)], idx: usize| {
        if let Some((_, value)) = idx
            .checked_sub(3)
            .and_then(|param_idx| params.get(param_idx))
        {
            *v = *value;
        }
    };

    match record.model.as_ref() {
        "GENROU" | "GENROE" => {
            set(&mut data.h, &record.params, 7);
            set(&mut data.d, &record.params, 8);
            set(&mut data.xd_prime, &record.params, 11);
            Some(data)
        }
        "GENSAL" | "GENSAE" => {
            set(&mut data.h, &record.params, 5);
            set(&mut data.d, &record.params, 6);
            set(&mut data.xd_prime, &record.params, 9);
            Some(data)
        }
        "GENCLS" => {
            set(&mut data.h, &record.params, 3);
            set(&mut data.d, &record.params, 4);
            Some(data)
        }
        _ => None, // Exciter, governor, etc. — not a machine model
    }
}

/// Uppercase and trim a PSS/E machine ID token.
fn normalize_machine_id(s: &str) -> String {
    s.trim().to_ascii_uppercase()
}
