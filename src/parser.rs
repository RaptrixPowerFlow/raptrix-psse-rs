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
//! * **Quote-aware comment split**: `/` starts a trailing comment only outside
//!   a single-quoted field (a name such as `'N/1'` stays intact).
//! * **3-winding transformer star expansion**: converts each pairwise Z on its
//!   own SBASE, then creates a fictitious star bus and three 2-winding legs.
//! * **DYR parser**: preserves all numeric dynamic model records and extracts
//!   synchronous-machine parameters used by the generator table.

use std::{fs, path::Path};

use anyhow::{Context, Result};

use crate::models::{
    Area, Branch, Bus, BusType, CaseId, DcConverter, DcLine2W, DyrGeneratorData, DyrModelData,
    FactsDeviceRaw, FixedShunt, Generator, Load, MultiSectionLine, Network, Owner, SwitchedShunt,
    ThreeWindingTransformer, TwoWindingTransformer, Zone,
};
use crate::transformer_convert::{
    TransformerId, convert_z_to_system, validate_transformer_codes, winding_pu_of_baskv,
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
    /// Index of WMOD in a GENERATOR record (after O1,F1,…,O4,F4 owner block).
    pub gen_wmod_idx: usize,
    /// Index of WPF in a GENERATOR record.
    pub gen_wpf_idx: usize,
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
        // v35 GENERATOR: NREG @ 8 → MBASE @ 9; BASLOD @ 19 → O1 @ 20; WMOD @ 28, WPF @ 29
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
            gen_o1_idx: 20,
            gen_wmod_idx: 28,
            gen_wpf_idx: 29,
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
            gen_wmod_idx: 26,
            gen_wpf_idx: 27,
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

/// v34 decks may use the expanded branch layout (NAME + 12 ratings) before v35.
/// v33 lines can also exceed 24 tokens when tap/owner tail fields are present — those
/// still use classic STATUS at index 13 (RATEA remains at index 6).
fn branch_record_uses_v35_expanded_layout(f: &[String], psse_version: u32) -> bool {
    if psse_version >= 35 {
        return true;
    }
    if psse_version != 34 || f.len() < 24 {
        return false;
    }
    // Expanded v34 inserts a non-numeric NAME at index 6; classic rows have RATEA there.
    f.get(6)
        .and_then(|s| s.trim().parse::<f64>().ok())
        .is_none()
}

fn branch_offsets_for_record(f: &[String], psse_version: u32) -> VersionOffsets {
    let mut off = version_offsets(psse_version);
    if branch_record_uses_v35_expanded_layout(f, psse_version) {
        let expanded = version_offsets(35);
        off.branch_status_idx = expanded.branch_status_idx;
        off.branch_ratea_idx = expanded.branch_ratea_idx;
        off.branch_gi_idx = expanded.branch_gi_idx;
    }
    off
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
fn next_line<'a>(lines: &mut std::str::Lines<'a>) -> Option<String> {
    lines.next().map(|l| l.trim_end_matches('\r').to_string())
}

/// Read a PSS/E deck. Prefer UTF-8; fall back to Windows-1252.
/// Some exported titles carry `0x93` / `0x94` smart quotes.
fn read_vendor_text(path: &Path, kind: &str) -> Result<String> {
    let bytes =
        fs::read(path).with_context(|| format!("cannot open {kind} file: {}", path.display()))?;
    Ok(decode_vendor_text(&bytes))
}

fn decode_vendor_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_owned(),
        Err(_) => bytes.iter().copied().map(windows_1252_char).collect(),
    }
}

fn windows_1252_char(b: u8) -> char {
    const C1: [char; 32] = [
        '€', '\u{81}', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', '\u{8D}', 'Ž',
        '\u{8F}', '\u{90}', '‘', '’', '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', '\u{9D}',
        'ž', 'Ÿ',
    ];
    match b {
        0x80..=0x9F => C1[(b - 0x80) as usize],
        _ => char::from(b),
    }
}

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

/// Shared PSS/E single-quote rules used by [`tokenize`] and [`split_comment`].
///
/// Decks commonly embed apostrophes inside bus names without doubling them
/// (`'Q'Bus 1'`). A naive toggle on every `'` swallows the rest of the
/// line into one token. Closing a quoted field only when the next significant
/// character is `,` (or EOS), and treating `''` as an escaped apostrophe,
/// preserves both well-formed and legacy undoubled names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuoteClass {
    Enter,
    Leave,
    EscapedApostrophe,
    LiteralApostrophe,
}

fn classify_quote(chars: &[char], i: usize, in_quotes: bool) -> QuoteClass {
    if !in_quotes {
        QuoteClass::Enter
    } else if i + 1 < chars.len() && chars[i + 1] == '\'' {
        QuoteClass::EscapedApostrophe
    } else {
        let mut j = i + 1;
        while j < chars.len() && chars[j].is_whitespace() {
            j += 1;
        }
        if j >= chars.len() || chars[j] == ',' {
            QuoteClass::Leave
        } else {
            QuoteClass::LiteralApostrophe
        }
    }
}

/// Quote-aware comma tokeniser.  Strips surrounding single quotes from each
/// token and trims leading/trailing whitespace.  Does NOT split at commas
/// that appear inside a quoted string.
fn tokenize(line: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut token = String::new();
    let mut in_quotes = false;
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0usize;

    while i < chars.len() {
        let ch = chars[i];
        match ch {
            '\'' => match classify_quote(&chars, i, in_quotes) {
                QuoteClass::Enter => in_quotes = true,
                QuoteClass::Leave => in_quotes = false,
                QuoteClass::EscapedApostrophe => {
                    token.push('\'');
                    i += 1;
                }
                QuoteClass::LiteralApostrophe => token.push('\''),
            },
            ',' if !in_quotes => {
                tokens.push(token.trim().to_string());
                token.clear();
            }
            _ => token.push(ch),
        }
        i += 1;
    }
    // Push the last token (may be empty for trailing comma)
    let t = token.trim().to_string();
    if !t.is_empty() || !tokens.is_empty() {
        tokens.push(t);
    }
    tokens
}

/// Byte index of the first `/` that is not inside a single-quoted field.
fn first_unquoted_slash(line: &str) -> Option<usize> {
    let indexed: Vec<(usize, char)> = line.char_indices().collect();
    let chars: Vec<char> = indexed.iter().map(|&(_, c)| c).collect();
    let mut in_quotes = false;
    let mut i = 0usize;
    while i < indexed.len() {
        let (byte_idx, ch) = indexed[i];
        match ch {
            '\'' => match classify_quote(&chars, i, in_quotes) {
                QuoteClass::Enter => in_quotes = true,
                QuoteClass::Leave => in_quotes = false,
                QuoteClass::EscapedApostrophe => i += 1,
                QuoteClass::LiteralApostrophe => {}
            },
            '/' if !in_quotes => return Some(byte_idx),
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split a line at the first **unquoted** `/` into `(data, hint)`.
/// The `hint` may contain a section-transition marker like
/// `"END OF BUS DATA, BEGIN LOAD DATA"`.
///
/// `/` inside a single-quoted field is data (e.g. `'N/1 '`).
fn split_comment(line: &str) -> (&str, &str) {
    match first_unquoted_slash(line) {
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

fn field_present(fields: &[String], idx: usize) -> bool {
    fields
        .get(idx)
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
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

/// Winding-1 control block. A 3-rating record puts COD at index 6.
/// A 12-rating record (nine extra RATE columns) puts COD at index 15.
/// A complete 3-rating line ends near CNXA (~17 tokens). Lines at least 22
/// tokens long are the 12-rating layout. NOMV stays at index 1 either way.
struct WindingControl {
    cod: i32,
    cont: i32,
    rma: f64,
    rmi: f64,
    ntp: i32,
}

fn winding_control(fields: &[String]) -> WindingControl {
    let cod_idx = if fields.len() >= 22 { 15 } else { 6 };
    WindingControl {
        cod: field_i32(fields, cod_idx),
        cont: field_i32(fields, cod_idx + 1),
        rma: field_f64(fields, cod_idx + 2),
        rmi: field_f64(fields, cod_idx + 3),
        ntp: field_i32(fields, cod_idx + 6),
    }
}

fn token_looks_float(token: &str) -> bool {
    let t = token.trim();
    t.contains('.') || t.contains('e') || t.contains('E') || t.contains('d') || t.contains('D')
}

fn token_looks_alpha(token: &str) -> bool {
    token.trim().chars().any(|c| c.is_ascii_alphabetic())
}

/// PSS/E bus type tokens are always a single digit 1–4 in modern RAW decks.
/// Used to detect an **extra** v34/v35 field (e.g. substation name) inserted
/// between `BASKV` and `IDE` without mis-parsing area/zone integers as IDE.
fn strict_psse_bus_ide_token(t: &str) -> Option<u8> {
    match t.trim() {
        "1" | "2" | "3" | "4" => t.trim().parse().ok(),
        _ => None,
    }
}

/// Return the token index of the `IDE` field for this bus record.
///
/// PSS/E v35+ may insert an optional alphanumeric field (substation / metadata)
/// immediately after `BASKV`, before `IDE`.  When the token after `BASKV` is
/// not a strict `1`…`4`, treat the following token as `IDE` instead.
fn resolve_bus_ide_index(f: &[String], baskv_idx: usize, psse_version: u32) -> usize {
    if psse_version < 35 {
        return baskv_idx + 1;
    }
    if f.get(baskv_idx + 1)
        .and_then(|s| strict_psse_bus_ide_token(s))
        .is_some()
    {
        baskv_idx + 1
    } else if f
        .get(baskv_idx + 2)
        .and_then(|s| strict_psse_bus_ide_token(s))
        .is_some()
    {
        baskv_idx + 2
    } else {
        baskv_idx + 1
    }
}

fn psse_bus_ide_raw_to_type(ide_raw: u8) -> BusType {
    // Authoritative PSS/E PSSE-33/35 IDE field semantics (Bus Data Section):
    //   1 = Load bus (no generator boundary condition)        → canonical PQ  (RPF type 1)
    //   2 = Generator/plant bus (voltage-regulating; PV)       → canonical PV  (RPF type 2)
    //   3 = Swing bus                                          → canonical Slack (RPF type 3)
    //   4 = Disconnected / isolated bus                        → treated as PQ (RPF type 1)
    // Notes:
    //   • PSS/E does *not* define a "PQ generator" IDE code. A machine that doesn't
    //     regulate voltage still lives at an IDE=1 (load) bus or IDE=2 bus with
    //     reactive scheduling — the bus IDE itself is unaffected.
    //   • IDE=4 buses are folded into the PQ pool to mirror the raptrix-core RAW
    //     parser convention (`type = (IDE == 4) ? 1 : IDE`); the converter's
    //     deterministic-slack pass later drops them into the export untouched.
    match ide_raw {
        1 => BusType::LoadBus,
        2 => BusType::GeneratorPV,
        3 => BusType::Slack,
        4 => BusType::LoadBus,
        _ => BusType::LoadBus,
    }
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
///
/// PSS/E v35+ may insert an optional field between `BASKV` and `IDE` (handled
/// via [`resolve_bus_ide_index`]); all fields after `IDE` are indexed from
/// the resolved `IDE` position so `AREA`…`VA` stay aligned.
fn parse_bus_record(f: &[String], psse_version: u32) -> Option<Bus> {
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
    let ide_idx = resolve_bus_ide_index(f, baskv_idx, psse_version);
    let (gl_idx, bl_idx, area_idx, zone_idx, owner_idx, vm_idx, va_idx) = {
        let modern_area_idx = ide_idx + 1;
        let modern_zone_idx = ide_idx + 2;
        let modern_owner_idx = ide_idx + 3;
        let modern_vm_idx = ide_idx + 4;
        let modern_va_idx = ide_idx + 5;

        // Some legacy/variant RAW exports include inline GL/BL in BUS records:
        // I, NAME, BASKV, IDE, GL, BL, AREA, ZONE, OWNER, VM, VA, ...
        // Use a conservative heuristic so v33/v35 layouts remain unchanged.
        let has_inline_shunt = if f.len() > modern_va_idx + 2 {
            let old_area_idx = ide_idx + 3;
            let old_zone_idx = ide_idx + 4;
            let old_owner_idx = ide_idx + 5;
            let old_vm_idx = ide_idx + 6;

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

            let gl_token = f.get(ide_idx + 1).map(|s| s.as_str()).unwrap_or("");
            let bl_token = f.get(ide_idx + 2).map(|s| s.as_str()).unwrap_or("");
            let shunt_tokens_floaty = token_looks_float(gl_token) || token_looks_float(bl_token);

            old_score > modern_score || (old_score == modern_score && shunt_tokens_floaty)
        } else {
            false
        };

        if has_inline_shunt {
            (
                Some(ide_idx + 1),
                Some(ide_idx + 2),
                ide_idx + 3,
                ide_idx + 4,
                ide_idx + 5,
                ide_idx + 6,
                ide_idx + 7,
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

    let ide_raw = if psse_version >= 35 {
        f.get(ide_idx)
            .and_then(|s| strict_psse_bus_ide_token(s))
            .unwrap_or_else(|| field_u8(f, ide_idx))
    } else {
        field_u8(f, ide_idx)
    };
    let ide = psse_bus_ide_raw_to_type(ide_raw);

    let vm_raw = field_f64(f, vm_idx);

    // NV/EV limits: pass through parsed tokens (PSS/E defaults apply when fields are absent → 0.0).
    let nvhi = field_f64(f, va_idx + 1);
    let nvlo = field_f64(f, va_idx + 2);
    let evhi = field_f64(f, va_idx + 3);
    let evlo = field_f64(f, va_idx + 4);

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
        ip_available: field_present(f, 7),
        iq: field_f64(f, 8),
        iq_available: field_present(f, 8),
        yp: field_f64(f, 9),
        yp_available: field_present(f, 9),
        yq: field_f64(f, 10),
        yq_available: field_present(f, 10),
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
/// PSS/E v35 inserts `NREG` at index 8 and `BASLOD` before the owner block.
/// After PB: `O1,F1,O2,F2,O3,F3,O4,F4,WMOD,WPF` (v33–v35).
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

    // Blank PT (exactly 0) falls back to MBASE. A negative PT is a real cap
    // when PT = PB = PG < 0. Negative PB is legal. Do not rewrite either one to 0.
    let pg = field_f64(f, 2);
    let pt = {
        let raw = field_f64(f, off.gen_pt_idx);
        if raw == 0.0 { mbase } else { raw }
    };
    let pb = {
        let raw = field_f64(f, off.gen_pb_idx);
        if raw > pt { pt } else { raw }
    };

    Some(Generator {
        i,
        id: field_str(f, 1).into_boxed_str(),
        pg,
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
        wmod: field_u8(f, off.gen_wmod_idx),
        wpf: field_f64(f, off.gen_wpf_idx),
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
        if let Ok(v) = token.trim().parse::<i64>()
            && v > 0
        {
            bus_indices.push(idx);
            buses.push(v as u32);
            if buses.len() == 2 {
                break;
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

fn token_is_number(token: &str) -> bool {
    let t = token.trim().trim_matches('\'');
    if t.is_empty() {
        return false;
    }
    // parse_fortran_double turns a non-numeric token into 0.0, so it cannot
    // tell a name from a number.
    t.replace(['D', 'd'], "e").parse::<f64>().is_ok()
}

/// Named PSS/E two-terminal control record: `'NAME', MDC, RDC, SETVL, VSCHD, ...`.
/// MDC=1 is power control. The megawatts are rectifier DC power and the kilovolts
/// are inverter DC voltage.
fn is_psse_lcc_control(f: &[String]) -> bool {
    if f.len() < 5 || token_is_number(&f[0]) {
        return false;
    }
    token_to_positive_u32(&f[1]) == Some(1)
        && token_to_f64(&f[2]).is_some_and(|v| v.is_finite())
        && token_to_f64(&f[3]).is_some_and(|v| v > 0.0)
        && token_to_f64(&f[4]).is_some_and(|v| v > 0.0)
}

/// Fields stored from a recognized rectifier or inverter row.
///
/// Recognition still requires a bus, bridge count in 1..=12, and EBAS > 0.
/// Ratio, tap, and commutating reactance are stored when the token is a real
/// number, including 0. A missing or non-numeric token stays null. ANMX/ANMN
/// are limits and are not copied into a firing angle.
struct LccTerminal {
    bus_id: u32,
    n_bridges: Option<i32>,
    xc_ohm: Option<f64>,
    ebas_kv: Option<f64>,
    tr: Option<f64>,
    tap: Option<f64>,
    tap_max: Option<f64>,
    tap_min: Option<f64>,
}

/// A real number at `idx`, or `None` when the token is missing or not numeric.
/// `parse_fortran_double` turns a name into 0, so this uses [`token_is_number`].
/// A parsed 0 is returned as 0.0. This does not substitute 1.0.
fn optional_number(fields: &[String], idx: usize) -> Option<f64> {
    let token = fields.get(idx)?;
    if !token_is_number(token) {
        return None;
    }
    let t = token.trim().trim_matches('\'');
    t.replace(['D', 'd'], "e")
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
}

/// Confirm a rectifier or inverter row and return the terminal fields.
/// Bridge count and commutating kV are required so a control record is not
/// paired with an unrelated line.
fn parse_psse_lcc_terminal(row: &[String]) -> Option<LccTerminal> {
    if row.len() < 9 {
        return None;
    }
    let bus = token_to_positive_u32(&row[0])?;
    let bridges = token_to_positive_u32(&row[1])?;
    if !(1..=12).contains(&bridges) {
        return None;
    }
    let ebas = token_to_f64(&row[6])?;
    if ebas <= 0.0 {
        return None;
    }
    Some(LccTerminal {
        bus_id: bus,
        n_bridges: Some(bridges as i32),
        xc_ohm: optional_number(row, 5),
        ebas_kv: Some(ebas),
        tr: optional_number(row, 7),
        tap: optional_number(row, 8),
        tap_max: optional_number(row, 9),
        tap_min: optional_number(row, 10),
    })
}

/// METER token `I` names the inverter. `R` names the rectifier. Anything else
/// is unknown and meters neither end.
fn meter_role(token: &str) -> Option<&'static str> {
    match token.trim().trim_matches('\'').trim() {
        "I" | "i" => Some("inverter"),
        "R" | "r" => Some("rectifier"),
        _ => None,
    }
}

fn lcc_converter(dc_line_id: i32, end: &LccTerminal, role: &str, meter: bool) -> DcConverter {
    DcConverter {
        dc_line_id,
        bus_id: end.bus_id,
        role: role.into(),
        converter_kind: "lcc".into(),
        n_bridges: end.n_bridges,
        ebas_kv: end.ebas_kv,
        tr: end.tr,
        tap: end.tap,
        tap_min: end.tap_min,
        tap_max: end.tap_max,
        xc_ohm: end.xc_ohm,
        alpha_deg: None,
        gamma_deg: None,
        is_meter_end: meter,
    }
}

/// One PSS/E LCC line is three records: control, rectifier, inverter.
/// `from_bus_id` is the rectifier and `to_bus_id` is the inverter.
/// `p_setpoint_mw` is rectifier DC power and `v_setpoint_kv` is inverter DC voltage.
/// `METER` records which end is metered and does not move `SETVL`.
fn parse_psse_lcc_triplet(
    group: &[Vec<String>],
    dc_line_id: i32,
) -> Option<(DcLine2W, [DcConverter; 2])> {
    if group.len() != 3 || !is_psse_lcc_control(&group[0]) {
        return None;
    }
    let ctrl = &group[0];
    let rdc = token_to_f64(&ctrl[2])?;
    let setvl = token_to_f64(&ctrl[3])?;
    let vschd = token_to_f64(&ctrl[4])?;
    let from = parse_psse_lcc_terminal(&group[1])?;
    let to = parse_psse_lcc_terminal(&group[2])?;
    if from.bus_id == to.bus_id {
        return None;
    }
    let metered = ctrl.get(8).and_then(|t| meter_role(t));
    let name = ctrl[0].trim().to_string();
    let line = DcLine2W {
        dc_line_id,
        from_bus_id: from.bus_id,
        to_bus_id: to.bus_id,
        ckt: "1".into(),
        r_ohm: rdc,
        l_henry: None,
        control_mode: "power".into(),
        p_setpoint_mw: Some(setvl),
        i_setpoint_ka: None,
        v_setpoint_kv: Some(vschd),
        q_from_mvar: None,
        q_to_mvar: None,
        status: true,
        name: if name.is_empty() {
            None
        } else {
            Some(name.into())
        },
        converter_type: "lcc".into(),
    };
    let ends = [
        lcc_converter(dc_line_id, &from, "rectifier", metered == Some("rectifier")),
        lcc_converter(dc_line_id, &to, "inverter", metered == Some("inverter")),
    ];
    Some((line, ends))
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
            if let Ok(v) = tok.trim().parse::<i32>()
                && (20..=40).contains(&v)
            {
                psse_version = v;
                break;
            }
        }
    }

    // Last resort: infer v33 from base MVA
    if psse_version < 0
        && let Some(mva) = f.get(1).and_then(|s| s.parse::<f64>().ok())
        && mva > 1.0
        && mva < 1.0e6
    {
        psse_version = 33;
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
        cw: 1,
        cz: 1,
        cm: 1,
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
        cod1: 0,
        cont1: 0,
        rma1: 0.0,
        rmi1: 0.0,
        ntp1: 0,
    }
}

fn bus_baskv(buses: &[Bus], bus_id: u32, nomv_fallback: f64) -> f64 {
    buses
        .iter()
        .find(|b| b.i == bus_id)
        .map(|b| b.baskv)
        .filter(|v| *v > 1.0e-9)
        .unwrap_or(nomv_fallback)
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

/// Raw `ST` token tallies and line counts for the BRANCH deck section only.
///
/// Populated by [`parse_raw_with_branch_deck_stats`] alongside a full parse.
/// `status_token_histogram` keys are the integer tokens at the PSS/E `ST`
/// column (version-aware index); values are how often that token appeared on
/// non-terminator BRANCH lines with enough fields to read that column.
#[derive(Debug, Default, Clone)]
pub struct BranchDeckStats {
    /// Non-terminator data lines seen while `ParseState::Branch` is active.
    pub branch_section_lines: usize,
    /// Histogram of the raw `ST` column integer (before `parse_branch_record`
    /// maps non-zero values to in-service).
    pub status_token_histogram: std::collections::BTreeMap<i32, usize>,
    /// Lines in the BRANCH section where [`parse_branch_record`] returned `None`.
    pub rejected_branch_lines: usize,
}

/// Parse a PSS/E RAW file and return the [`Network`] plus BRANCH-section deck
/// statistics (raw `ST` token histogram, rejected lines).
pub fn parse_raw_with_branch_deck_stats(path: &Path) -> Result<(Network, BranchDeckStats)> {
    let mut deck = BranchDeckStats::default();
    let network = parse_raw_impl(path, Some(&mut deck))?;
    Ok((network, deck))
}

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
    parse_raw_impl(path, None)
}

fn parse_raw_impl(path: &Path, mut branch_diag: Option<&mut BranchDeckStats>) -> Result<Network> {
    let text = read_vendor_text(path, "RAW")?;
    let mut lines_iter = text.lines();

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
    let mut lcc_group: Vec<Vec<String>> = Vec::new();

    loop {
        let raw_line = match next_line(&mut lines_iter) {
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
            if state == ParseState::TwoTerminalDc && !lcc_group.is_empty() {
                dc_rows_rejected += lcc_group.len();
                lcc_group.clear();
            }
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
                let _ = next_line(&mut lines_iter);
                let _ = next_line(&mut lines_iter);

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
                if let Some(bus) = parse_bus_record(&f, psse_version) {
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
                let branch_off = branch_offsets_for_record(&f, psse_version);
                if let Some(d) = branch_diag.as_mut() {
                    d.branch_section_lines += 1;
                    if f.len() > branch_off.branch_status_idx
                        && let Ok(v) = f[branch_off.branch_status_idx].trim().parse::<i32>()
                    {
                        *d.status_token_histogram.entry(v).or_insert(0) += 1;
                    }
                }
                if let Some(branch) = parse_branch_record(&f, &branch_off) {
                    result.branches.push(branch);
                } else if let Some(d) = branch_diag.as_mut() {
                    d.rejected_branch_lines += 1;
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
                let cm = field_u8(&f1, 6);
                let mag1 = field_f64(&f1, 7);
                let mag2 = field_f64(&f1, 8);
                let stat = field_u8_default(&f1, 11, 1);
                let tx_id = TransformerId {
                    i: i_bus,
                    j: j_bus,
                    k: k_bus,
                };
                validate_transformer_codes(tx_id, cw, cz, cm)?;

                // Always read lines 2, 3, 4 (and 5 for 3W) regardless of status,
                // so the line iterator stays synchronised with the file.
                let l2 = match next_line(&mut lines_iter) {
                    None => break,
                    Some(l) => l,
                };
                let l3 = match next_line(&mut lines_iter) {
                    None => break,
                    Some(l) => l,
                };
                let l4 = match next_line(&mut lines_iter) {
                    None => break,
                    Some(l) => l,
                };
                let l5 = if k_bus != 0 {
                    match next_line(&mut lines_iter) {
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
                let wind = winding_control(&f3);

                if k_bus == 0 {
                    // ---- 2-winding transformer ----
                    result.transformers.push(TwoWindingTransformer {
                        i: i_bus,
                        j: j_bus,
                        ckt: ckt.into_boxed_str(),
                        cw,
                        cz,
                        cm,
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
                        cod1: wind.cod,
                        cont1: wind.cont,
                        rma1: wind.rma,
                        rmi1: wind.rmi,
                        ntp1: wind.ntp,
                    });
                } else {
                    // ---- 3-winding transformer → star equivalent ----
                    let f5 = tokenize(l5.unwrap().trim());

                    // Record 2 (3W): R1-2, X1-2, SBASE1-2, R2-3, X2-3, SBASE2-3, R3-1, X3-1, SBASE3-1
                    let r23 = field_f64(&f2, 3);
                    let x23 = field_f64(&f2, 4);
                    let sbase23 = field_f64(&f2, 5);
                    let r31 = field_f64(&f2, 6);
                    let x31 = field_f64(&f2, 7);
                    let sbase31 = field_f64(&f2, 8);

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

                    // Convert each pairwise Z on its own SBASE, then star-decompose
                    // in one base. Applying the 2W SBASE1-2 scale to all three pairs
                    // is wrong when SBASE2-3 / SBASE3-1 differ.
                    let sbase_sys = if result.case_id.sbase.abs() > 1.0e-9 {
                        result.case_id.sbase
                    } else {
                        100.0
                    };
                    let (r12_sys, x12_sys) =
                        convert_z_to_system(tx_id, cz, r12, x12, sbase12, sbase_sys)?;
                    let (r23_sys, x23_sys) =
                        convert_z_to_system(tx_id, cz, r23, x23, sbase23, sbase_sys)?;
                    let (r31_sys, x31_sys) =
                        convert_z_to_system(tx_id, cz, r31, x31, sbase31, sbase_sys)?;

                    let baskv_h = bus_baskv(&result.buses, i_bus, nomv1);
                    let baskv_m = bus_baskv(&result.buses, j_bus, nomv2);
                    let baskv_l = bus_baskv(&result.buses, k_bus, nomv3);
                    let tap_h = winding_pu_of_baskv(tx_id, cw, windv1, nomv1, baskv_h)?;
                    let tap_m = winding_pu_of_baskv(tx_id, cw, windv2, nomv2, baskv_m)?;
                    let tap_l = winding_pu_of_baskv(tx_id, cw, windv3, nomv3, baskv_l)?;

                    // Star-delta impedance decomposition (system-base pairs)
                    let za_r = 0.5 * (r12_sys + r31_sys - r23_sys);
                    let za_x = 0.5 * (x12_sys + x31_sys - x23_sys);
                    let zb_r = 0.5 * (r12_sys + r23_sys - r31_sys);
                    let zb_x = 0.5 * (x12_sys + x23_sys - x31_sys);
                    let zc_r = 0.5 * (r23_sys + r31_sys - r12_sys);
                    let zc_x = 0.5 * (x23_sys + x31_sys - x12_sys);

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
                        r_hm: r12_sys,
                        x_hm: x12_sys,
                        r_hl: r31_sys,
                        x_hl: x31_sys,
                        r_ml: r23_sys,
                        x_ml: x23_sys,
                        tap_h,
                        tap_m,
                        tap_l,
                        phase_shift_deg: ang1,
                        rate_a_mva: rate,
                        rate_b_mva: rate_b,
                        rate_c_mva: rate_c,
                        nominal_kv_h: nomv1,
                        nominal_kv_m: nomv2,
                        nominal_kv_l: nomv3,
                        cod1: wind.cod,
                        cont1: wind.cont,
                        rma1: wind.rma,
                        rmi1: wind.rmi,
                        ntp1: wind.ntp,
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
                        i_bus, star_id, 1, za_r, za_x, tap_h, ang1, rate, sbase_sys, stat,
                    ));
                    result.transformers.push(star_leg_transformer(
                        j_bus, star_id, 2, zb_r, zb_x, tap_m, ang2, rate, sbase_sys, stat,
                    ));
                    result.transformers.push(star_leg_transformer(
                        k_bus, star_id, 3, zc_r, zc_x, tap_l, ang3, rate, sbase_sys, stat,
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
                if lcc_group.is_empty() {
                    if is_psse_lcc_control(&f) {
                        lcc_group.push(f);
                    } else if let Some(row) = parse_dc_line_record(&f, next_dc_line_id, "lcc") {
                        result.dc_lines_2w.push(row);
                        next_dc_line_id += 1;
                    } else {
                        dc_rows_rejected += 1;
                    }
                } else {
                    lcc_group.push(f);
                    if lcc_group.len() == 3 {
                        if let Some((row, ends)) =
                            parse_psse_lcc_triplet(&lcc_group, next_dc_line_id)
                        {
                            result.dc_lines_2w.push(row);
                            result.dc_converters.extend(ends);
                            next_dc_line_id += 1;
                        } else {
                            dc_rows_rejected += lcc_group.len();
                        }
                        lcc_group.clear();
                    }
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

    if !lcc_group.is_empty() {
        dc_rows_rejected += lcc_group.len();
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
    use std::io::Write;

    use super::{parse_facts_record, parse_raw, parse_raw_with_branch_deck_stats, split_comment};

    fn minimal_raw_tail() -> &'static str {
        r#"0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA
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
"#
    }

    #[test]
    fn split_comment_keeps_slash_inside_quoted_name() {
        let line = "  351,'N/1 ', 115.0000,1,  67,  67,   1,1.00890040,  97.978183";
        let (data, hint) = split_comment(line);
        assert_eq!(data, line);
        assert!(hint.is_empty());
    }

    #[test]
    fn split_comment_still_splits_section_terminator_and_header() {
        let (data, hint) = split_comment("0 / END OF BUS DATA, BEGIN LOAD DATA");
        assert_eq!(data.trim(), "0");
        assert!(hint.contains("BEGIN LOAD DATA"));

        let (data, hint) = split_comment("0, 100.00, 33, 0, 1, 60.00 / September 20, 2022");
        assert!(data.contains("100.00"));
        assert!(hint.contains("September"));
    }

    #[test]
    fn split_comment_strips_trailing_comment_after_quoted_slash_name() {
        let line = "351,'N/1 ', 115.0000,1,  67,  67,   1,1.00890040 / leftover";
        let (data, hint) = split_comment(line);
        assert!(data.contains("1.00890040"));
        assert!(data.contains("N/1"));
        assert_eq!(hint.trim(), "leftover");
    }

    #[test]
    fn bus_name_with_embedded_slash_preserves_vm_va() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("slash_bus.raw");
        let raw = format!(
            r#"0, 100.0, 33, 0, 0, 60.0 / SLASH_BUS
T1
T2
351,'N/1 ', 115.0000,1,  67,  67,   1,1.00890040,  97.978183, 1.10000, 0.90000, 1.10000, 0.90000
1275,'S/~2', 115.0000,1,  65,  65,   1,1.02146566,  94.848269, 1.10000, 0.90000, 1.10000, 0.90000
101,'Q'Bus/1',  69.0000,1,   1,   1,   1,1.02000000,  12.500000, 1.10000, 0.90000, 1.10000, 0.90000
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
{tail}"#,
            tail = minimal_raw_tail()
        );
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(raw.as_bytes()).expect("write");

        let net = parse_raw(&path).expect("parse buses with slash in quoted names");
        let slash_name = net
            .buses
            .iter()
            .find(|b| b.i == 351)
            .expect("slash-name bus retained");
        assert!(
            slash_name.name.contains("N/1"),
            "name must keep '/', got {:?}",
            slash_name.name
        );
        assert!(
            (slash_name.vm - 1.00890040).abs() < 1e-8,
            "got vm {}",
            slash_name.vm
        );
        assert!(
            (slash_name.va - 97.978183).abs() < 1e-6,
            "got va {}",
            slash_name.va
        );
        assert!((slash_name.baskv - 115.0).abs() < 1e-9);
        assert_eq!(slash_name.area, 67);

        let tilde = net
            .buses
            .iter()
            .find(|b| b.i == 1275)
            .expect("tilde-name bus retained");
        assert!(tilde.name.contains("S/~2"));
        assert!((tilde.vm - 1.02146566).abs() < 1e-8);

        let apostrophe = net
            .buses
            .iter()
            .find(|b| b.i == 101)
            .expect("apostrophe-and-slash bus retained");
        assert!(
            apostrophe.name.contains("Q'Bus/1"),
            "apostrophe+slash name, got {:?}",
            apostrophe.name
        );
        assert!((apostrophe.vm - 1.02).abs() < 1e-8);
    }

    #[test]
    fn bus_name_with_embedded_apostrophe_preserves_vm_va() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("apostrophe_bus.raw");
        // Legacy undoubled quote inside a PSS/E name field.
        let raw = format!(
            r#"0, 100.0, 33, 0, 0, 60.0 / APOSTROPHE_BUS
T1
T2
101,'Q'Bus 1',  69.0000,1,   1,   1,   1,1.02000000,  12.500000, 1.10000, 0.90000, 1.10000, 0.90000
102,'Normal Bus  ',  22.0000,2,   1,   1,   1,1.01000000,  10.000000, 1.10000, 0.90000, 1.10000, 0.90000
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
{tail}"#,
            tail = minimal_raw_tail()
        );
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(raw.as_bytes()).expect("write");

        let net = parse_raw(&path).expect("parse bus with embedded apostrophe");
        let b = net
            .buses
            .iter()
            .find(|b| b.i == 101)
            .expect("apostrophe bus retained");
        assert!(
            b.name.starts_with("Q'Bus"),
            "name should keep embedded apostrophe, got {:?}",
            b.name
        );
        assert!(
            (b.vm - 1.02).abs() < 1e-8,
            "VM must not collapse to default; got {}",
            b.vm
        );
        assert!(
            (b.va - 12.5).abs() < 1e-6,
            "VA must not collapse to 0; got {}",
            b.va
        );
        assert!((b.baskv - 69.0).abs() < 1e-9);
    }

    #[test]
    fn windows_1252_smart_quotes_in_title_do_not_abort_parse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cp1252_title.raw");
        let mut raw = Vec::new();
        raw.extend_from_slice(b"0, 100.0, 33, 0, 0, 60.0 / CP1252\n");
        raw.extend_from_slice(b"Birchfield, \x93Dynamic\x94 grids\n");
        raw.extend_from_slice(b"T2\n");
        raw.extend_from_slice(
            b"101,'Test Bus     ',  69.0000,1,   1,   1,   1,1.00000000,   0.000000, 1.10000, 0.90000, 1.10000, 0.90000\n",
        );
        raw.extend_from_slice(b"0 / END OF BUS DATA, BEGIN LOAD DATA\n");
        raw.extend_from_slice(b"0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA\n");
        raw.extend_from_slice(b"0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA\n");
        raw.extend_from_slice(b"0 / END OF GENERATOR DATA, BEGIN BRANCH DATA\n");
        raw.extend_from_slice(minimal_raw_tail().as_bytes());
        std::fs::write(&path, &raw).expect("write");

        let net = parse_raw(&path).expect("Windows-1252 title must parse");
        assert_eq!(net.buses.len(), 1);
        assert_eq!(net.buses[0].i, 101);
    }

    #[test]
    fn generator_wmod_wpf_v33_full_owner_block() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gen_wmod_v33.raw");
        // v33: O1 @ 18, full owner block, WMOD @ 26, WPF @ 27. F1=1 (integer) must not become WMOD.
        let raw = format!(
            r#"0, 100.0, 33, 0, 0, 60.0 / GEN_WMOD_V33
T1
T2
100,'BUS1',230.0,2,1,1,1,1.02,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
100,'1',75.0,10.0,40.0,-20.0,1.02,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0,5,1,0,1.0000,0,1.0000,0,1.0000,0,0.95
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
{tail}"#,
            tail = minimal_raw_tail()
        );
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(raw.as_bytes()).expect("write");

        let net = parse_raw(&path).expect("parse v33 generator with full owner block");
        assert_eq!(net.generators.len(), 1);
        let machine = &net.generators[0];
        assert_eq!(machine.o1, 5);
        assert_eq!(machine.wmod, 0, "WMOD must not be read from integer F1=1");
        assert!((machine.wpf - 0.95).abs() < 1e-9);
    }

    #[test]
    fn generator_wmod_wpf_v35_with_baslod() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gen_wmod_v35.raw");
        // v35: NREG @ 8, BASLOD @ 19, O1 @ 20, WMOD @ 28, WPF @ 29.
        let raw = format!(
            r#"0, 100.0, 35, 0, 0, 60.0 / GEN_WMOD_V35
T1
T2
100,'BUS1',230.0,2,1,1,1,1.02,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
100,'1',75.0,10.0,40.0,-20.0,1.02,0,0,100.0,0.0,0.2,0.0,0.1,1.0,1,100.0,90.0,10.0,0,5,1,0,1.0000,0,1.0000,0,1.0000,2,0.95
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
{tail}"#,
            tail = minimal_raw_tail()
        );
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(raw.as_bytes()).expect("write");

        let net = parse_raw(&path).expect("parse v35 generator with BASLOD");
        assert_eq!(net.generators.len(), 1);
        let machine = &net.generators[0];
        assert_eq!(
            machine.o1, 5,
            "O1 must follow BASLOD, not read BASLOD as owner"
        );
        assert_eq!(machine.wmod, 2);
        assert!((machine.wpf - 0.95).abs() < 1e-9);
    }

    #[test]
    fn branch_deck_stats_v33_long_tail_uses_status_at_13() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("activsg_branch_v33.raw");
        // v33 branch row: RATEA at idx 6, ST at idx 13, long tap tail.
        let raw = r#"0, 100.0, 33, 0, 0, 60.0 / ACTIVSG_BRANCH_V33
T1
T2
10001,'BUS1',138.0,1,1,1,1,1.0,0.0,1.1,0.9,1.1,0.9
10002,'BUS2',138.0,1,1,1,1,1.0,0.0,1.1,0.9,1.1,0.9
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
10002, 10001,'1 ',5.74738E-2,2.97874E-1,4.55668E-2, 185.33,   0.00,   0.00,  0.00000,  0.00000,  0.00000,  0.00000,1,1,  46.8,   1,1.0000,   0,1.0000,   0,1.0000,   0,1.0000
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
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(raw.as_bytes()).expect("write");

        let (net, deck) =
            parse_raw_with_branch_deck_stats(&path).expect("parse with branch deck stats");
        assert_eq!(net.branches.len(), 1);
        assert_eq!(net.branches[0].st, 1);
        assert_eq!(deck.status_token_histogram.get(&1), Some(&1));
    }

    #[test]
    fn branch_deck_stats_v34_expanded_layout_uses_status_at_23() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("deck_v34_expanded.raw");
        // v34 header with expanded branch row (NAME + 12 ratings): STAT at idx 23.
        let raw = r#"0, 100.0, 34, 0, 0, 60.0 / V34_EXPANDED
T1
T2
1,'BUS1',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'BUS2',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1',0.01,0.05,0.0,'LINE1',100.0,110.0,120.0,130.0,140.0,150.0,160.0,170.0,180.0,190.0,200.0,210.0,0,0,0,0,1,1,1.0,1
1,2,'2',0.02,0.06,0.0,'LINE2',100.0,110.0,120.0,130.0,140.0,150.0,160.0,170.0,180.0,190.0,200.0,210.0,0,0,0,0,0,1,1.0,1
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
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(raw.as_bytes()).expect("write");

        let (net, deck) =
            parse_raw_with_branch_deck_stats(&path).expect("parse with branch deck stats");
        assert_eq!(net.branches.len(), 2);
        assert_eq!(net.branches[0].st, 1);
        assert_eq!(net.branches[1].st, 0);
        assert_eq!(deck.status_token_histogram.get(&0), Some(&1));
        assert_eq!(deck.status_token_histogram.get(&1), Some(&1));
    }

    #[test]
    fn branch_deck_stats_histogram_matches_branch_lines_v33() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("deck.raw");
        let raw = r#"0, 100.0, 33, 0, 0, 60.0 / BRANCH_DECK_TEST
T1
T2
1,'BUS1',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
2,'BUS2',230.0,1,1,1,1,1.00,0.00,1.10,0.90,1.10,0.90
0 / END OF BUS DATA, BEGIN LOAD DATA
0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA
0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA
0 / END OF GENERATOR DATA, BEGIN BRANCH DATA
1,2,'1',0.01,0.05,0.0,100.0,110.0,120.0,0,0,0,0,1,1,1.0,1
1,2,'2',0.02,0.06,0.0,100.0,110.0,120.0,0,0,0,0,0,1,1.0,1
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
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(raw.as_bytes()).expect("write");

        let (net, deck) =
            parse_raw_with_branch_deck_stats(&path).expect("parse with branch deck stats");
        assert_eq!(net.branches.len(), 2);
        assert_eq!(deck.branch_section_lines, 2);
        assert_eq!(deck.rejected_branch_lines, 0);
        assert_eq!(deck.status_token_histogram.get(&0), Some(&1));
        assert_eq!(deck.status_token_histogram.get(&1), Some(&1));
    }

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
    let text = read_vendor_text(path, "DYR")?;

    let mut records: Vec<DyrModelData> = Vec::new();
    let mut pending = String::new();

    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        let mut remaining = line;

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
    if !pending.is_empty()
        && let Some(rec) = try_parse_dyr_record(&pending)
    {
        records.push(rec);
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
