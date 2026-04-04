// raptrix-psse-rs
// Copyright (c) 2026 Musto Technologies LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! Zero-copy PSS/E data model.
//!
//! Each struct corresponds to one PSS/E RAW record section.  Field names match
//! the PSS/E v35 documentation exactly so that the C++ parser can map 1-to-1.
//! See [`docs/psse-mapping.md`] for the full field-by-field mapping to the
//! Raptrix PowerFlow Interchange schema.
//!
//! [`docs/psse-mapping.md`]: https://github.com/MustoTechnologies/raptrix-psse-rs/blob/main/docs/psse-mapping.md

// ---------------------------------------------------------------------------
// Top-level container
// ---------------------------------------------------------------------------

/// Complete PSS/E network case, populated by the parser.
///
/// All field names match PSS/E 35 documentation for 1-to-1 C++ mapping.
#[derive(Debug, Default)]
pub struct Network {
    /// Case identification record (section 0).
    pub case_id: CaseId,
    /// Bus data records (section 1).
    pub buses: Vec<Bus>,
    /// Load data records (section 2).
    pub loads: Vec<Load>,
    /// Fixed shunt data records (section 3).
    pub fixed_shunts: Vec<FixedShunt>,
    /// Generator data records (section 4).
    pub generators: Vec<Generator>,
    /// Non-transformer branch data records (section 5).
    pub branches: Vec<Branch>,
    /// Two-winding transformer data records (section 6; K=0, plus star legs for 3W).
    pub transformers: Vec<TwoWindingTransformer>,
    /// Area interchange data records (section 7).
    pub areas: Vec<Area>,
    /// Zone data records (section 13).
    pub zones: Vec<Zone>,
    /// Owner data records (section 15).
    pub owners: Vec<Owner>,
    /// Switched shunt data records (section 17).
    pub switched_shunts: Vec<SwitchedShunt>,
    /// Dynamic model records parsed from a paired `.dyr` file.
    pub dyr_generators: Vec<DyrGeneratorData>,
}

/// Backward-compatible alias used in tests and external tooling.
pub type RawCase = Network;

// ---------------------------------------------------------------------------
// Section 0 — Case identification
// ---------------------------------------------------------------------------

/// PSS/E case identification record (the first non-comment line in a RAW file).
#[derive(Debug, Default)]
pub struct CaseId {
    /// System MVA base (SBASE).
    pub sbase: f64,
    /// RAW file revision (REV), e.g. 33 or 35.
    pub rev: u32,
    /// Transformer rated voltage / system base voltage ratio (XFRRAT).
    pub xfrrat: u8,
    /// Nominal system frequency in Hz (BASFRQ), e.g. 60.0.
    pub basfrq: f64,
    /// Free-form case title / description (from the `/` comment on line 1).
    pub title: Box<str>,
}

// ---------------------------------------------------------------------------
// Section 1 — Bus data
// ---------------------------------------------------------------------------

/// Bus type codes as defined in the PSS/E documentation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BusType {
    /// Load bus / isolated (no generation, not a slack).
    #[default]
    LoadBus = 1,
    /// Generator bus (PQ bus — generator regulates Q).
    GeneratorPQ = 2,
    /// Generator bus (PV bus — generator regulates V).
    GeneratorPV = 3,
    /// Slack (swing) bus.
    Slack = 4,
}

/// PSS/E bus data record.
#[derive(Debug, Default)]
pub struct Bus {
    /// Bus number (I) — positive integer ≤ 999997.
    pub i: u32,
    /// Bus name (NAME) — up to 12 characters.
    pub name: Box<str>,
    /// Base voltage in kV (BASKV).
    pub baskv: f64,
    /// Bus type code (IDE).
    pub ide: BusType,
    /// Area number (AREA).
    pub area: u32,
    /// Zone number (ZONE).
    pub zone: u32,
    /// Owner number (OWNER).
    pub owner: u32,
    /// Per-unit voltage magnitude (VM).
    pub vm: f64,
    /// Voltage angle in degrees (VA).
    pub va: f64,
    /// Normal voltage high limit in per-unit (NVHI).
    pub nvhi: f64,
    /// Normal voltage low limit in per-unit (NVLO).
    pub nvlo: f64,
    /// Emergency voltage high limit in per-unit (EVHI).
    pub evhi: f64,
    /// Emergency voltage low limit in per-unit (EVLO).
    pub evlo: f64,
}

// ---------------------------------------------------------------------------
// Section 2 — Load data
// ---------------------------------------------------------------------------

/// PSS/E load data record.
#[derive(Debug, Default)]
pub struct Load {
    /// Bus number to which the load is connected (I).
    pub i: u32,
    /// Non-blank alphanumeric load identifier (ID), up to 2 characters.
    pub id: Box<str>,
    /// Load status: 1 = in service, 0 = out of service (STATUS).
    pub status: u8,
    /// Area to which the load is assigned (AREA).
    pub area: u32,
    /// Zone to which the load is assigned (ZONE).
    pub zone: u32,
    /// Active power component of constant power load in MW (PL).
    pub pl: f64,
    /// Reactive power component of constant power load in MVAr (QL).
    pub ql: f64,
    /// Active power component of constant current load in MW (IP).
    pub ip: f64,
    /// Reactive power component of constant current load in MVAr (IQ).
    pub iq: f64,
    /// Active power component of constant admittance load in MW (YP).
    pub yp: f64,
    /// Reactive power component of constant admittance load in MVAr (YQ).
    pub yq: f64,
    /// Owner to which the load is assigned (OWNER).
    pub owner: u32,
    /// Wind machine flag: 0 = load is not a wind machine (SCALE).
    pub scale: u8,
    /// Interruptible load flag (INTRPT).
    pub intrpt: u8,
}

// ---------------------------------------------------------------------------
// Section 3 — Fixed shunt data
// ---------------------------------------------------------------------------

/// PSS/E fixed shunt data record.
#[derive(Debug, Default)]
pub struct FixedShunt {
    /// Bus number (I).
    pub i: u32,
    /// Shunt identifier (ID), up to 2 characters.
    pub id: Box<str>,
    /// Shunt status: 1 = in service (STATUS).
    pub status: u8,
    /// Active component of shunt admittance in MW at unity voltage (GL).
    pub gl: f64,
    /// Reactive component of shunt admittance in MVAr at unity voltage (BL).
    pub bl: f64,
}

// ---------------------------------------------------------------------------
// Section 4 — Generator data
// ---------------------------------------------------------------------------

/// PSS/E generator data record.
#[derive(Debug, Default)]
pub struct Generator {
    /// Bus number (I).
    pub i: u32,
    /// Machine identifier (ID), up to 2 characters.
    pub id: Box<str>,
    /// Generator active power output in MW (PG).
    pub pg: f64,
    /// Generator reactive power output in MVAr (QG).
    pub qg: f64,
    /// Maximum reactive power output in MVAr (QT).
    pub qt: f64,
    /// Minimum reactive power output in MVAr (QB).
    pub qb: f64,
    /// Regulated bus voltage setpoint in per-unit (VS).
    pub vs: f64,
    /// Bus number of remotely regulated bus (IREG).
    pub ireg: u32,
    /// Total MVA base in MVA (MBASE).
    pub mbase: f64,
    /// Positive-sequence resistance in per-unit on MBASE (ZR).
    pub zr: f64,
    /// Positive-sequence reactance in per-unit on MBASE (ZX).
    pub zx: f64,
    /// Step-up transformer resistance (RT).
    pub rt: f64,
    /// Step-up transformer reactance (XT).
    pub xt: f64,
    /// Step-up transformer off-nominal turns ratio (GTAP).
    pub gtap: f64,
    /// Machine status: 1 = in service (STAT).
    pub stat: u8,
    /// Fraction of total MVAR range available for automatic reactive control (RMPCT).
    pub rmpct: f64,
    /// Maximum active power output in MW (PT).
    pub pt: f64,
    /// Minimum active power output in MW (PB).
    pub pb: f64,
    /// Owner number (O1).
    pub o1: u32,
    /// Wind machine flag (WMOD).
    pub wmod: u8,
    /// Power factor for WMOD modes 2 and 3 (WPF).
    pub wpf: f64,
}

// ---------------------------------------------------------------------------
// Section 5 — Non-transformer branch data
// ---------------------------------------------------------------------------

/// PSS/E non-transformer branch (line) data record.
#[derive(Debug, Default)]
pub struct Branch {
    /// "From" bus number (I).
    pub i: u32,
    /// "To" bus number (J).
    pub j: u32,
    /// Circuit identifier (CKT), up to 2 characters.
    pub ckt: Box<str>,
    /// Branch resistance in per-unit (R).
    pub r: f64,
    /// Branch reactance in per-unit (X).
    pub x: f64,
    /// Total line charging susceptance in per-unit (B).
    pub b: f64,
    /// First rating in MVA (RATEA).
    pub ratea: f64,
    /// Second rating in MVA (RATEB).
    pub rateb: f64,
    /// Third rating in MVA (RATEC).
    pub ratec: f64,
    /// Line shunt admittance — from-bus side, conductance (GI).
    pub gi: f64,
    /// Line shunt admittance — from-bus side, susceptance (BI).
    pub bi: f64,
    /// Line shunt admittance — to-bus side, conductance (GJ).
    pub gj: f64,
    /// Line shunt admittance — to-bus side, susceptance (BJ).
    pub bj: f64,
    /// Branch status: 1 = in service (ST).
    pub st: u8,
    /// Metered end flag (MET).
    pub met: u8,
    /// Line length in user-defined units (LEN).
    pub len: f64,
    /// Owner number (O1).
    pub o1: u32,
}

// ---------------------------------------------------------------------------
// Section 6 — Two-winding transformer data
// ---------------------------------------------------------------------------

/// PSS/E two-winding transformer data record (K = 0 on line 1).
///
/// Also used for star-equivalent legs generated from 3-winding transformers
/// (K ≠ 0) during parsing.
#[derive(Debug, Default)]
pub struct TwoWindingTransformer {
    /// "From" bus number — winding 1 (I).
    pub i: u32,
    /// "To" bus number — winding 2 (J).
    pub j: u32,
    /// Circuit identifier, up to 2 characters (CKT).
    pub ckt: Box<str>,
    /// Transformer status: 1 = in service (STAT).
    pub stat: u8,
    /// Magnetising conductance (MAG1, p.u. on system base).
    pub mag1: f64,
    /// Magnetising susceptance (MAG2, p.u. on system base).
    pub mag2: f64,
    // --- Line 2: leakage impedance on SBASE1-2 base ---
    /// Series resistance of the two-winding branch (R1-2).
    pub r12: f64,
    /// Series reactance of the two-winding branch (X1-2).
    pub x12: f64,
    /// Winding 1–2 MVA base (SBASE1-2).
    pub sbase12: f64,
    // --- Line 3: winding 1 (primary) ---
    /// Off-nominal turns ratio — winding 1 (WINDV1).
    pub windv1: f64,
    /// Nominal (rated) voltage of winding 1 in kV (NOMV1).
    pub nomv1: f64,
    /// Phase shift angle in degrees (ANG1).
    pub ang1: f64,
    /// Normal MVA rating — winding 1 (RATA1).
    pub rata1: f64,
    /// Emergency MVA rating — winding 1 (RATB1).
    pub ratb1: f64,
    /// Short-term MVA rating — winding 1 (RATC1).
    pub ratc1: f64,
    // --- Line 4: winding 2 (secondary) ---
    /// Off-nominal turns ratio — winding 2 (WINDV2).
    pub windv2: f64,
    /// Nominal voltage of winding 2 in kV (NOMV2).
    pub nomv2: f64,
}

// ---------------------------------------------------------------------------
// Section 7 — Area interchange data
// ---------------------------------------------------------------------------

/// PSS/E area interchange data record.
#[derive(Debug, Default)]
pub struct Area {
    /// Area number (I).
    pub i: u32,
    /// Swing bus number of the area (ISW).
    pub isw: u32,
    /// Desired net interchange leaving the area in MW (PDES).
    pub pdes: f64,
    /// Interchange tolerance bandwidth in MW (PTOL).
    pub ptol: f64,
    /// Area name, up to 12 characters (ARNAM).
    pub arnam: Box<str>,
}

// ---------------------------------------------------------------------------
// Section 13 — Zone data
// ---------------------------------------------------------------------------

/// PSS/E zone data record.
#[derive(Debug, Default)]
pub struct Zone {
    /// Zone number (I).
    pub i: u32,
    /// Zone name, up to 12 characters (ZONAM).
    pub zonam: Box<str>,
}

// ---------------------------------------------------------------------------
// Section 15 — Owner data
// ---------------------------------------------------------------------------

/// PSS/E owner data record.
#[derive(Debug, Default)]
pub struct Owner {
    /// Owner number (I).
    pub i: u32,
    /// Owner name, up to 12 characters (OWNAM).
    pub ownam: Box<str>,
}

// ---------------------------------------------------------------------------
// Section 17 — Switched shunt data
// ---------------------------------------------------------------------------

/// PSS/E switched shunt data record.
#[derive(Debug, Default)]
pub struct SwitchedShunt {
    /// Bus number (I).
    pub i: u32,
    /// Control mode (MODSW): 0 = locked, 1 = discrete, 2 = continuous.
    pub modsw: u8,
    /// Adjustment method (ADJM): 0 = steps, 1 = admittance.
    pub adjm: u8,
    /// Shunt status: 1 = in service (STAT).
    pub stat: u8,
    /// Voltage upper limit in per-unit (VSWHI).
    pub vswhi: f64,
    /// Voltage lower limit in per-unit (VSWLO).
    pub vswlo: f64,
    /// Remotely regulated bus number (SWREM).
    pub swrem: u32,
    /// Reactive power demand in MVAr (RMPCT).
    pub rmpct: f64,
    /// Remotely regulated bus name (RMIDNT), up to 12 characters.
    pub rmidnt: Box<str>,
    /// Initial reactive power output in MVAr (BINIT).
    pub binit: f64,
    /// Flat list of per-step susceptance values in MVAr: N_k copies of B_k
    /// for each Nk/Bk pair in the RAW record.
    pub steps: Vec<f64>,
}

// ---------------------------------------------------------------------------
// Dynamic model data — from `.dyr` file
// ---------------------------------------------------------------------------

/// Dynamic model data for one synchronous machine, parsed from a `.dyr` file.
///
/// Only GENROU, GENSAL, GENCLS (and their enhanced variants GENROE/GENSAE)
/// are extracted.  Exciter, governor, and PSS model records are ignored.
#[derive(Debug, Default, Clone)]
pub struct DyrGeneratorData {
    /// Bus number the machine is connected to.
    pub bus_id: u32,
    /// Machine identifier, up to 2 characters (matches [`Generator::id`]).
    pub id: Box<str>,
    /// Model name, e.g. `"GENROU"`, `"GENSAL"`, `"GENCLS"`.
    pub model: Box<str>,
    /// Inertia constant H in MW·s/MVA.
    pub h: f64,
    /// Damping coefficient D (pu torque / pu speed deviation).
    pub d: f64,
    /// d-axis transient reactance Xd′ in per-unit on machine base.
    pub xd_prime: f64,
}
