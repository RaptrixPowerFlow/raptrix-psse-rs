// Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
// Copyright (c) 2026 Musto Technologies LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! `raptrix-psse-rs` — High-performance PSS/E (`.raw` + `.dyr`) →
//! Raptrix PowerFlow Interchange v0.6.0 converter.
//!
//! # Crate layout
//! * [`models`] — zero-copy PSS/E data structures.
//! * [`parser`] — PSS/E `.raw` / `.dyr` parser (scaffold; C++ port in progress).
//!
//! The actual serialisation to `.rpf` is delegated to the
//! [`raptrix_cim_arrow`] shared crate.

pub mod models;
pub mod parser;
