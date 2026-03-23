// Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
// Copyright (c) 2026 Musto Technologies LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! Stub for the `raptrix-cim-arrow` shared crate.
//!
//! **Replace this entire crate** with the real implementation from the
//! `raptrix-cim-rs` workspace once that workspace is available alongside
//! this repository:
//!
//! ```toml
//! # Cargo.toml
//! raptrix-cim-arrow = { path = "../raptrix-cim-rs/raptrix-cim-arrow" }
//! ```
//!
//! The real crate exposes a zero-copy Arrow-based writer for the Raptrix
//! PowerFlow Interchange (`.rpf`) format v0.6.0.

/// Writes a Raptrix PowerFlow Interchange (`.rpf`) file.
///
/// # Stub behaviour
/// This stub always returns `Ok(())`.  The real implementation will encode
/// the supplied data into Apache Arrow IPC format and flush it to `path`.
pub fn write_rpf(_path: &std::path::Path, _data: &[u8]) -> std::io::Result<()> {
    // TODO: delegate to the real raptrix-cim-arrow writer once available.
    Ok(())
}

/// Reads and pretty-prints a Raptrix PowerFlow Interchange (`.rpf`) file.
///
/// # Stub behaviour
/// This stub always returns `Ok(())`.  The real implementation will decode
/// the Apache Arrow IPC payload and display a human-readable summary.
pub fn view_rpf(_path: &std::path::Path) -> std::io::Result<()> {
    // TODO: delegate to the real raptrix-cim-arrow reader once available.
    Ok(())
}
