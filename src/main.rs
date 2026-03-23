// Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
// Copyright (c) 2026 Musto Technologies LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! CLI entry-point for `raptrix-psse-rs`.
//!
//! ## Subcommands
//! * `convert` — parse a PSS/E `.raw` file (and optional `.dyr`) and write a
//!   Raptrix PowerFlow Interchange `.rpf` file.
//! * `view`    — pretty-print an existing `.rpf` file summary.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

/// High-performance PSS/E → Raptrix PowerFlow Interchange converter.
///
/// Raptrix CIM-Arrow — High-performance open CIM profile by Musto Technologies LLC
/// Copyright (c) 2026 Musto Technologies LLC
#[derive(Parser, Debug)]
#[command(name = "raptrix-psse-rs")]
#[command(author = "Musto Technologies LLC")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Parse a PSS/E case and write a Raptrix PowerFlow Interchange (.rpf) file.
    ///
    /// Example:
    ///   raptrix-psse-rs convert --raw tests/data/external/Texas7k_20210804.RAW --output case.rpf
    Convert {
        /// Path to the PSS/E RAW file (.raw).
        #[arg(long)]
        raw: PathBuf,

        /// Optional path to the PSS/E dynamic data file (.dyr).
        #[arg(long)]
        dyr: Option<PathBuf>,

        /// Output path for the Raptrix PowerFlow Interchange file (.rpf).
        #[arg(long)]
        output: PathBuf,
    },

    /// Pretty-print a Raptrix PowerFlow Interchange (.rpf) file summary.
    ///
    /// Example:
    ///   raptrix-psse-rs view --input case.rpf
    View {
        /// Path to the Raptrix PowerFlow Interchange file (.rpf).
        #[arg(long)]
        input: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert { raw, dyr, output } => {
            let raw_str = raw
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("RAW path is not valid UTF-8"))?;
            let dyr_str: Option<&str> = dyr.as_deref().and_then(|p| p.to_str());
            if let Some(d) = dyr_str {
                eprintln!("[raptrix-psse-rs] DYR file: {d}");
            }
            let out_str = output
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("output path is not valid UTF-8"))?;

            raptrix_psse_rs::write_psse_to_rpf(raw_str, dyr_str, out_str)?;

            let summary = raptrix_cim_arrow::summarize_rpf(&output)?;
            eprintln!(
                "[raptrix-psse-rs] Wrote {} — {} tables, {} total rows",
                output.display(),
                summary.tables.len(),
                summary.total_rows,
            );
            for t in &summary.tables {
                eprintln!("  {:30} {:6} rows", t.table_name, t.rows);
            }
        }

        Commands::View { input } => {
            let summary = raptrix_cim_arrow::summarize_rpf(&input)?;
            println!("RPF file: {}", input.display());
            println!(
                "  tables: {}  total rows: {}  all canonical: {}",
                summary.tables.len(),
                summary.total_rows,
                summary.has_all_canonical_tables,
            );
            for t in &summary.tables {
                println!("  {:30} {:6} rows", t.table_name, t.rows);
            }
        }
    }

    Ok(())
}
