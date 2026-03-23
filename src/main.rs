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
//! * `view`    — pretty-print an existing `.rpf` file.

use std::path::PathBuf;

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
    ///   raptrix-psse-rs convert --raw case.raw --dyr case.dyr --output case.rpf
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

    /// Pretty-print a Raptrix PowerFlow Interchange (.rpf) file.
    ///
    /// Example:
    ///   raptrix-psse-rs view --input case.rpf
    View {
        /// Path to the Raptrix PowerFlow Interchange file (.rpf).
        #[arg(long)]
        input: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert { raw, dyr, output } => {
            eprintln!("[raptrix-psse-rs] Parsing RAW file: {}", raw.display());
            if let Some(ref dyr_path) = dyr {
                eprintln!("[raptrix-psse-rs] Parsing DYR file: {}", dyr_path.display());
            }

            // TODO: invoke the real parser once ported from C++.
            let network = raptrix_psse_rs::parser::parse_raw(&raw)?;
            let _dyn_data = dyr
                .map(|p| raptrix_psse_rs::parser::parse_dyr(&p))
                .transpose()?;

            eprintln!(
                "[raptrix-psse-rs] Parsed {} buses. Writing {}…",
                network.buses.len(),
                output.display()
            );

            // TODO: encode `network` with raptrix_cim_arrow and write to `output`.
            raptrix_cim_arrow::write_rpf(&output, &[])?;

            eprintln!("[raptrix-psse-rs] Done.");
        }

        Commands::View { input } => {
            eprintln!("[raptrix-psse-rs] Reading {}", input.display());
            raptrix_cim_arrow::view_rpf(&input)?;
        }
    }

    Ok(())
}
