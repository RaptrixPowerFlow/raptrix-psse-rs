// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! CLI entry-point for `raptrix-psse-rs`.
//!
//! ## Subcommands
//! * `convert`  — parse a PSS/E `.raw` file (and optional `.dyr`) and write a
//!   Raptrix PowerFlow Interchange `.rpf` file.
//! * `view`     — pretty-print an existing `.rpf` file summary.
//! * `validate` — run MMWG §7.3 conformance checks on a PSS/E `.raw` file
//!   without writing any output (opt-in only; zero overhead on the convert path).

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

/// High-performance PSS/E → Raptrix PowerFlow Interchange converter.
#[derive(Parser, Debug)]
#[command(name = "raptrix-psse-rs")]
#[command(author = "Raptrix PowerFlow")]
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

        /// Transformer representation mode for 3-winding devices.
        ///
        /// - `native-3w`: export only native `transformers_3w` rows (default)
        /// - `expanded`: export only star-expanded `transformers_2w` legs
        #[arg(long, default_value = "native-3w")]
        transformer_mode: String,
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

    /// Run MMWG §7.3 power flow data quality checks on a PSS/E RAW file.
    ///
    /// Parses the RAW file but writes no output — prints a validation report to
    /// stderr.  Use `--strict` in CI to exit with code 1 on any errors.
    ///
    /// Example:
    ///   raptrix-psse-rs validate --raw case.raw
    ///   raptrix-psse-rs validate --raw case.raw --strict
    Validate {
        /// Path to the PSS/E RAW file (.raw) to validate.
        #[arg(long)]
        raw: PathBuf,

        /// Exit with code 1 if any ERROR-level issues are found (useful for CI).
        #[arg(long, default_value_t = false)]
        strict: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert {
            raw,
            dyr,
            output,
            transformer_mode,
        } => {
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

            let transformer_representation_mode =
                raptrix_psse_rs::TransformerRepresentationMode::from_cli_value(&transformer_mode)
                    .ok_or_else(|| {
                    anyhow::anyhow!(
                        "invalid --transformer-mode '{}'; expected one of: expanded, native-3w",
                        transformer_mode
                    )
                })?;
            let export_options = raptrix_psse_rs::ExportOptions {
                transformer_representation_mode,
            };

            raptrix_psse_rs::write_psse_to_rpf_with_options(
                raw_str,
                dyr_str,
                out_str,
                &export_options,
            )?;

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

        Commands::Validate { raw, strict } => {
            let raw_str = raw
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("RAW path is not valid UTF-8"))?;
            let report = raptrix_psse_rs::validate_psse_raw(raw_str)?;
            report.print_summary();
            if strict && !report.is_clean() {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
