// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.

//! CLI tool: BRANCH deck statistics, optional RAW↔RPF `branches` multiset diff.
//!
//! Example (Eastern 515GW or any large RAW):
//! ```text
//! cargo run --bin branch_deck_scan -- --raw path/to/case.raw --rpf path/to/case_static.rpf
//! ```

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::array::{AsArray, BooleanArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use clap::Parser;
use raptrix_cim_arrow::TABLE_BRANCHES;
use raptrix_psse_rs::parser::{BranchDeckStats, parse_raw_with_branch_deck_stats};
use raptrix_psse_rs::read_rpf_tables;

#[derive(Parser, Debug)]
#[command(name = "branch_deck_scan")]
struct Opt {
    /// PSS/E RAW file path.
    #[arg(long)]
    raw: PathBuf,

    /// Optional RPF from the same case (e.g. golden `*_static.rpf`) for multiset parity checks.
    #[arg(long)]
    rpf: Option<PathBuf>,

    /// Print every `(from,to,ckt)` that appears more than once among in-service RAW branches.
    #[arg(long, default_value_t = false)]
    list_duplicate_in_service_keys: bool,
}

type BranchKey = (i32, i32, String);

fn multiset_from_raw(network: &raptrix_psse_rs::models::Network) -> HashMap<BranchKey, usize> {
    let mut m: HashMap<BranchKey, usize> = HashMap::new();
    for b in &network.branches {
        if b.st == 0 {
            continue;
        }
        let k = (b.i as i32, b.j as i32, b.ckt.to_string());
        *m.entry(k).or_insert(0) += 1;
    }
    m
}

fn multiset_from_rpf_in_service(path: &std::path::Path) -> Result<HashMap<BranchKey, usize>> {
    let tables = read_rpf_tables(path).with_context(|| format!("read RPF {}", path.display()))?;
    let batch = tables
        .iter()
        .find(|(n, _)| n == TABLE_BRANCHES)
        .map(|(_, b)| b)
        .context("missing branches table")?;

    let from = batch
        .column_by_name("from_bus_id")
        .context("branches.from_bus_id")?
        .as_primitive::<arrow::datatypes::Int32Type>();
    let to = batch
        .column_by_name("to_bus_id")
        .context("branches.to_bus_id")?
        .as_primitive::<arrow::datatypes::Int32Type>();
    let status = batch
        .column_by_name("status")
        .context("branches.status")?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context("branches.status must be Boolean")?;

    let ckt_col = batch.column_by_name("ckt").context("branches.ckt")?;
    let ckt_utf8 = cast(ckt_col, &DataType::Utf8).context("cast branches.ckt to Utf8")?;
    let ckt = ckt_utf8.as_string::<i32>();

    let mut m: HashMap<BranchKey, usize> = HashMap::new();
    for row in 0..batch.num_rows() {
        if !status.value(row) {
            continue;
        }
        let k = (from.value(row), to.value(row), ckt.value(row).to_string());
        *m.entry(k).or_insert(0) += 1;
    }
    Ok(m)
}

fn multiset_all_rpf(path: &std::path::Path) -> Result<HashMap<BranchKey, usize>> {
    let tables = read_rpf_tables(path).with_context(|| format!("read RPF {}", path.display()))?;
    let batch = tables
        .iter()
        .find(|(n, _)| n == TABLE_BRANCHES)
        .map(|(_, b)| b)
        .context("missing branches table")?;

    let from = batch
        .column_by_name("from_bus_id")
        .context("branches.from_bus_id")?
        .as_primitive::<arrow::datatypes::Int32Type>();
    let to = batch
        .column_by_name("to_bus_id")
        .context("branches.to_bus_id")?
        .as_primitive::<arrow::datatypes::Int32Type>();
    let ckt_col = batch.column_by_name("ckt").context("branches.ckt")?;
    let ckt_utf8 = cast(ckt_col, &DataType::Utf8).context("cast branches.ckt to Utf8")?;
    let ckt = ckt_utf8.as_string::<i32>();

    let mut m: HashMap<BranchKey, usize> = HashMap::new();
    for row in 0..batch.num_rows() {
        let k = (from.value(row), to.value(row), ckt.value(row).to_string());
        *m.entry(k).or_insert(0) += 1;
    }
    Ok(m)
}

fn print_deck_stats(deck: &BranchDeckStats, network_branches: usize) {
    println!("--- BRANCH deck (RAW file) ---");
    println!("branch_section_lines: {}", deck.branch_section_lines);
    println!("rejected_branch_lines: {}", deck.rejected_branch_lines);
    println!("parsed branches (network): {network_branches}");
    println!("raw ST token histogram:");
    for (k, v) in &deck.status_token_histogram {
        println!("  ST={k}: {v}");
    }
}

fn main() -> Result<()> {
    let opt = Opt::parse();
    let (network, deck) = parse_raw_with_branch_deck_stats(&opt.raw)
        .with_context(|| format!("parse RAW {}", opt.raw.display()))?;

    let n_br = network.branches.len();
    let in_svc = network.branches.iter().filter(|b| b.st != 0).count();
    let out_svc = network.branches.iter().filter(|b| b.st == 0).count();

    print_deck_stats(&deck, n_br);
    println!("--- Model (after parse_branch_record) ---");
    println!("branches total: {n_br}");
    println!("in_service (st != 0): {in_svc}");
    println!("out_of_service (st == 0): {out_svc}");

    if opt.list_duplicate_in_service_keys {
        let mut counts: HashMap<BranchKey, usize> = HashMap::new();
        for b in &network.branches {
            if b.st == 0 {
                continue;
            }
            let k = (b.i as i32, b.j as i32, b.ckt.to_string());
            *counts.entry(k).or_insert(0) += 1;
        }
        let dups: Vec<_> = counts.iter().filter(|(_, c)| **c > 1).collect();
        println!("duplicate in-service keys (count > 1): {}", dups.len());
        for ((i, j, c), ctn) in dups.iter().take(50) {
            println!("  ({i},{j}) ckt='{c}' x{ctn}");
        }
        if dups.len() > 50 {
            println!("  ...");
        }
    }

    if let Some(ref rpf_path) = opt.rpf {
        let rpf_in = multiset_from_rpf_in_service(rpf_path)?;
        let raw_in = multiset_from_raw(&network);

        println!("--- RAW vs RPF (in-service keys only) ---");
        let keys_raw: HashSet<_> = raw_in.keys().cloned().collect();
        let keys_rpf: HashSet<_> = rpf_in.keys().cloned().collect();
        let only_rpf: Vec<_> = keys_rpf.difference(&keys_raw).cloned().collect();
        let only_raw: Vec<_> = keys_raw.difference(&keys_rpf).cloned().collect();
        println!("in_service key count RAW: {}", keys_raw.len());
        println!("in_service key count RPF: {}", keys_rpf.len());
        println!(
            "keys in RPF not in RAW (multiset mismatch): {}",
            only_rpf.len()
        );
        for k in only_rpf.iter().take(20) {
            let a = raw_in.get(k).copied().unwrap_or(0);
            let b = rpf_in.get(k).copied().unwrap_or(0);
            println!("  {:?}  raw_count={a} rpf_count={b}", k);
        }
        if only_rpf.len() > 20 {
            println!("  ...");
        }
        println!("keys in RAW not in RPF: {}", only_raw.len());
        for k in only_raw.iter().take(20) {
            let a = raw_in.get(k).copied().unwrap_or(0);
            let b = rpf_in.get(k).copied().unwrap_or(0);
            println!("  {:?}  raw_count={a} rpf_count={b}", k);
        }

        // Multiset multiplicity diff (same key, different counts)
        let mut mult_mismatch = 0usize;
        for k in keys_raw.union(&keys_rpf) {
            let a = raw_in.get(k).copied().unwrap_or(0);
            let b = rpf_in.get(k).copied().unwrap_or(0);
            if a != b {
                mult_mismatch += 1;
                if mult_mismatch <= 10 {
                    println!("multiset count mismatch: {:?} raw={a} rpf={b}", k);
                }
            }
        }
        if mult_mismatch > 10 {
            println!(
                "... {} keys total with multiplicity mismatch",
                mult_mismatch
            );
        }

        let rpf_all = multiset_all_rpf(rpf_path)?;
        let tables = read_rpf_tables(rpf_path)?;
        let batch = tables
            .iter()
            .find(|(n, _)| n == TABLE_BRANCHES)
            .map(|(_, b)| b)
            .context("missing branches table")?;
        let status = batch
            .column_by_name("status")
            .context("branches.status")?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .context("branches.status must be Boolean")?;
        let status_true = (0..batch.num_rows())
            .filter(|&row| status.value(row))
            .count();
        println!("RPF branches status=true: {status_true} / {}", batch.num_rows());
        println!("--- RPF row count sanity ---");
        println!("RPF branches rows: {}", rpf_all.values().sum::<usize>());
    }

    Ok(())
}
