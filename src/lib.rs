// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! `raptrix-psse-rs` â€” High-performance PSS/E (`.raw` + `.dyr`) â†’
//! Raptrix PowerFlow Interchange v0.8.7 converter.
//!
//! # Crate layout
//! * [`models`] â€” PSS/E data structures.
//! * [`parser`] â€” PSS/E `.raw` / `.dyr` parser.
//!
//! Serialisation to `.rpf` is delegated to the [`raptrix_cim_arrow`] crate.
//!
//! # Branding
//! raptrix-psse-rs
//! Copyright (c) 2026 Raptrix PowerFlow

pub mod models;
pub mod parser;
pub mod validation;

// Re-export reader utilities so tests and tools can use them directly.
pub use raptrix_cim_arrow::{RpfSummary, TableSummary, read_rpf_tables, summarize_rpf};

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};

use anyhow::{Context, Result};
use arrow::{
    array::{
        BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int8Builder, Int32Array,
        Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
        StringDictionaryBuilder, new_null_array,
    },
    datatypes::{Int32Type, UInt32Type},
    record_batch::RecordBatch,
};
use chrono::{SecondsFormat, Utc};
use raptrix_cim_arrow::{
    METADATA_KEY_CASE_FINGERPRINT, METADATA_KEY_CASE_MODE, METADATA_KEY_SOLVED_STATE_PRESENCE,
    METADATA_KEY_VALIDATION_MODE, RootWriteOptions, TABLE_AREAS, TABLE_BRANCHES, TABLE_BUSES,
    TABLE_CONTINGENCIES, TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS, TABLE_GENERATORS,
    TABLE_INTERFACES, TABLE_LOADS, TABLE_METADATA, TABLE_OWNERS, TABLE_SWITCHED_SHUNTS,
    TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, TABLE_ZONES, table_schema,
    write_root_rpf_with_metadata,
};

use crate::models::Network;

const METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE: &str = "rpf.transformer_representation_mode";
const SYNTHETIC_STAR_BUS_MIN_ID_EXCLUSIVE: u32 = 10_000_000;

/// Export-time policy for representing 3-winding transformers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransformerRepresentationMode {
    /// Export only star-expanded 2-winding legs for 3-winding devices.
    Expanded,
    /// Export only native `transformers_3w` rows for 3-winding devices.
    #[default]
    Native3W,
}

impl TransformerRepresentationMode {
    pub fn as_stable_str(self) -> &'static str {
        match self {
            TransformerRepresentationMode::Expanded => "expanded",
            TransformerRepresentationMode::Native3W => "native_3w",
        }
    }

    pub fn from_cli_value(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "expanded" => Some(TransformerRepresentationMode::Expanded),
            "native" | "native_3w" | "native-3w" => Some(TransformerRepresentationMode::Native3W),
            _ => None,
        }
    }
}

/// Export configuration for [`write_psse_to_rpf_with_options`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ExportOptions {
    /// Transformer representation mode used for this export run.
    pub transformer_representation_mode: TransformerRepresentationMode,
}

#[derive(Debug, Clone, Default)]
struct BusAggregate {
    p_sched: f64,
    q_sched: f64,
    q_min: f64,
    q_max: f64,
    g_shunt: f64,
    b_shunt: f64,
    p_min_agg: f64,
    p_max_agg: f64,
    has_generator: bool,
    v_mag_set_override: Option<f64>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse `raw_path` and write a Raptrix PowerFlow Interchange `.rpf` file.
///
/// # C++ port TODO
/// - Populate aggregated bus P/Q fields from loads and generators.
/// - Fill `g_shunt` / `b_shunt` from fixed-shunt records keyed by bus.
/// - Map PSS/E vector-group codes to CIM VectorGroup enum values.
/// - Extend solver-side interpretation for non-machine DYR model families.
///
/// Pass `dyr_path = None` when no dynamic data file is available.
pub fn write_psse_to_rpf(raw_path: &str, dyr_path: Option<&str>, output: &str) -> Result<()> {
    write_psse_to_rpf_with_options(raw_path, dyr_path, output, &ExportOptions::default())
}

/// Parse `raw_path` and write a Raptrix PowerFlow Interchange `.rpf` file
/// using explicit export options.
pub fn write_psse_to_rpf_with_options(
    raw_path: &str,
    dyr_path: Option<&str>,
    output: &str,
    options: &ExportOptions,
) -> Result<()> {
    let mut network = parser::parse_raw(std::path::Path::new(raw_path))
        .with_context(|| format!("failed to parse RAW file: {raw_path}"))?;

    if let Some(dyr) = dyr_path {
        network.dyr_models = parser::parse_dyr_records(std::path::Path::new(dyr))
            .with_context(|| format!("failed to parse DYR file: {dyr}"))?;
        network.dyr_generators = parser::extract_dyr_generators(&network.dyr_models);
    }

    normalize_transformer_representation(&mut network, options.transformer_representation_mode)?;

    // Build a (bus_id, machine_id) → DyrGeneratorData lookup for the generators table.
    let dyr_lookup: HashMap<(u32, String), &models::DyrGeneratorData> = network
        .dyr_generators
        .iter()
        .map(|r| ((r.bus_id, r.id.to_string()), r))
        .collect();

    let mut table_batches: HashMap<&'static str, RecordBatch> = HashMap::new();
    let bus_aggregates = build_bus_aggregates(&network);
    let connected_buses = build_connected_bus_set(&network);
    let bus_nominal_kv = build_bus_nominal_kv_map(&network);
    let base_mva = if network.case_id.sbase.abs() > 1.0e-9 {
        network.case_id.sbase
    } else {
        100.0
    };
    let case_fingerprint = compute_case_fingerprint(&network);
    // v0.8.5: detect warm vs flat start from RAW bus voltage state.
    let case_mode = detect_case_mode(&network);

    table_batches.insert(
        TABLE_METADATA,
        build_metadata_batch(&network, &case_fingerprint, case_mode)?,
    );
    table_batches.insert(
        TABLE_BUSES,
        build_buses_batch(&network.buses, &bus_aggregates, &connected_buses)?,
    );
    table_batches.insert(
        TABLE_BRANCHES,
        build_branches_batch(
            &network.branches,
            &network.facts_devices,
            &bus_nominal_kv,
            base_mva,
        )?,
    );
    table_batches.insert(
        TABLE_GENERATORS,
        build_generators_batch(&network.generators, &dyr_lookup, base_mva)?,
    );
    table_batches.insert(TABLE_LOADS, build_loads_batch(&network.loads, base_mva)?);
    table_batches.insert(
        TABLE_FIXED_SHUNTS,
        build_fixed_shunts_batch(&network.fixed_shunts, &network.buses, base_mva)?,
    );
    table_batches.insert(
        TABLE_SWITCHED_SHUNTS,
        build_switched_shunts_batch(&network.switched_shunts, base_mva)?,
    );
    table_batches.insert(
        TABLE_TRANSFORMERS_2W,
        build_transformers_2w_batch(&network.transformers, base_mva)?,
    );
    table_batches.insert(
        TABLE_TRANSFORMERS_3W,
        build_transformers_3w_batch(&network.transformers_3w, base_mva)?,
    );
    table_batches.insert(TABLE_AREAS, build_areas_batch(&network.areas)?);
    table_batches.insert(TABLE_ZONES, build_zones_batch(&network.zones)?);
    table_batches.insert(TABLE_OWNERS, build_owners_batch(&network.owners)?);
    table_batches.insert(TABLE_CONTINGENCIES, empty_table(TABLE_CONTINGENCIES)?);
    table_batches.insert(TABLE_INTERFACES, empty_table(TABLE_INTERFACES)?);
    let dynamics_batch = if network.dyr_models.is_empty() {
        empty_table(TABLE_DYNAMICS_MODELS)?
    } else {
        build_dynamics_models_batch(&network.dyr_models)?
    };
    table_batches.insert(TABLE_DYNAMICS_MODELS, dynamics_batch);

    validate_export_invariants(
        &table_batches,
        &connected_buses,
        options.transformer_representation_mode,
    )?;

    let root_options = RootWriteOptions {
        contingencies_are_stub: true,
        dynamics_are_stub: network.dyr_models.is_empty(),
        ..RootWriteOptions::default()
    };
    let mut additional_root_metadata = HashMap::new();
    additional_root_metadata.insert(
        METADATA_KEY_CASE_FINGERPRINT.to_string(),
        case_fingerprint.clone(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_VALIDATION_MODE.to_string(),
        "converter_export".to_string(),
    );
    // v0.8.5: case_mode — detected from RAW bus voltage state.
    additional_root_metadata.insert(METADATA_KEY_CASE_MODE.to_string(), case_mode.to_string());
    // v0.8.5: solved_state_presence — this converter never produces solved data.
    additional_root_metadata.insert(
        METADATA_KEY_SOLVED_STATE_PRESENCE.to_string(),
        "not_computed".to_string(),
    );
    additional_root_metadata.insert(
        METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE.to_string(),
        options
            .transformer_representation_mode
            .as_stable_str()
            .to_string(),
    );

    write_root_rpf_with_metadata(
        output,
        &table_batches,
        &root_options,
        &additional_root_metadata,
    )
    .with_context(|| format!("failed to write RPF file: {output}"))?;

    eprintln!("[converter] wrote {output}");
    Ok(())
}

/// Parse a PSS/E RAW file and run the full MMWG §7.3 validation suite
/// against the resulting network model.
///
/// This function never writes an output file — it is purely diagnostic.
/// For the default (speed-optimised) conversion path see [`write_psse_to_rpf`].
///
/// # Errors
/// Returns an error if the RAW file cannot be parsed.
pub fn validate_psse_raw(raw_path: &str) -> Result<validation::ValidationReport> {
    let network = parser::parse_raw(std::path::Path::new(raw_path))
        .with_context(|| format!("failed to parse RAW file: {raw_path}"))?;
    Ok(validation::run_mmwg_checks(&network))
}

#[allow(dead_code)]
fn emit_fast_diagnostics(network: &Network, dyr_path: Option<&str>) {
    let mut warnings: Vec<String> = Vec::new();

    let mut bus_ids: HashSet<u32> = HashSet::with_capacity(network.buses.len());
    for bus in &network.buses {
        bus_ids.insert(bus.i);
    }

    if network.buses.is_empty() {
        warnings.push("RAW produced 0 buses; case is likely invalid or empty".to_string());
    }
    if network.branches.is_empty() {
        warnings
            .push("RAW produced 0 branches; network may be disconnected or incomplete".to_string());
    }

    let slack_count = network
        .buses
        .iter()
        .filter(|b| b.ide == models::BusType::Slack)
        .count();
    if slack_count == 0 {
        warnings.push("no explicit slack bus (IDE=4) found in RAW".to_string());
    }

    let in_service_gen_count = network.generators.iter().filter(|g| g.stat != 0).count();
    if in_service_gen_count == 0 {
        warnings.push("no in-service generators found in RAW".to_string());
    }

    let dangling_loads = network
        .loads
        .iter()
        .filter(|l| l.status != 0 && !bus_ids.contains(&l.i))
        .count();
    if dangling_loads > 0 {
        warnings.push(format!(
            "{dangling_loads} in-service load records reference missing buses"
        ));
    }

    let dangling_fixed_shunts = network
        .fixed_shunts
        .iter()
        .filter(|s| s.status != 0 && !bus_ids.contains(&s.i))
        .count();
    if dangling_fixed_shunts > 0 {
        warnings.push(format!(
            "{dangling_fixed_shunts} in-service fixed-shunt records reference missing buses"
        ));
    }

    let dangling_generators = network
        .generators
        .iter()
        .filter(|g| g.stat != 0 && !bus_ids.contains(&g.i))
        .count();
    if dangling_generators > 0 {
        warnings.push(format!(
            "{dangling_generators} in-service generator records reference missing buses"
        ));
    }

    let dangling_switched_shunts = network
        .switched_shunts
        .iter()
        .filter(|s| s.stat != 0 && !bus_ids.contains(&s.i))
        .count();
    if dangling_switched_shunts > 0 {
        warnings.push(format!(
            "{dangling_switched_shunts} in-service switched-shunt records reference missing buses"
        ));
    }

    if let Some(dyr) = dyr_path {
        if network.dyr_generators.is_empty() {
            warnings.push(format!(
                "DYR file '{dyr}' produced 0 supported machine models (GENROU/GENSAL/GENCLS family)"
            ));
        }

        let mut in_service_gen_keys: HashSet<(u32, &str)> =
            HashSet::with_capacity(network.generators.len());
        for g in &network.generators {
            if g.stat != 0 {
                in_service_gen_keys.insert((g.i, g.id.as_ref()));
            }
        }

        let mut unmatched_dyr = 0usize;
        for dyn_rec in &network.dyr_generators {
            if !in_service_gen_keys.contains(&(dyn_rec.bus_id, dyn_rec.id.as_ref())) {
                unmatched_dyr += 1;
            }
        }
        if unmatched_dyr > 0 {
            warnings.push(format!(
                "{unmatched_dyr} DYR machine records do not match any in-service RAW generator (bus,id)"
            ));
        }

        let mut dyr_keys: HashSet<(u32, &str)> =
            HashSet::with_capacity(network.dyr_generators.len());
        for dyn_rec in &network.dyr_generators {
            dyr_keys.insert((dyn_rec.bus_id, dyn_rec.id.as_ref()));
        }

        let mut raw_without_dyr = 0usize;
        for g in &network.generators {
            if g.stat != 0 && !dyr_keys.contains(&(g.i, g.id.as_ref())) {
                raw_without_dyr += 1;
            }
        }
        if raw_without_dyr > 0 {
            warnings.push(format!(
                "{raw_without_dyr} in-service RAW generators have no matching supported DYR model"
            ));
        }
    }

    if warnings.is_empty() {
        eprintln!("[converter] fast validation: no obvious RAW/DYR completeness issues detected");
        return;
    }

    eprintln!(
        "[converter] fast validation: {} potential RAW/DYR completeness issue(s) detected:",
        warnings.len()
    );
    for warning in warnings {
        eprintln!("  - {warning}");
    }
}

// ---------------------------------------------------------------------------
// Helper: empty 0-row table
// ---------------------------------------------------------------------------

fn empty_table(name: &'static str) -> Result<RecordBatch> {
    let schema =
        table_schema(name).ok_or_else(|| anyhow::anyhow!("unknown canonical table: {name}"))?;
    Ok(RecordBatch::new_empty(Arc::new(schema)))
}

fn build_bus_nominal_kv_map(network: &Network) -> HashMap<u32, f64> {
    network
        .buses
        .iter()
        .map(|b| (b.i, b.baskv))
        .collect::<HashMap<_, _>>()
}

fn build_connected_bus_set(network: &Network) -> HashSet<u32> {
    let mut connected = HashSet::new();

    for branch in &network.branches {
        if branch.st != 0 {
            connected.insert(branch.i);
            connected.insert(branch.j);
        }
    }

    for transformer in &network.transformers {
        if transformer.stat != 0 {
            connected.insert(transformer.i);
            connected.insert(transformer.j);
        }
    }

    for generator in &network.generators {
        if generator.stat != 0 {
            connected.insert(generator.i);
        }
    }

    for load in &network.loads {
        if load.status != 0 {
            connected.insert(load.i);
        }
    }

    for shunt in &network.fixed_shunts {
        if shunt.status != 0 {
            connected.insert(shunt.i);
        }
    }

    for shunt in &network.switched_shunts {
        if shunt.stat != 0 {
            connected.insert(shunt.i);
        }
    }

    connected
}

fn normalize_transformer_representation(
    network: &mut Network,
    mode: TransformerRepresentationMode,
) -> Result<()> {
    let star_bus_ids: HashSet<u32> = network
        .transformers_3w
        .iter()
        .map(|t| t.star_bus_id)
        .collect();
    if star_bus_ids.is_empty() {
        return Ok(());
    }

    if mode == TransformerRepresentationMode::Native3W {
        // In native mode we must safely identify and remove only synthetic star legs.
        ensure_star_leg_mapping_is_resolvable(network)?;

        network
            .transformers
            .retain(|t| !(star_bus_ids.contains(&t.i) || star_bus_ids.contains(&t.j)));
    } else {
        network.transformers_3w.clear();
    }

    network.buses.retain(|b| !star_bus_ids.contains(&b.i));

    Ok(())
}

fn ensure_star_leg_mapping_is_resolvable(network: &Network) -> Result<()> {
    for tx3 in network.transformers_3w.iter().filter(|t| t.stat != 0) {
        let mut active_legs: Vec<(usize, u32)> = Vec::new();
        for (idx, tx2) in network.transformers.iter().enumerate() {
            if tx2.stat == 0 {
                continue;
            }
            if tx2.i == tx3.star_bus_id {
                active_legs.push((idx + 1, tx2.j));
            } else if tx2.j == tx3.star_bus_id {
                active_legs.push((idx + 1, tx2.i));
            }
        }

        if active_legs.is_empty() {
            continue;
        }

        let expected_endpoints: HashSet<u32> =
            [tx3.bus_h, tx3.bus_m, tx3.bus_l].into_iter().collect();
        let observed_endpoints: HashSet<u32> =
            active_legs.iter().map(|(_, other)| *other).collect();
        if active_legs.len() != 3 || observed_endpoints != expected_endpoints {
            let observed_rows: Vec<usize> = active_legs.iter().map(|(row, _)| *row).collect();
            let observed_buses: Vec<u32> = active_legs.iter().map(|(_, other)| *other).collect();
            anyhow::bail!(
                "export invariant violation: ambiguous 3-winding overlap for native mode (3w ckt='{}' buses=({}, {}, {}) star_bus_id={}) expected exactly 3 star legs to endpoint buses {:?}, found rows {:?} -> buses {:?}",
                tx3.ckt,
                tx3.bus_h,
                tx3.bus_m,
                tx3.bus_l,
                tx3.star_bus_id,
                expected_endpoints,
                observed_rows,
                observed_buses,
            );
        }
    }

    Ok(())
}

fn validate_export_invariants(
    table_batches: &HashMap<&'static str, RecordBatch>,
    connected_buses: &HashSet<u32>,
    transformer_mode: TransformerRepresentationMode,
) -> Result<()> {
    let buses = table_batches
        .get(TABLE_BUSES)
        .ok_or_else(|| anyhow::anyhow!("missing buses batch"))?;

    let bus_ids = buses
        .column_by_name("bus_id")
        .ok_or_else(|| anyhow::anyhow!("buses.bus_id missing"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("buses.bus_id is not Int32"))?;

    let v_mag_set = buses
        .column_by_name("v_mag_set")
        .ok_or_else(|| anyhow::anyhow!("buses.v_mag_set missing"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("buses.v_mag_set is not Float64"))?;

    let mut invalid: Vec<i32> = Vec::new();
    for i in 0..buses.num_rows() {
        let bus_id = bus_ids.value(i);
        if !connected_buses.contains(&(bus_id as u32)) {
            continue;
        }
        let vm = v_mag_set.value(i);
        if !vm.is_finite() || vm <= 0.0 {
            invalid.push(bus_id);
            if invalid.len() >= 8 {
                break;
            }
        }
    }

    if !invalid.is_empty() {
        anyhow::bail!(
            "export invariant violation: connected buses with nonpositive/invalid v_mag_set: {:?}",
            invalid
        );
    }

    let branches = table_batches
        .get(TABLE_BRANCHES)
        .ok_or_else(|| anyhow::anyhow!("missing branches batch"))?;
    validate_nonnegative_finite_column(branches, "rate_a", TABLE_BRANCHES)?;
    validate_nonnegative_finite_column(branches, "rate_b", TABLE_BRANCHES)?;
    validate_nonnegative_finite_column(branches, "rate_c", TABLE_BRANCHES)?;

    let transformers = table_batches
        .get(TABLE_TRANSFORMERS_2W)
        .ok_or_else(|| anyhow::anyhow!("missing transformers_2w batch"))?;
    validate_nonnegative_finite_column(transformers, "rate_a", TABLE_TRANSFORMERS_2W)?;
    validate_nonnegative_finite_column(transformers, "rate_b", TABLE_TRANSFORMERS_2W)?;
    validate_nonnegative_finite_column(transformers, "rate_c", TABLE_TRANSFORMERS_2W)?;

    let status = transformers
        .column_by_name("status")
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.status missing"))?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.status is not Boolean"))?;
    let tap_ratio = transformers
        .column_by_name("tap_ratio")
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.tap_ratio missing"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.tap_ratio is not Float64"))?;
    let nominal_tap_ratio = transformers
        .column_by_name("nominal_tap_ratio")
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.nominal_tap_ratio missing"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.nominal_tap_ratio is not Float64"))?;

    let mut invalid_tap_rows: Vec<usize> = Vec::new();
    let mut invalid_nominal_tap_rows: Vec<usize> = Vec::new();
    for i in 0..transformers.num_rows() {
        if !status.value(i) {
            continue;
        }
        let tap = tap_ratio.value(i);
        if !tap.is_finite() || tap <= 0.0 {
            invalid_tap_rows.push(i + 1);
            if invalid_tap_rows.len() >= 8 {
                break;
            }
        }
    }
    for i in 0..transformers.num_rows() {
        if !status.value(i) {
            continue;
        }
        let tap = nominal_tap_ratio.value(i);
        if !tap.is_finite() || tap <= 0.0 {
            invalid_nominal_tap_rows.push(i + 1);
            if invalid_nominal_tap_rows.len() >= 8 {
                break;
            }
        }
    }

    if !invalid_tap_rows.is_empty() {
        anyhow::bail!(
            "export invariant violation: in-service transformers with invalid tap_ratio at 1-based row(s): {:?}",
            invalid_tap_rows
        );
    }
    if !invalid_nominal_tap_rows.is_empty() {
        anyhow::bail!(
            "export invariant violation: in-service transformers with invalid nominal_tap_ratio at 1-based row(s): {:?}",
            invalid_nominal_tap_rows
        );
    }

    let transformers_3w = table_batches
        .get(TABLE_TRANSFORMERS_3W)
        .ok_or_else(|| anyhow::anyhow!("missing transformers_3w batch"))?;
    validate_nonnegative_finite_column(transformers_3w, "rate_a", TABLE_TRANSFORMERS_3W)?;
    validate_nonnegative_finite_column(transformers_3w, "rate_b", TABLE_TRANSFORMERS_3W)?;
    validate_nonnegative_finite_column(transformers_3w, "rate_c", TABLE_TRANSFORMERS_3W)?;

    let status_3w = transformers_3w
        .column_by_name("status")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.status missing"))?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.status is not Boolean"))?;
    let tap_h = transformers_3w
        .column_by_name("tap_h")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.tap_h missing"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.tap_h is not Float64"))?;
    let tap_m = transformers_3w
        .column_by_name("tap_m")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.tap_m missing"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.tap_m is not Float64"))?;
    let tap_l = transformers_3w
        .column_by_name("tap_l")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.tap_l missing"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.tap_l is not Float64"))?;

    let mut invalid_3w_taps: Vec<usize> = Vec::new();
    for i in 0..transformers_3w.num_rows() {
        if !status_3w.value(i) {
            continue;
        }
        let h = tap_h.value(i);
        let m = tap_m.value(i);
        let l = tap_l.value(i);
        if !h.is_finite() || !m.is_finite() || !l.is_finite() || h <= 0.0 || m <= 0.0 || l <= 0.0 {
            invalid_3w_taps.push(i + 1);
            if invalid_3w_taps.len() >= 8 {
                break;
            }
        }
    }
    if !invalid_3w_taps.is_empty() {
        anyhow::bail!(
            "export invariant violation: in-service transformers_3w with invalid tap_h/tap_m/tap_l at 1-based row(s): {:?}",
            invalid_3w_taps
        );
    }

    validate_transformer_representation_mode(transformers, transformers_3w, transformer_mode)?;

    Ok(())
}

fn validate_transformer_representation_mode(
    transformers_2w: &RecordBatch,
    transformers_3w: &RecordBatch,
    transformer_mode: TransformerRepresentationMode,
) -> Result<()> {
    let from_2w = transformers_2w
        .column_by_name("from_bus_id")
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.from_bus_id missing"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.from_bus_id is not Int32"))?;
    let to_2w = transformers_2w
        .column_by_name("to_bus_id")
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.to_bus_id missing"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.to_bus_id is not Int32"))?;
    let status_2w = transformers_2w
        .column_by_name("status")
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.status missing"))?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow::anyhow!("transformers_2w.status is not Boolean"))?;

    let bus_h_3w = transformers_3w
        .column_by_name("bus_h_id")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.bus_h_id missing"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.bus_h_id is not Int32"))?;
    let bus_m_3w = transformers_3w
        .column_by_name("bus_m_id")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.bus_m_id missing"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.bus_m_id is not Int32"))?;
    let bus_l_3w = transformers_3w
        .column_by_name("bus_l_id")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.bus_l_id missing"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.bus_l_id is not Int32"))?;
    let star_3w = transformers_3w
        .column_by_name("star_bus_id")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.star_bus_id missing"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.star_bus_id is not Int32"))?;
    let status_3w = transformers_3w
        .column_by_name("status")
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.status missing"))?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow::anyhow!("transformers_3w.status is not Boolean"))?;

    let mut active_star_to_endpoints: HashMap<i32, Vec<(usize, i32)>> = HashMap::new();
    for row in 0..transformers_2w.num_rows() {
        if !status_2w.value(row) {
            continue;
        }
        let from = from_2w.value(row);
        let to = to_2w.value(row);
        if from > SYNTHETIC_STAR_BUS_MIN_ID_EXCLUSIVE as i32 {
            active_star_to_endpoints
                .entry(from)
                .or_default()
                .push((row + 1, to));
        }
        if to > SYNTHETIC_STAR_BUS_MIN_ID_EXCLUSIVE as i32 {
            active_star_to_endpoints
                .entry(to)
                .or_default()
                .push((row + 1, from));
        }
    }

    let mut overlap_examples: Vec<String> = Vec::new();
    let mut active_3w_count = 0usize;
    for row in 0..transformers_3w.num_rows() {
        if !status_3w.value(row) {
            continue;
        }
        active_3w_count += 1;

        let star = star_3w.value(row);
        if let Some(legs) = active_star_to_endpoints.get(&star) {
            let expected: HashSet<i32> = [
                bus_h_3w.value(row),
                bus_m_3w.value(row),
                bus_l_3w.value(row),
            ]
            .into_iter()
            .collect();
            let observed: HashSet<i32> = legs.iter().map(|(_, other)| *other).collect();
            if legs.len() != 3 || observed != expected {
                let leg_rows: Vec<usize> = legs.iter().map(|(r, _)| *r).collect();
                let leg_buses: Vec<i32> = legs.iter().map(|(_, b)| *b).collect();
                anyhow::bail!(
                    "export invariant violation: ambiguous dual transformer materialization around star_bus_id={} (transformers_3w row={} expected buses {:?}, found transformers_2w rows {:?} -> buses {:?})",
                    star,
                    row + 1,
                    expected,
                    leg_rows,
                    leg_buses,
                );
            }
            overlap_examples.push(format!(
                "3w_row={} star_bus_id={} tx2w_rows={:?}",
                row + 1,
                star,
                legs.iter().map(|(r, _)| *r).collect::<Vec<_>>()
            ));
            if overlap_examples.len() >= 4 {
                break;
            }
        }
    }

    if !overlap_examples.is_empty() {
        anyhow::bail!(
            "export invariant violation: active transformers encode the same physical 3-winding unit in both forms: {:?}",
            overlap_examples
        );
    }

    match transformer_mode {
        TransformerRepresentationMode::Expanded => {
            if active_3w_count > 0 {
                anyhow::bail!(
                    "export invariant violation: transformer mode 'expanded' requires zero active transformers_3w rows, found {}",
                    active_3w_count
                );
            }
        }
        TransformerRepresentationMode::Native3W => {
            if !active_star_to_endpoints.is_empty() {
                let mut stars: Vec<i32> = active_star_to_endpoints.keys().copied().collect();
                stars.sort_unstable();
                let preview: Vec<i32> = stars.into_iter().take(8).collect();
                anyhow::bail!(
                    "export invariant violation: transformer mode 'native_3w' forbids active star-leg transformers_2w rows, found star bus IDs {:?}",
                    preview
                );
            }
        }
    }

    Ok(())
}

fn validate_nonnegative_finite_column(
    batch: &RecordBatch,
    column_name: &str,
    table_name: &str,
) -> Result<()> {
    let values = batch
        .column_by_name(column_name)
        .ok_or_else(|| anyhow::anyhow!("{}.{} missing", table_name, column_name))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("{}.{} is not Float64", table_name, column_name))?;

    let mut invalid_rows: Vec<usize> = Vec::new();
    for i in 0..batch.num_rows() {
        let v = values.value(i);
        if !v.is_finite() || v < 0.0 {
            invalid_rows.push(i + 1);
            if invalid_rows.len() >= 8 {
                break;
            }
        }
    }

    if !invalid_rows.is_empty() {
        anyhow::bail!(
            "export invariant violation: {}.{} has negative or non-finite value(s) at 1-based row(s): {:?}",
            table_name,
            column_name,
            invalid_rows
        );
    }

    Ok(())
}

fn compute_case_fingerprint(network: &Network) -> String {
    // Deterministic FNV-1a over core case identity fields and topology counts.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    network.case_id.rev.hash(&mut hasher);
    network.case_id.sbase.to_bits().hash(&mut hasher);
    network.case_id.basfrq.to_bits().hash(&mut hasher);
    network.case_id.title.hash(&mut hasher);
    network.buses.len().hash(&mut hasher);
    network.branches.len().hash(&mut hasher);
    network.generators.len().hash(&mut hasher);
    network.loads.len().hash(&mut hasher);
    network.transformers.len().hash(&mut hasher);
    network.buses.iter().for_each(|b| {
        b.i.hash(&mut hasher);
        b.vm.to_bits().hash(&mut hasher);
        b.va.to_bits().hash(&mut hasher);
    });
    format!("psse:{:016x}", hasher.finish())
}

/// Determine case_mode from RAW bus voltage state.
///
/// If all buses have vm ≈ 1.0 pu and va ≈ 0.0°, the RAW file is a flat-start
/// case and we export `flat_start_planning`.  Otherwise bus.vm / bus.va contain
/// a solved operating point from the RAW file so we export `warm_start_planning`
/// and preserve those values in the buses table v_mag_set / v_ang_set columns.
fn detect_case_mode(network: &Network) -> &'static str {
    let is_flat = network
        .buses
        .iter()
        .all(|b| (b.vm - 1.0).abs() < 1.0e-4 && b.va.abs() < 1.0e-4);
    if is_flat {
        "flat_start_planning"
    } else {
        "warm_start_planning"
    }
}

// ---------------------------------------------------------------------------
// Table builders
// ---------------------------------------------------------------------------

fn build_metadata_batch(
    network: &Network,
    case_fingerprint_value: &str,
    case_mode: &str,
) -> Result<RecordBatch> {
    let now_utc = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let schema = Arc::new(table_schema(TABLE_METADATA).expect("metadata schema must exist"));

    // simple scalar columns
    let base_mva = arrow::array::Float64Array::from(vec![network.case_id.sbase]);
    let frequency_hz = arrow::array::Float64Array::from(vec![network.case_id.basfrq]);
    let psse_version = arrow::array::Int32Array::from(vec![network.case_id.rev as i32]);
    let is_planning_case = arrow::array::BooleanArray::from(vec![true]);

    // dict string columns
    let mut study_name = StringDictionaryBuilder::<Int32Type>::new();
    study_name.append_value(network.case_id.title.as_ref());
    let mut source_case_id = StringDictionaryBuilder::<Int32Type>::new();
    source_case_id.append_value(network.case_id.title.as_ref());
    let mut validation_mode = StringDictionaryBuilder::<Int32Type>::new();
    validation_mode.append_value("converter_export");

    // plain string columns
    let mut timestamp_utc = StringBuilder::new();
    timestamp_utc.append_value(now_utc.as_str());
    let mut snapshot_timestamp_utc = StringBuilder::new();
    snapshot_timestamp_utc.append_value(now_utc.as_str());
    let mut raptrix_version = StringBuilder::new();
    raptrix_version.append_value(env!("CARGO_PKG_VERSION"));
    let mut case_fingerprint = StringBuilder::new();
    case_fingerprint.append_value(case_fingerprint_value);

    // custom_metadata is nullable â€” emit a single null value.
    let custom_meta_type = schema
        .field_with_name("custom_metadata")
        .expect("custom_metadata field must exist in metadata schema")
        .data_type()
        .clone();
    let custom_metadata = new_null_array(&custom_meta_type, 1);

    // v0.8.5: case_mode (required) — determined by caller based on RAW voltage state.
    let mut case_mode_arr = StringDictionaryBuilder::<Int32Type>::new();
    case_mode_arr.append_value(case_mode);

    // v0.8.5: solved_state_presence — this converter never produces solved data.
    let mut solved_state_presence_arr = StringDictionaryBuilder::<Int32Type>::new();
    solved_state_presence_arr.append_value("not_computed");

    // v0.8.5: solver provenance — all null for planning exports.
    let mut solver_version_arr = StringBuilder::new();
    solver_version_arr.append_null();
    let mut solver_iterations_arr = Int32Builder::new();
    solver_iterations_arr.append_null();
    let mut solver_accuracy_arr = Float64Builder::new();
    solver_accuracy_arr.append_null();
    let mut solver_mode_arr = StringDictionaryBuilder::<Int32Type>::new();
    solver_mode_arr.append_null();

    // v0.8.5: angle-reference metadata — all null for planning exports.
    let mut slack_bus_id_solved_arr = Int32Builder::new();
    slack_bus_id_solved_arr.append_null();
    let mut angle_reference_deg_arr = Float64Builder::new();
    angle_reference_deg_arr.append_null();
    // v0.8.5: solved_shunt_state_presence — null for planning exports;
    // only populated by the solver when case_mode=solved_snapshot.
    let mut solved_shunt_state_presence_arr = StringDictionaryBuilder::<Int32Type>::new();
    solved_shunt_state_presence_arr.append_null();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(base_mva),
            Arc::new(frequency_hz),
            Arc::new(psse_version),
            Arc::new(study_name.finish()),
            Arc::new(timestamp_utc.finish()),
            Arc::new(raptrix_version.finish()),
            Arc::new(is_planning_case),
            Arc::new(source_case_id.finish()),
            Arc::new(snapshot_timestamp_utc.finish()),
            Arc::new(case_fingerprint.finish()),
            Arc::new(validation_mode.finish()),
            custom_metadata,
            // v0.8.4 columns
            Arc::new(case_mode_arr.finish()),
            Arc::new(solved_state_presence_arr.finish()),
            Arc::new(solver_version_arr.finish()),
            Arc::new(solver_iterations_arr.finish()),
            Arc::new(solver_accuracy_arr.finish()),
            Arc::new(solver_mode_arr.finish()),
            // v0.8.5 columns
            Arc::new(slack_bus_id_solved_arr.finish()),
            Arc::new(angle_reference_deg_arr.finish()),
            Arc::new(solved_shunt_state_presence_arr.finish()),
        ],
    )
    .context("building metadata batch")
}

fn build_bus_aggregates(network: &Network) -> HashMap<u32, BusAggregate> {
    let base_mva = if network.case_id.sbase.abs() > 1.0e-9 {
        network.case_id.sbase
    } else {
        100.0
    };

    let mut agg_by_bus = HashMap::with_capacity(network.buses.len());
    for bus in &network.buses {
        let mut agg = BusAggregate {
            g_shunt: bus.gl / base_mva,
            b_shunt: bus.bl / base_mva,
            ..Default::default()
        };
        if bus.ide == models::BusType::LoadBus {
            agg.q_min = -9999.0;
            agg.q_max = 9999.0;
            agg.p_max_agg = 9999.0;
        }
        agg_by_bus.insert(bus.i, agg);
    }

    for shunt in &network.fixed_shunts {
        if shunt.status == 0 {
            continue;
        }
        if let Some(agg) = agg_by_bus.get_mut(&shunt.i) {
            agg.g_shunt += shunt.gl / base_mva;
            agg.b_shunt += shunt.bl / base_mva;
        }
    }

    for branch in &network.branches {
        if branch.st == 0 {
            continue;
        }
        if let Some(agg) = agg_by_bus.get_mut(&branch.i) {
            agg.g_shunt += branch.gi / base_mva;
            agg.b_shunt += branch.bi / base_mva;
        }
        if let Some(agg) = agg_by_bus.get_mut(&branch.j) {
            agg.g_shunt += branch.gj / base_mva;
            agg.b_shunt += branch.bj / base_mva;
        }
    }

    for load in &network.loads {
        if load.status == 0 {
            continue;
        }
        if let Some(agg) = agg_by_bus.get_mut(&load.i) {
            agg.p_sched -= load.pl / base_mva;
            agg.q_sched -= load.ql / base_mva;
        }
    }

    for generator in &network.generators {
        if generator.stat == 0 {
            continue;
        }
        if let Some(agg) = agg_by_bus.get_mut(&generator.i) {
            agg.p_sched += generator.pg / base_mva;
            agg.q_sched += generator.qg / base_mva;

            let qmin = generator.qb / base_mva;
            let qmax = generator.qt / base_mva;
            if agg.has_generator {
                agg.q_min = agg.q_min.min(qmin);
                agg.q_max = agg.q_max.max(qmax);
            } else {
                agg.q_min = qmin;
                agg.q_max = qmax;
                agg.has_generator = true;
            }

            agg.p_min_agg += generator.pb / base_mva;
            agg.p_max_agg += generator.pt / base_mva;

            if (0.85..1.15).contains(&generator.vs) {
                agg.v_mag_set_override = Some(generator.vs);
            }
        }
    }

    agg_by_bus
}

fn build_buses_batch(
    buses: &[models::Bus],
    agg_by_bus: &HashMap<u32, BusAggregate>,
    connected_buses: &HashSet<u32>,
) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_BUSES).expect("buses schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut name = StringDictionaryBuilder::<Int32Type>::new();
    let mut bus_type = Int8Builder::new();
    let mut p_sched = Float64Builder::new();
    let mut q_sched = Float64Builder::new();
    let mut v_mag_set = Float64Builder::new();
    let mut v_ang_set = Float64Builder::new();
    let mut q_min = Float64Builder::new();
    let mut q_max = Float64Builder::new();
    let mut g_shunt = Float64Builder::new();
    let mut b_shunt = Float64Builder::new();
    let mut area = Int32Builder::new();
    let mut zone = Int32Builder::new();
    let mut owner = Int32Builder::new();
    let mut v_min = Float64Builder::new();
    let mut v_max = Float64Builder::new();
    let mut p_min_agg = Float64Builder::new();
    let mut p_max_agg = Float64Builder::new();
    let mut nominal_kv = Float64Builder::new();
    let mut bus_uuid = StringDictionaryBuilder::<Int32Type>::new();

    let mut sanitized_count = 0usize;
    let mut sanitized_examples: Vec<u32> = Vec::new();

    for bus in buses {
        let agg = agg_by_bus.get(&bus.i).cloned().unwrap_or_default();
        let mut q_min_val = agg.q_min;
        let mut q_max_val = agg.q_max;
        if q_min_val > q_max_val {
            std::mem::swap(&mut q_min_val, &mut q_max_val);
        }

        let vm_raw = agg.v_mag_set_override.unwrap_or(bus.vm);
        let vm_sanitized = sanitize_bus_v_mag_set(vm_raw, bus.nvlo, bus.nvhi);
        if (vm_sanitized - vm_raw).abs() > 1.0e-12 {
            sanitized_count += 1;
            if sanitized_examples.len() < 8 {
                sanitized_examples.push(bus.i);
            }
        }

        bus_id.append_value(bus.i as i32);
        name.append_value(bus.name.as_ref());
        bus_type.append_value(bus.ide as i8);
        p_sched.append_value(agg.p_sched);
        q_sched.append_value(agg.q_sched);
        // v0.8.5: preserve RAW voltage setpoints for warm-start parity.
        // Use generator VS for regulated buses; fallback to RAW bus VM so
        // solved NYISO/external snapshots retain their initial conditions.
        v_mag_set.append_value(vm_sanitized);
        // Preserve RAW bus angle (PSS/E degrees → radians for raptrix-core).
        v_ang_set.append_value(bus.va.to_radians());
        q_min.append_value(q_min_val);
        q_max.append_value(q_max_val);
        g_shunt.append_value(agg.g_shunt);
        b_shunt.append_value(agg.b_shunt);
        area.append_value(bus.area as i32);
        zone.append_value(bus.zone as i32);
        owner.append_value(bus.owner as i32);
        v_min.append_value(bus.nvlo);
        v_max.append_value(bus.nvhi);
        p_min_agg.append_value(agg.p_min_agg);
        p_max_agg.append_value(agg.p_max_agg);
        nominal_kv.append_value(bus.baskv);
        bus_uuid.append_value(format!("psse:bus:{}", bus.i));

        if connected_buses.contains(&bus.i) && vm_sanitized <= 0.0 {
            anyhow::bail!(
                "connected bus {} would export nonpositive v_mag_set={}",
                bus.i,
                vm_sanitized
            );
        }
    }

    if sanitized_count > 0 {
        eprintln!(
            "[converter] sanitized {} bus voltage setpoint(s) to valid positive values (example bus IDs: {:?})",
            sanitized_count, sanitized_examples
        );
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(name.finish()),
            Arc::new(bus_type.finish()),
            Arc::new(p_sched.finish()),
            Arc::new(q_sched.finish()),
            Arc::new(v_mag_set.finish()),
            Arc::new(v_ang_set.finish()),
            Arc::new(q_min.finish()),
            Arc::new(q_max.finish()),
            Arc::new(g_shunt.finish()),
            Arc::new(b_shunt.finish()),
            Arc::new(area.finish()),
            Arc::new(zone.finish()),
            Arc::new(owner.finish()),
            Arc::new(v_min.finish()),
            Arc::new(v_max.finish()),
            Arc::new(p_min_agg.finish()),
            Arc::new(p_max_agg.finish()),
            Arc::new(nominal_kv.finish()),
            Arc::new(bus_uuid.finish()),
        ],
    )
    .context("building buses batch")
}

fn sanitize_bus_v_mag_set(vm_candidate: f64, nvlo: f64, nvhi: f64) -> f64 {
    let mut vm = if vm_candidate.is_finite() {
        vm_candidate
    } else {
        1.0
    };

    // Enforce a physically meaningful positive initialization.
    if vm <= 0.0 {
        vm = 1.0;
    }

    // If normal voltage bounds are valid and positive, keep v_mag_set inside them.
    if nvlo.is_finite() && nvhi.is_finite() && nvlo > 0.0 && nvhi >= nvlo {
        if vm < nvlo {
            vm = nvlo;
        } else if vm > nvhi {
            vm = nvhi;
        }
    }

    // Final safety net in case bounds were malformed.
    if !vm.is_finite() || vm <= 0.0 {
        1.0
    } else {
        vm
    }
}

fn build_branches_batch(
    branches: &[models::Branch],
    facts_devices: &[models::FactsDeviceRaw],
    bus_nominal_kv: &HashMap<u32, f64>,
    base_mva: f64,
) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_BRANCHES).expect("branches schema must exist"));

    let mut branch_id = Int32Builder::new();
    let mut from_bus_id = Int32Builder::new();
    let mut to_bus_id = Int32Builder::new();
    let mut ckt = StringDictionaryBuilder::<Int32Type>::new();
    let mut r = Float64Builder::new();
    let mut x = Float64Builder::new();
    let mut b_shunt = Float64Builder::new();
    let mut tap = Float64Builder::new();
    let mut phase = Float64Builder::new();
    let mut rate_a = Float64Builder::new();
    let mut rate_b = Float64Builder::new();
    let mut rate_c = Float64Builder::new();
    let mut status = BooleanBuilder::new();
    // name is nullable dict_utf8_u32
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
    let mut from_nominal_kv = Float64Builder::new();
    let mut to_nominal_kv = Float64Builder::new();

    let mut device_type = StringDictionaryBuilder::<Int32Type>::new();
    let mut control_mode = StringDictionaryBuilder::<Int32Type>::new();
    let mut control_target_flow_mw = Float64Builder::new();
    let mut x_min_pu = Float64Builder::new();
    let mut x_max_pu = Float64Builder::new();
    let mut injected_voltage_mag_pu = Float64Builder::new();
    let mut injected_voltage_angle_deg = Float64Builder::new();
    let map_field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut facts_params = MapBuilder::new(
        Some(map_field_names),
        StringBuilder::new(),
        Float64Builder::new(),
    );

    let mut facts_by_pair: HashMap<(u32, u32), Vec<&models::FactsDeviceRaw>> = HashMap::new();
    for facts in facts_devices {
        let key = if facts.bus_i <= facts.bus_j {
            (facts.bus_i, facts.bus_j)
        } else {
            (facts.bus_j, facts.bus_i)
        };
        facts_by_pair.entry(key).or_default().push(facts);
    }

    for (idx, branch) in branches.iter().enumerate() {
        branch_id.append_value((idx + 1) as i32);
        from_bus_id.append_value(branch.i as i32);
        to_bus_id.append_value(branch.j as i32);
        ckt.append_value(branch.ckt.as_ref());
        r.append_value(branch.r);
        x.append_value(branch.x);
        b_shunt.append_value(branch.b);
        tap.append_value(1.0); // PSS/E lines always have tap = 1.0
        phase.append_value(0.0); // no phase shift on line branches
        rate_a.append_value(branch.ratea / base_mva);
        rate_b.append_value(branch.rateb / base_mva);
        rate_c.append_value(branch.ratec / base_mva);
        status.append_value(branch.st != 0);
        name_b.append_null(); // branches have no name in RAW
        from_nominal_kv.append_option(bus_nominal_kv.get(&branch.i).copied());
        to_nominal_kv.append_option(bus_nominal_kv.get(&branch.j).copied());

        let pair_key = if branch.i <= branch.j {
            (branch.i, branch.j)
        } else {
            (branch.j, branch.i)
        };
        let matched_facts = facts_by_pair.get(&pair_key).and_then(|records| {
            if records.len() == 1 {
                Some(records[0])
            } else {
                None
            }
        });

        if let Some(facts) = matched_facts {
            device_type.append_value(facts.device_type.as_ref());
            control_mode.append_option(facts.control_mode.as_deref());
            control_target_flow_mw.append_option(facts.target_flow_mw);
            x_min_pu.append_option(facts.x_min_pu);
            x_max_pu.append_option(facts.x_max_pu);
            injected_voltage_mag_pu.append_option(facts.injected_voltage_mag_pu);
            injected_voltage_angle_deg.append_option(facts.injected_voltage_angle_deg);

            if facts.params.is_empty() {
                facts_params
                    .append(false)
                    .context("building branch facts_params null entry")?;
            } else {
                for (k, v) in &facts.params {
                    facts_params.keys().append_value(k.as_ref());
                    facts_params.values().append_value(*v);
                }
                facts_params
                    .append(true)
                    .context("building branch facts_params entry")?;
            }
        } else {
            device_type.append_null();
            control_mode.append_null();
            control_target_flow_mw.append_null();
            x_min_pu.append_null();
            x_max_pu.append_null();
            injected_voltage_mag_pu.append_null();
            injected_voltage_angle_deg.append_null();
            facts_params
                .append(false)
                .context("building branch facts_params null entry")?;
        }
    }

    let facts_params_arr = facts_params.finish();
    let facts_params_target_type = schema
        .field_with_name("facts_params")
        .expect("facts_params field must exist in branches schema")
        .data_type()
        .clone();
    let facts_params_cast = arrow::compute::cast(&facts_params_arr, &facts_params_target_type)
        .context("casting branches facts_params map")?;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(branch_id.finish()),
            Arc::new(from_bus_id.finish()),
            Arc::new(to_bus_id.finish()),
            Arc::new(ckt.finish()),
            Arc::new(r.finish()),
            Arc::new(x.finish()),
            Arc::new(b_shunt.finish()),
            Arc::new(tap.finish()),
            Arc::new(phase.finish()),
            Arc::new(rate_a.finish()),
            Arc::new(rate_b.finish()),
            Arc::new(rate_c.finish()),
            Arc::new(status.finish()),
            Arc::new(name_b.finish()),
            Arc::new(from_nominal_kv.finish()),
            Arc::new(to_nominal_kv.finish()),
            Arc::new(device_type.finish()),
            Arc::new(control_mode.finish()),
            Arc::new(control_target_flow_mw.finish()),
            Arc::new(x_min_pu.finish()),
            Arc::new(x_max_pu.finish()),
            Arc::new(injected_voltage_mag_pu.finish()),
            Arc::new(injected_voltage_angle_deg.finish()),
            facts_params_cast,
        ],
    )
    .context("building branches batch")
}

fn build_generators_batch(
    generators: &[models::Generator],
    dyr_lookup: &HashMap<(u32, String), &models::DyrGeneratorData>,
    base_mva: f64,
) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_GENERATORS).expect("generators schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut id = StringDictionaryBuilder::<Int32Type>::new();
    let mut p_sched_pu = Float64Builder::new();
    let mut p_min_pu = Float64Builder::new();
    let mut p_max_pu = Float64Builder::new();
    let mut q_min_pu = Float64Builder::new();
    let mut q_max_pu = Float64Builder::new();
    let mut status = BooleanBuilder::new();
    let mut mbase_mva = Float64Builder::new();
    let mut h = Float64Builder::new();
    let mut xd_prime = Float64Builder::new();
    let mut d = Float64Builder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();

    for generator in generators {
        bus_id.append_value(generator.i as i32);
        id.append_value(generator.id.as_ref());
        p_sched_pu.append_value(generator.pg / base_mva);
        p_min_pu.append_value(generator.pb / base_mva);
        p_max_pu.append_value(generator.pt / base_mva);
        q_min_pu.append_value(generator.qb / base_mva);
        q_max_pu.append_value(generator.qt / base_mva);
        status.append_value(generator.stat != 0);
        mbase_mva.append_value(generator.mbase);
        if let Some(dyn_data) = dyr_lookup.get(&(generator.i, generator.id.to_string())) {
            h.append_value(dyn_data.h);
            xd_prime.append_value(dyn_data.xd_prime);
            d.append_value(dyn_data.d);
        } else {
            h.append_value(0.0);
            xd_prime.append_value(generator.zx); // fallback: ZX as xd'
            d.append_value(0.0);
        }
        name_b.append_null();
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(id.finish()),
            Arc::new(p_sched_pu.finish()),
            Arc::new(p_min_pu.finish()),
            Arc::new(p_max_pu.finish()),
            Arc::new(q_min_pu.finish()),
            Arc::new(q_max_pu.finish()),
            Arc::new(status.finish()),
            Arc::new(mbase_mva.finish()),
            Arc::new(h.finish()),
            Arc::new(xd_prime.finish()),
            Arc::new(d.finish()),
            Arc::new(name_b.finish()),
        ],
    )
    .context("building generators batch")
}

fn build_loads_batch(loads: &[models::Load], base_mva: f64) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_LOADS).expect("loads schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut id = StringDictionaryBuilder::<Int32Type>::new();
    let mut status = BooleanBuilder::new();
    let mut p_pu = Float64Builder::new();
    let mut q_pu = Float64Builder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();

    for load in loads {
        bus_id.append_value(load.i as i32);
        id.append_value(load.id.as_ref());
        status.append_value(load.status != 0);
        p_pu.append_value(load.pl / base_mva);
        q_pu.append_value(load.ql / base_mva);
        name_b.append_null();
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(id.finish()),
            Arc::new(status.finish()),
            Arc::new(p_pu.finish()),
            Arc::new(q_pu.finish()),
            Arc::new(name_b.finish()),
        ],
    )
    .context("building loads batch")
}

fn build_fixed_shunts_batch(
    shunts: &[models::FixedShunt],
    buses: &[models::Bus],
    base_mva: f64,
) -> Result<RecordBatch> {
    let schema =
        Arc::new(table_schema(TABLE_FIXED_SHUNTS).expect("fixed_shunts schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut id = StringDictionaryBuilder::<Int32Type>::new();
    let mut status = BooleanBuilder::new();
    let mut g_pu = Float64Builder::new();
    let mut b_pu = Float64Builder::new();

    for shunt in shunts {
        bus_id.append_value(shunt.i as i32);
        id.append_value(shunt.id.as_ref());
        status.append_value(shunt.status != 0);
        g_pu.append_value(shunt.gl / base_mva);
        b_pu.append_value(shunt.bl / base_mva);
    }

    // Export inline bus GL/BL as synthetic fixed-shunt rows so downstream
    // readers that rebuild from fixed_shunts can recover full shunt injections.
    for bus in buses {
        if bus.gl.abs() <= 1.0e-12 && bus.bl.abs() <= 1.0e-12 {
            continue;
        }
        bus_id.append_value(bus.i as i32);
        id.append_value("1");
        status.append_value(true);
        g_pu.append_value(bus.gl / base_mva);
        b_pu.append_value(bus.bl / base_mva);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(id.finish()),
            Arc::new(status.finish()),
            Arc::new(g_pu.finish()),
            Arc::new(b_pu.finish()),
        ],
    )
    .context("building fixed_shunts batch")
}

fn estimate_current_step(target_binit: f64, steps: &[f64]) -> i32 {
    if steps.is_empty() {
        return 0;
    }

    let mut best_step = 0usize;
    let mut best_error = target_binit.abs();
    let mut cumulative = 0.0;

    for (idx, step) in steps.iter().enumerate() {
        cumulative += *step;
        let error = (cumulative - target_binit).abs();
        if error < best_error - 1.0e-12
            || ((error - best_error).abs() <= 1.0e-12 && (idx + 1) > best_step)
        {
            best_error = error;
            best_step = idx + 1;
        }
    }

    best_step as i32
}

fn build_switched_shunts_batch(
    shunts: &[models::SwitchedShunt],
    base_mva: f64,
) -> Result<RecordBatch> {
    let schema =
        Arc::new(table_schema(TABLE_SWITCHED_SHUNTS).expect("switched_shunts schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut status = BooleanBuilder::new();
    let mut v_low = Float64Builder::new();
    let mut v_high = Float64Builder::new();
    // b_steps: List<item: Float64, not-null>
    // Use with_field to match the schema's inner field nullability exactly.
    let inner_field = Arc::new(arrow::datatypes::Field::new(
        "item",
        arrow::datatypes::DataType::Float64,
        false,
    ));
    let mut b_steps = ListBuilder::new(Float64Builder::new()).with_field(inner_field);
    let mut current_step = Int32Builder::new();
    // v0.8.3: b_init_pu is the authoritative initial susceptance — written directly
    // from BINIT / base_mva so mixed-sign banks round-trip exactly regardless of step ordering.
    let mut b_init_pu = Float64Builder::new();
    // v0.8.5: shunt_id — stable per-bank identity; synthesized as "{bus_id}_shunt_{n}"
    // (1-indexed among banks sharing a bus).  Matches CIM ShuntCompensator mRID path.
    let mut shunt_id = StringDictionaryBuilder::<Int32Type>::new();
    // Track per-bus shunt index for synthesizing shunt_id.
    let mut bus_shunt_counter: std::collections::HashMap<u32, u32> =
        std::collections::HashMap::new();

    for shunt in shunts {
        bus_id.append_value(shunt.i as i32);
        status.append_value(shunt.stat != 0);
        v_low.append_value(shunt.vswlo);
        v_high.append_value(shunt.vswhi);

        let mut step_values_pu = Vec::with_capacity(shunt.steps.len());
        for &step_mvar in &shunt.steps {
            step_values_pu.push(step_mvar / base_mva);
        }
        // Append each step value to the inner list
        for &step_pu in &step_values_pu {
            b_steps.values().append_value(step_pu);
        }
        b_steps.append(true);

        let binit_pu = shunt.binit / base_mva;
        let step_count = estimate_current_step(binit_pu, &step_values_pu);
        current_step.append_value(step_count);
        b_init_pu.append_value(binit_pu);

        // v0.8.5: synthesize stable shunt_id — "{bus_id}_shunt_{n}" (1-indexed per bus).
        let n = {
            let cnt = bus_shunt_counter.entry(shunt.i).or_insert(0);
            *cnt += 1;
            *cnt
        };
        shunt_id.append_value(format!("{}_shunt_{}", shunt.i, n));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(status.finish()),
            Arc::new(v_low.finish()),
            Arc::new(v_high.finish()),
            Arc::new(b_steps.finish()),
            Arc::new(current_step.finish()),
            Arc::new(b_init_pu.finish()),
            Arc::new(shunt_id.finish()),
        ],
    )
    .context("building switched_shunts batch")
}

fn build_transformers_2w_batch(
    transformers: &[models::TwoWindingTransformer],
    base_mva: f64,
) -> Result<RecordBatch> {
    let schema =
        Arc::new(table_schema(TABLE_TRANSFORMERS_2W).expect("transformers_2w schema must exist"));

    let mut from_bus_id = Int32Builder::new();
    let mut to_bus_id = Int32Builder::new();
    let mut ckt = StringDictionaryBuilder::<Int32Type>::new();
    let mut r = Float64Builder::new();
    let mut x = Float64Builder::new();
    let mut winding1_r = Float64Builder::new();
    let mut winding1_x = Float64Builder::new();
    let mut winding2_r = Float64Builder::new();
    let mut winding2_x = Float64Builder::new();
    let mut g = Float64Builder::new();
    let mut b = Float64Builder::new();
    let mut tap_ratio = Float64Builder::new();
    let mut nominal_tap_ratio = Float64Builder::new();
    let mut phase_shift = Float64Builder::new();
    let mut vector_group = StringDictionaryBuilder::<Int32Type>::new();
    let mut rate_a = Float64Builder::new();
    let mut rate_b = Float64Builder::new();
    let mut rate_c = Float64Builder::new();
    let mut status = BooleanBuilder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
    let mut from_nominal_kv = Float64Builder::new();
    let mut to_nominal_kv = Float64Builder::new();

    for t in transformers {
        from_bus_id.append_value(t.i as i32);
        to_bus_id.append_value(t.j as i32);
        ckt.append_value(t.ckt.as_ref());
        r.append_value(t.r12);
        x.append_value(t.x12);
        winding1_r.append_value(0.0); // TODO: decompose per-winding impedance
        winding1_x.append_value(0.0);
        winding2_r.append_value(0.0);
        winding2_x.append_value(0.0);
        g.append_value(t.mag1);
        b.append_value(t.mag2);
        tap_ratio.append_value(t.windv1);
        nominal_tap_ratio.append_value(derive_nominal_tap_ratio(t));
        phase_shift.append_value(t.ang1.to_radians());
        append_vector_group(&mut vector_group, t);
        rate_a.append_value(t.rata1 / base_mva);
        rate_b.append_value(t.ratb1 / base_mva);
        rate_c.append_value(t.ratc1 / base_mva);
        status.append_value(t.stat != 0);
        name_b.append_null();
        from_nominal_kv.append_option(if t.nomv1 > 0.0 { Some(t.nomv1) } else { None });
        to_nominal_kv.append_option(if t.nomv2 > 0.0 { Some(t.nomv2) } else { None });
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(from_bus_id.finish()),
            Arc::new(to_bus_id.finish()),
            Arc::new(ckt.finish()),
            Arc::new(r.finish()),
            Arc::new(x.finish()),
            Arc::new(winding1_r.finish()),
            Arc::new(winding1_x.finish()),
            Arc::new(winding2_r.finish()),
            Arc::new(winding2_x.finish()),
            Arc::new(g.finish()),
            Arc::new(b.finish()),
            Arc::new(tap_ratio.finish()),
            Arc::new(nominal_tap_ratio.finish()),
            Arc::new(phase_shift.finish()),
            Arc::new(vector_group.finish()),
            Arc::new(rate_a.finish()),
            Arc::new(rate_b.finish()),
            Arc::new(rate_c.finish()),
            Arc::new(status.finish()),
            Arc::new(name_b.finish()),
            Arc::new(from_nominal_kv.finish()),
            Arc::new(to_nominal_kv.finish()),
        ],
    )
    .context("building transformers_2w batch")
}

fn build_transformers_3w_batch(
    transformers: &[models::ThreeWindingTransformer],
    base_mva: f64,
) -> Result<RecordBatch> {
    let schema =
        Arc::new(table_schema(TABLE_TRANSFORMERS_3W).expect("transformers_3w schema must exist"));

    let mut bus_h_id = Int32Builder::new();
    let mut bus_m_id = Int32Builder::new();
    let mut bus_l_id = Int32Builder::new();
    let mut star_bus_id = Int32Builder::new();
    let mut ckt = StringDictionaryBuilder::<Int32Type>::new();
    let mut r_hm = Float64Builder::new();
    let mut x_hm = Float64Builder::new();
    let mut r_hl = Float64Builder::new();
    let mut x_hl = Float64Builder::new();
    let mut r_ml = Float64Builder::new();
    let mut x_ml = Float64Builder::new();
    let mut tap_h = Float64Builder::new();
    let mut tap_m = Float64Builder::new();
    let mut tap_l = Float64Builder::new();
    let mut phase_shift = Float64Builder::new();
    let mut vector_group = StringDictionaryBuilder::<Int32Type>::new();
    let mut rate_a = Float64Builder::new();
    let mut rate_b = Float64Builder::new();
    let mut rate_c = Float64Builder::new();
    let mut status = BooleanBuilder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();
    let mut nominal_kv_h = Float64Builder::new();
    let mut nominal_kv_m = Float64Builder::new();
    let mut nominal_kv_l = Float64Builder::new();

    for t in transformers {
        bus_h_id.append_value(t.bus_h as i32);
        bus_m_id.append_value(t.bus_m as i32);
        bus_l_id.append_value(t.bus_l as i32);
        star_bus_id.append_value(t.star_bus_id as i32);
        ckt.append_value(t.ckt.as_ref());
        r_hm.append_value(t.r_hm);
        x_hm.append_value(t.x_hm);
        r_hl.append_value(t.r_hl);
        x_hl.append_value(t.x_hl);
        r_ml.append_value(t.r_ml);
        x_ml.append_value(t.x_ml);
        tap_h.append_value(t.tap_h);
        tap_m.append_value(t.tap_m);
        tap_l.append_value(t.tap_l);
        phase_shift.append_value(t.phase_shift_deg.to_radians());
        vector_group.append_value("unknown");
        rate_a.append_value(t.rate_a_mva / base_mva);
        rate_b.append_value(t.rate_b_mva / base_mva);
        rate_c.append_value(t.rate_c_mva / base_mva);
        status.append_value(t.stat != 0);
        name_b.append_null();
        nominal_kv_h.append_option(if t.nominal_kv_h > 0.0 {
            Some(t.nominal_kv_h)
        } else {
            None
        });
        nominal_kv_m.append_option(if t.nominal_kv_m > 0.0 {
            Some(t.nominal_kv_m)
        } else {
            None
        });
        nominal_kv_l.append_option(if t.nominal_kv_l > 0.0 {
            Some(t.nominal_kv_l)
        } else {
            None
        });
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_h_id.finish()),
            Arc::new(bus_m_id.finish()),
            Arc::new(bus_l_id.finish()),
            Arc::new(star_bus_id.finish()),
            Arc::new(ckt.finish()),
            Arc::new(r_hm.finish()),
            Arc::new(x_hm.finish()),
            Arc::new(r_hl.finish()),
            Arc::new(x_hl.finish()),
            Arc::new(r_ml.finish()),
            Arc::new(x_ml.finish()),
            Arc::new(tap_h.finish()),
            Arc::new(tap_m.finish()),
            Arc::new(tap_l.finish()),
            Arc::new(phase_shift.finish()),
            Arc::new(vector_group.finish()),
            Arc::new(rate_a.finish()),
            Arc::new(rate_b.finish()),
            Arc::new(rate_c.finish()),
            Arc::new(status.finish()),
            Arc::new(name_b.finish()),
            Arc::new(nominal_kv_h.finish()),
            Arc::new(nominal_kv_m.finish()),
            Arc::new(nominal_kv_l.finish()),
        ],
    )
    .context("building transformers_3w batch")
}

fn derive_nominal_tap_ratio(transformer: &models::TwoWindingTransformer) -> f64 {
    if transformer.nomv1 > 0.0 && transformer.nomv2 > 0.0 {
        transformer.nomv1 / transformer.nomv2
    } else {
        1.0
    }
}

fn append_vector_group(
    builder: &mut StringDictionaryBuilder<Int32Type>,
    transformer: &models::TwoWindingTransformer,
) {
    // PSS/E RAW does not directly encode IEC vector-group semantics.
    // CW/CZ describe voltage/impedance coding, not winding connection group.
    // The schema requires a non-null value, so use an explicit sentinel rather
    // than fabricating a specific IEC vector group.
    let _ = transformer;
    builder.append_value("unknown");
}

fn build_areas_batch(areas: &[models::Area]) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_AREAS).expect("areas schema must exist"));

    let mut area_id = Int32Builder::new();
    let mut name = StringDictionaryBuilder::<Int32Type>::new();
    let mut interchange_mw = Float64Builder::new();

    for area in areas {
        area_id.append_value(area.i as i32);
        name.append_value(area.arnam.as_ref());
        interchange_mw.append_value(area.pdes);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(area_id.finish()),
            Arc::new(name.finish()),
            Arc::new(interchange_mw.finish()),
        ],
    )
    .context("building areas batch")
}

fn build_zones_batch(zones: &[models::Zone]) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_ZONES).expect("zones schema must exist"));

    let mut zone_id = Int32Builder::new();
    let mut name = StringDictionaryBuilder::<Int32Type>::new();

    for zone in zones {
        zone_id.append_value(zone.i as i32);
        name.append_value(zone.zonam.as_ref());
    }

    RecordBatch::try_new(
        schema,
        vec![Arc::new(zone_id.finish()), Arc::new(name.finish())],
    )
    .context("building zones batch")
}

fn build_owners_batch(owners: &[models::Owner]) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_OWNERS).expect("owners schema must exist"));

    let mut owner_id = Int32Builder::new();
    let mut name = StringDictionaryBuilder::<Int32Type>::new();

    for owner in owners {
        owner_id.append_value(owner.i as i32);
        name.append_value(owner.ownam.as_ref());
    }

    RecordBatch::try_new(
        schema,
        vec![Arc::new(owner_id.finish()), Arc::new(name.finish())],
    )
    .context("building owners batch")
}

fn build_dynamics_models_batch(records: &[models::DyrModelData]) -> Result<RecordBatch> {
    let schema =
        Arc::new(table_schema(TABLE_DYNAMICS_MODELS).expect("dynamics_models schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut gen_id = StringDictionaryBuilder::<Int32Type>::new();
    let mut model_type = StringDictionaryBuilder::<Int32Type>::new();
    // Map<Utf8, Float64> — field names must match the schema: "entries" / "key" / "value"
    let map_field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut params = MapBuilder::new(
        Some(map_field_names),
        StringBuilder::new(),
        Float64Builder::new(),
    );

    for rec in records {
        bus_id.append_value(rec.bus_id as i32);
        gen_id.append_value(rec.id.as_ref());
        model_type.append_value(rec.model.as_ref());

        for (key, value) in &rec.params {
            params.keys().append_value(key.as_ref());
            params.values().append_value(*value);
        }
        params
            .append(true)
            .context("building dynamics params map entry")?;
    }

    let params_arr = params.finish();
    // Cast to the exact schema type to align nullability (Float64Builder emits
    // nullable values; the canonical schema requires non-null Float64 values).
    let params_target_type = schema
        .field_with_name("params")
        .expect("params field must exist in dynamics_models schema")
        .data_type()
        .clone();
    let params_cast = arrow::compute::cast(&params_arr, &params_target_type)
        .context("casting dynamics params map")?;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(gen_id.finish()),
            Arc::new(model_type.finish()),
            params_cast,
        ],
    )
    .context("building dynamics_models batch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Bus, ThreeWindingTransformer, TwoWindingTransformer};

    fn sample_3w() -> ThreeWindingTransformer {
        ThreeWindingTransformer {
            bus_h: 10,
            bus_m: 20,
            bus_l: 30,
            star_bus_id: SYNTHETIC_STAR_BUS_MIN_ID_EXCLUSIVE + 1,
            ckt: "1".into(),
            stat: 1,
            r_hm: 0.01,
            x_hm: 0.1,
            r_hl: 0.02,
            x_hl: 0.2,
            r_ml: 0.03,
            x_ml: 0.3,
            tap_h: 1.0,
            tap_m: 1.0,
            tap_l: 1.0,
            phase_shift_deg: 0.0,
            rate_a_mva: 100.0,
            rate_b_mva: 110.0,
            rate_c_mva: 120.0,
            nominal_kv_h: 230.0,
            nominal_kv_m: 115.0,
            nominal_kv_l: 13.8,
        }
    }

    fn sample_star_leg(from: u32, star: u32, suffix: &str) -> TwoWindingTransformer {
        TwoWindingTransformer {
            i: from,
            j: star,
            ckt: suffix.into(),
            stat: 1,
            windv1: 1.0,
            nomv1: 230.0,
            nomv2: 115.0,
            ..Default::default()
        }
    }

    fn sample_bus(id: u32) -> Bus {
        Bus {
            i: id,
            name: format!("B{id}").into_boxed_str(),
            ..Default::default()
        }
    }

    #[test]
    fn expanded_mode_removes_native_3w_rows() {
        let tx3 = sample_3w();
        let mut network = Network {
            buses: vec![
                sample_bus(10),
                sample_bus(20),
                sample_bus(30),
                sample_bus(tx3.star_bus_id),
            ],
            transformers: vec![
                sample_star_leg(10, tx3.star_bus_id, "S1"),
                sample_star_leg(20, tx3.star_bus_id, "S2"),
                sample_star_leg(30, tx3.star_bus_id, "S3"),
            ],
            transformers_3w: vec![tx3],
            ..Default::default()
        };

        normalize_transformer_representation(&mut network, TransformerRepresentationMode::Expanded)
            .expect("expanded mode normalization should succeed");

        assert!(network.transformers_3w.is_empty());
        assert_eq!(network.transformers.len(), 3);
        assert!(
            network
                .buses
                .iter()
                .all(|b| b.i != SYNTHETIC_STAR_BUS_MIN_ID_EXCLUSIVE + 1)
        );
    }

    #[test]
    fn native_mode_keeps_native_3w_and_real_2w_but_removes_star_legs() {
        let tx3 = sample_3w();
        let real_2w = TwoWindingTransformer {
            i: 40,
            j: 50,
            ckt: "R1".into(),
            stat: 1,
            ..Default::default()
        };
        let mut network = Network {
            buses: vec![
                sample_bus(10),
                sample_bus(20),
                sample_bus(30),
                sample_bus(40),
                sample_bus(50),
                sample_bus(tx3.star_bus_id),
            ],
            transformers: vec![
                sample_star_leg(10, tx3.star_bus_id, "S1"),
                sample_star_leg(20, tx3.star_bus_id, "S2"),
                sample_star_leg(30, tx3.star_bus_id, "S3"),
                real_2w,
            ],
            transformers_3w: vec![tx3],
            ..Default::default()
        };

        normalize_transformer_representation(&mut network, TransformerRepresentationMode::Native3W)
            .expect("native mode normalization should succeed");

        assert_eq!(network.transformers_3w.len(), 1);
        assert_eq!(network.transformers.len(), 1);
        assert_eq!(network.transformers[0].i, 40);
        assert_eq!(network.transformers[0].j, 50);
        assert!(
            network
                .buses
                .iter()
                .all(|b| b.i != SYNTHETIC_STAR_BUS_MIN_ID_EXCLUSIVE + 1)
        );
    }

    #[test]
    fn native_mode_rejects_ambiguous_overlap() {
        let tx3 = sample_3w();
        let mut network = Network {
            buses: vec![
                sample_bus(10),
                sample_bus(20),
                sample_bus(30),
                sample_bus(tx3.star_bus_id),
            ],
            transformers: vec![
                sample_star_leg(10, tx3.star_bus_id, "S1"),
                sample_star_leg(20, tx3.star_bus_id, "S2"),
                sample_star_leg(30, tx3.star_bus_id, "S3"),
                // Extra active leg touching the same star bus makes overlap ambiguous.
                sample_star_leg(99, tx3.star_bus_id, "SX"),
            ],
            transformers_3w: vec![tx3],
            ..Default::default()
        };

        let err = normalize_transformer_representation(
            &mut network,
            TransformerRepresentationMode::Native3W,
        )
        .expect_err("ambiguous overlap must be rejected");
        let message = format!("{err:#}");
        assert!(message.contains("ambiguous 3-winding overlap"));
        assert!(message.contains("star_bus_id=10000001"));
    }
}
