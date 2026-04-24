// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! `raptrix-psse-rs` — High-performance PSS/E (`.raw` + `.dyr`) →
//! Raptrix PowerFlow Interchange v0.9.0 converter.
//!
//! # Crate layout
//! * [`models`] — PSS/E data structures.
//! * [`parser`] — PSS/E `.raw` / `.dyr` parser.
//!
//! Serialisation to `.rpf` is delegated to the [`raptrix_cim_arrow`] crate.
//!
//! # Fidelity
//!
//! The converter **maps** PSS/E `.raw` / `.dyr` into the interchange schema and
//! adds **context** only where the contract requires it (metadata timestamps,
//! deterministic `bus_uuid`, `case_fingerprint`, optional CLI metadata). Deck
//! numbers and codes are **not** clamped or solver-tuned here: parsed values are
//! written as-is, except for schema-defined structure (per-unit scaling by
//! `SBASE`, bus-table **aggregates** in `docs/psse-mapping.md`, and PSS/E-documented
//! defaults when a token is missing). One explicit **interchange boundary**:
//! after aggregating reactive limits onto a bus, if `q_min` > `q_max` the exporter
//! **swaps** them so the bus row obeys min/max ordering (PSS/E `QB`/`QT` on each
//! `generators` row stay as in the deck). Extra RAW numerics without dedicated
//! columns (e.g. generator `VS` / `IREG` / impedance taps) are packed into typed
//! `params` maps where the schema provides them.
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
    TABLE_CONTINGENCIES, TABLE_DC_LINES_2W, TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS,
    TABLE_GENERATORS, TABLE_INTERFACES, TABLE_LOADS, TABLE_METADATA, TABLE_MULTI_SECTION_LINES,
    TABLE_OWNERS, TABLE_SCENARIO_CONTEXT, TABLE_SWITCHED_SHUNT_BANKS, TABLE_SWITCHED_SHUNTS,
    TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, TABLE_ZONES, table_schema,
    write_root_rpf_with_metadata,
};

use crate::models::Network;

const METADATA_KEY_TRANSFORMER_REPRESENTATION_MODE: &str = "rpf.transformer_representation_mode";
const METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE: &str = "rpf.loads.zip_fidelity_presence";
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

/// One row for the optional v0.9.0 `scenario_context` table when a writer emits that optional root.
///
/// Persisting these rows requires optional-root IPC support from the linked `raptrix-cim-arrow`
/// build. [`write_psse_to_rpf_with_options`] returns an error if [`ExportOptions::scenario_context_rows`]
/// is non-empty when that path is unavailable.
#[derive(Debug, Clone)]
pub struct ScenarioContextRow {
    pub scenario_context_id: i32,
    pub case_id: String,
    pub source_type: String,
    pub priority: String,
    pub violation_type: Option<String>,
    pub nerc_recovery_status: Option<String>,
    pub recovery_time_min: Option<f64>,
    pub cleared_by_reserves: Option<bool>,
    pub planning_feedback_flag: bool,
    pub planning_assumption_violated: Option<String>,
    pub recommended_action: Option<String>,
    pub investigation_summary: Option<String>,
    pub load_forecast_error_pct: Option<f64>,
    pub created_timestamp_utc: String,
    pub params: Vec<(String, f64)>,
}

/// Export configuration for [`write_psse_to_rpf_with_options`].
#[derive(Debug, Clone, Default)]
pub struct ExportOptions {
    /// Transformer representation mode used for this export run.
    pub transformer_representation_mode: TransformerRepresentationMode,
    /// Optional study purpose override for metadata.
    pub study_purpose: Option<String>,
    /// Optional scenario tags override for metadata.
    pub scenario_tags: Vec<String>,
    /// Optional `metadata.case_mode` / root `rpf.case_mode` override. When `None`, mode is
    /// inferred from RAW bus voltages (`flat_start_planning` vs `warm_start_planning`).
    /// Allowed: `flat_start_planning`, `warm_start_planning`, `solved_snapshot`,
    /// `hour_ahead_advisory`.
    pub case_mode_override: Option<String>,
    /// Optional `scenario_context` rows. Non-empty input is rejected when the Arrow IPC
    /// writer cannot emit that optional root (see README).
    pub scenario_context_rows: Vec<ScenarioContextRow>,
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
/// Authoritative field coverage and “not stored” rules are documented in
/// `docs/psse-mapping.md` for the released crate—rustdoc here stays minimal so
/// the public API does not read like an internal task list.
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
    if !options.scenario_context_rows.is_empty() {
        anyhow::bail!(
            "scenario_context_rows is non-empty, but optional `scenario_context` root emission is unsupported in this build's Arrow IPC path. Omit scenario_context_rows for standard PSS/E exports."
        );
    }

    let mut network = parser::parse_raw(std::path::Path::new(raw_path))
        .with_context(|| format!("failed to parse RAW file: {raw_path}"))?;

    if let Some(dyr) = dyr_path {
        network.dyr_models = parser::parse_dyr_records(std::path::Path::new(dyr))
            .with_context(|| format!("failed to parse DYR file: {dyr}"))?;
        network.dyr_generators = parser::extract_dyr_generators(&network.dyr_models);
    }

    derive_switched_shunt_banks(&mut network);
    let ibr_subtype_by_gen = compute_ibr_subtype_by_generator(&network);

    normalize_transformer_representation(&mut network, options.transformer_representation_mode)?;

    // Build a (bus_id, machine_id) → DyrGeneratorData lookup for the generators table.
    let dyr_lookup: HashMap<(u32, String), &models::DyrGeneratorData> = network
        .dyr_generators
        .iter()
        .map(|r| ((r.bus_id, r.id.to_string()), r))
        .collect();

    let mut table_batches: HashMap<&'static str, RecordBatch> = HashMap::new();
    let bus_aggregates = build_bus_aggregates(&network);
    let bus_nominal_kv = build_bus_nominal_kv_map(&network);
    let base_mva = if network.case_id.sbase.abs() > 1.0e-9 {
        network.case_id.sbase
    } else {
        100.0
    };
    let case_fingerprint = compute_case_fingerprint(&network);
    let case_mode = resolve_case_mode(&network, options)?;

    table_batches.insert(
        TABLE_METADATA,
        build_metadata_batch(
            &network,
            &case_fingerprint,
            case_mode.as_str(),
            &ibr_subtype_by_gen,
            options,
        )?,
    );
    table_batches.insert(
        TABLE_BUSES,
        build_buses_batch(&network.buses, &bus_aggregates)?,
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
        build_generators_batch(&network.generators, &dyr_lookup, &ibr_subtype_by_gen)?,
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
        TABLE_SWITCHED_SHUNT_BANKS,
        build_switched_shunt_banks_batch(&network.switched_shunt_banks)?,
    );
    table_batches.insert(
        TABLE_MULTI_SECTION_LINES,
        build_multi_section_lines_batch(&network.multi_section_lines)?,
    );
    table_batches.insert(
        TABLE_DC_LINES_2W,
        build_dc_lines_2w_batch(&network.dc_lines_2w)?,
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

    validate_export_invariants(&table_batches, options.transformer_representation_mode)?;

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
    additional_root_metadata.insert(METADATA_KEY_CASE_MODE.to_string(), case_mode.clone());
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
    additional_root_metadata.insert(
        METADATA_KEY_LOADS_ZIP_FIDELITY_PRESENCE.to_string(),
        classify_loads_zip_fidelity_presence(&network.loads).to_string(),
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
    transformer_mode: TransformerRepresentationMode,
) -> Result<()> {
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

fn resolve_case_mode(network: &Network, options: &ExportOptions) -> Result<String> {
    const ALLOWED: &[&str] = &[
        "flat_start_planning",
        "warm_start_planning",
        "solved_snapshot",
        "hour_ahead_advisory",
    ];
    if let Some(raw) = &options.case_mode_override {
        let token = raw.trim();
        if ALLOWED.contains(&token) {
            return Ok(token.to_string());
        }
        anyhow::bail!(
            "invalid case_mode override '{raw}'; expected one of: {}",
            ALLOWED.join(", ")
        );
    }
    Ok(detect_case_mode(network).to_string())
}

fn derive_switched_shunt_banks(network: &mut Network) {
    network.switched_shunt_banks.clear();
    for (shunt_row_idx, shunt) in network.switched_shunts.iter().enumerate() {
        let shunt_id = (shunt_row_idx + 1) as i32;
        for (bank_idx, (n_steps, b_mvar)) in shunt.bank_pairs.iter().enumerate() {
            let bank_id = (bank_idx + 1) as i32;
            for step in 1..=(*n_steps as i32) {
                network
                    .switched_shunt_banks
                    .push(models::SwitchedShuntBank {
                        shunt_id,
                        bank_id,
                        b_mvar: *b_mvar,
                        status: shunt.stat != 0,
                        step,
                    });
            }
        }
    }
}

/// Classify an IBR device type and control mode based on DYR model name.
///
/// This classifier matches PSS/E dynamic model families to Raptrix device categories:
/// - **solar_pv**: REGCA/REECA/REPCA, solar inverter controls
/// - **wind_type3**: WT3*, DFIG wind turbine families
/// - **wind_type4**: WT4*, full-converter wind, VSC-based
/// - **bess**: REGCB/REECB/REPCB, battery/storage inverters
/// - **generic_ibr**: REGC/REEC/REPC (device-agnostic), other renewable controls
///
/// Returns (device_type, control_mode) where control_mode is typically "grid_following"
/// unless explicitly marked "grid_forming" (GFM suffix).
///
/// DYR-first priority prevents false positives from generic REGC overlapping with typed families.
fn classify_ibr_model(model: &str) -> Option<(&'static str, &'static str)> {
    let m = model.to_ascii_lowercase();

    // --- BESS / Storage families (check FIRST to avoid generic REGC collision)
    // REGCB, REECB, REPCB are battery/storage-specific control families.
    if [
        "regcb", "reecb", "repcb", "beca", "becb", "bess", "bat", "esst", "esst1", "esst2",
        "esst3", "esst4", "esdc", "batt",
    ]
    .iter()
    .any(|p| m.starts_with(p))
    {
        return Some(("bess", "grid_following"));
    }

    // --- Solar PV families
    // REGCA, REECA, REPCA are solar/PV plant control families.
    if [
        "regca", "reeca", "repca", "pv", "pvgen", "pvmod", "solar", "pvinv",
    ]
    .iter()
    .any(|p| m.starts_with(p))
    {
        return Some(("solar_pv", "grid_following"));
    }

    // --- Wind Type-4 (Full-Converter / Synchronous-Reference-Frame inverter)
    // WT4, WTGA, WTGQ, WTGT families and VSC-based high-speed controls.
    if [
        "wt4", "wtg4", "wt4g", "wtga", "wtgb", "wtgq", "wtgt", "wtga4", "wtgq4",
    ]
    .iter()
    .any(|p| m.starts_with(p))
    {
        return Some(("wind_type4", "grid_following"));
    }

    // --- Wind Type-3 (DFIG / Doubly-Fed Induction Generator)
    // WT3, WTARA, WTARV families and generator-based wind models.
    if [
        "wt3", "wtg3", "wt3g", "wtara", "wtarv", "wtga3", "wtgq3", "dfig",
    ]
    .iter()
    .any(|p| m.starts_with(p))
    {
        return Some(("wind_type3", "grid_following"));
    }

    // --- Generic Renewable / Device-Agnostic Controls
    // REGC, REEC, REPC (without type suffix) apply to unspecified renewable source.
    // Lower priority to avoid shadowing typed families.
    if ["regc", "reec", "repc", "rep", "reg", "cim"]
        .iter()
        .any(|p| m.starts_with(p))
    {
        return Some(("generic_ibr", "grid_following"));
    }

    // --- Grid-Forming (GFM) Explicit Indicators
    // Any model with GFM suffix is flagged as grid-forming regardless of family.
    if m.contains("gfm") || m.contains("vsg") || m.contains("vsm") {
        return Some(("generic_ibr", "grid_forming"));
    }

    // No match
    None
}

fn classify_ibr_from_wmod(wmod: u8) -> Option<(&'static str, &'static str)> {
    match wmod {
        0 => None,
        1 => Some(("wind_type4", "grid_following")),
        2 | 3 => Some(("generic_ibr", "wmod")),
        _ => Some(("generic_ibr", "wmod")),
    }
}

/// DYR / WMOD-derived IBR classification: `(bus_id, gen_id)` → canonical `generators.ibr_subtype`.
fn compute_ibr_subtype_by_generator(network: &Network) -> HashMap<(u32, String), String> {
    let mut out: HashMap<(u32, String), String> = HashMap::new();

    let mut dyr_by_gen: HashMap<(u32, String), Vec<&models::DyrModelData>> = HashMap::new();
    for rec in &network.dyr_models {
        dyr_by_gen
            .entry((rec.bus_id, rec.id.to_string()))
            .or_default()
            .push(rec);
    }

    for generator in &network.generators {
        if generator.stat == 0 {
            continue;
        }

        let key = (generator.i, generator.id.to_string());
        let mut selected_type: Option<&'static str> = None;
        let mut selected_mode: Option<&'static str> = None;

        if let Some(records) = dyr_by_gen.get(&key) {
            for rec in records {
                if let Some((device_type, control_mode)) = classify_ibr_model(rec.model.as_ref()) {
                    selected_type = Some(device_type);
                    selected_mode = Some(control_mode);
                    break;
                }
            }
        }

        if selected_type.is_none() {
            if let Some((device_type, control_mode)) = classify_ibr_from_wmod(generator.wmod) {
                selected_type = Some(device_type);
                selected_mode = Some(control_mode);
            }
        }

        if let (Some(device_type), Some(_control_mode)) = (selected_type, selected_mode) {
            out.insert(key, canonical_ibr_subtype(device_type).to_string());
        }
    }
    out
}

fn infer_study_purpose(title: &str) -> Option<String> {
    let t = title.to_ascii_lowercase();
    if t.contains("planning") || t.contains("2030") || t.contains("future") {
        return Some("planning".to_string());
    }
    if t.contains("onpeak") || t.contains("offpeak") || t.contains("operations") {
        return Some("operations".to_string());
    }
    None
}

fn infer_scenario_tags(title: &str) -> Vec<String> {
    let t = title.to_ascii_lowercase();
    let mut tags = Vec::new();
    for (needle, tag) in [
        ("onpeak", "onpeak"),
        ("offpeak", "offpeak"),
        ("summerpeak", "summer_peak"),
        ("winter", "winter"),
        ("dynamic", "dynamic"),
        ("static", "static"),
        ("gfm", "gfm"),
        ("ibr", "ibr"),
    ] {
        if t.contains(needle) {
            tags.push(tag.to_string());
        }
    }
    tags
}

// ---------------------------------------------------------------------------
// Table builders
// ---------------------------------------------------------------------------

fn build_metadata_batch(
    network: &Network,
    case_fingerprint_value: &str,
    case_mode: &str,
    ibr_subtype_by_gen: &HashMap<(u32, String), String>,
    _options: &ExportOptions,
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

    // custom_metadata is nullable — emit a single null value.
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

    // v0.8.9 modern-grid metadata (IBR signals unified on `generators` in v0.9.0).
    let has_ibr_value = !ibr_subtype_by_gen.is_empty();
    let has_smart_valve_value = network.facts_devices.iter().any(|d| {
        let t = d.device_type.as_ref().to_ascii_lowercase();
        t.contains("smart") || t.contains("valve")
    });
    let has_multi_terminal_dc_value = network.has_multi_terminal_dc;
    let modern_grid_profile_value = has_ibr_value
        || has_smart_valve_value
        || has_multi_terminal_dc_value
        || !network.dc_lines_2w.is_empty();

    let total_pmax_mw: f64 = network
        .generators
        .iter()
        .filter(|g| g.stat != 0)
        .map(|g| g.pt.max(0.0))
        .sum();
    let ibr_pmax_mw: f64 = network
        .generators
        .iter()
        .filter(|g| g.stat != 0)
        .filter(|g| ibr_subtype_by_gen.contains_key(&(g.i, g.id.to_string())))
        .map(|g| g.pt.max(0.0))
        .sum();
    let mut ibr_penetration_pct_arr = Float64Builder::new();
    if total_pmax_mw > 1.0e-9 {
        ibr_penetration_pct_arr.append_value((ibr_pmax_mw / total_pmax_mw) * 100.0);
    } else {
        ibr_penetration_pct_arr.append_null();
    }

    let study_purpose_value = _options
        .study_purpose
        .clone()
        .or_else(|| infer_study_purpose(network.case_id.title.as_ref()));
    let mut study_purpose_arr = StringBuilder::new();
    study_purpose_arr.append_option(study_purpose_value.as_deref());

    let scenario_tags_value = if _options.scenario_tags.is_empty() {
        infer_scenario_tags(network.case_id.title.as_ref())
    } else {
        _options.scenario_tags.clone()
    };
    let scenario_item_field = Arc::new(arrow::datatypes::Field::new(
        "item",
        arrow::datatypes::DataType::Utf8,
        false,
    ));
    let mut scenario_tags_arr =
        ListBuilder::new(StringBuilder::new()).with_field(scenario_item_field);
    if scenario_tags_value.is_empty() {
        scenario_tags_arr.append(false);
    } else {
        for tag in &scenario_tags_value {
            scenario_tags_arr.values().append_value(tag);
        }
        scenario_tags_arr.append(true);
    }

    // v0.9.0 extended nullable metadata columns — null for typical PSS/E planning exports.
    let mut hour_ahead_uncertainty_band = Float64Builder::new();
    hour_ahead_uncertainty_band.append_null();
    let mut commitment_source = StringBuilder::new();
    commitment_source.append_null();
    let mut solver_q_limit_infeasible_count = Int32Builder::new();
    solver_q_limit_infeasible_count.append_null();
    let mut pv_to_pq_switch_count = Int32Builder::new();
    pv_to_pq_switch_count.append_null();
    let mut real_time_discovery = BooleanBuilder::new();
    real_time_discovery.append_null();

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
            // v0.8.9 columns
            Arc::new(BooleanArray::from(vec![modern_grid_profile_value])),
            Arc::new(ibr_penetration_pct_arr.finish()),
            Arc::new(BooleanArray::from(vec![has_ibr_value])),
            Arc::new(BooleanArray::from(vec![has_smart_valve_value])),
            Arc::new(BooleanArray::from(vec![has_multi_terminal_dc_value])),
            Arc::new(study_purpose_arr.finish()),
            Arc::new(scenario_tags_arr.finish()),
            // v0.9.0 columns
            Arc::new(hour_ahead_uncertainty_band.finish()),
            Arc::new(commitment_source.finish()),
            Arc::new(solver_q_limit_infeasible_count.finish()),
            Arc::new(pv_to_pq_switch_count.finish()),
            Arc::new(real_time_discovery.finish()),
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

            // `buses.v_mag_set`: last in-service row with finite non-zero VS (PSS/E uses 0 as unset).
            if generator.vs.is_finite() && generator.vs != 0.0 {
                agg.v_mag_set_override = Some(generator.vs);
            }
        }
    }

    agg_by_bus
}

fn build_buses_batch(
    buses: &[models::Bus],
    agg_by_bus: &HashMap<u32, BusAggregate>,
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

    for bus in buses {
        let agg = agg_by_bus.get(&bus.i).cloned().unwrap_or_default();
        // PSS/E lists QB (min Q) and QT (max Q) in machine MVAr; decks sometimes
        // arrive with QB>QT (sign quirks, rounding, or tooling). The interchange
        // and downstream solvers expect `q_min` ≤ `q_max` on the **bus** row — so
        // we swap here only to restore ordering, not to discard RAW (per-machine
        // QB/QT remain on `generators` and in `generators.params`).
        let mut q_min_val = agg.q_min;
        let mut q_max_val = agg.q_max;
        if q_min_val > q_max_val {
            std::mem::swap(&mut q_min_val, &mut q_max_val);
        }

        let vm_export = agg.v_mag_set_override.unwrap_or(bus.vm);

        bus_id.append_value(bus.i as i32);
        name.append_value(bus.name.as_ref());
        bus_type.append_value(bus.ide as i8);
        p_sched.append_value(agg.p_sched);
        q_sched.append_value(agg.q_sched);
        // `v_mag_set`: interchange aggregate — last non-zero finite in-service `VS`
        // at the bus when present, else `bus.vm` from the RAW bus record (no clamp).
        v_mag_set.append_value(vm_export);
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
    let mut owner_id = Int32Builder::new();
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
    let mut parent_line_id = Int32Builder::new();
    let mut section_index = Int32Builder::new();
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
        if branch.o1 > 0 {
            owner_id.append_value(branch.o1 as i32);
        } else {
            owner_id.append_null();
        }
        name_b.append_null(); // branches have no name in RAW
        from_nominal_kv.append_option(bus_nominal_kv.get(&branch.i).copied());
        to_nominal_kv.append_option(bus_nominal_kv.get(&branch.j).copied());
        parent_line_id.append_null();
        section_index.append_null();

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
            Arc::new(owner_id.finish()),
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
            Arc::new(parent_line_id.finish()),
            Arc::new(section_index.finish()),
        ],
    )
    .context("building branches batch")
}

/// Push PSS/E generator section numerics into `generators.params` (keys are
/// stable lowercase PSS/E tokens). Units match the RAW file: `qg` is MVAr,
/// impedances are on `MBASE`, etc. Keeps first-class columns lean while giving
/// the solver the full machine record.
fn append_psse_generator_raw_params(
    params: &mut MapBuilder<StringBuilder, Float64Builder>,
    machine: &models::Generator,
) -> Result<()> {
    let mut push = |k: &str, v: f64| -> Result<()> {
        params.keys().append_value(k);
        params.values().append_value(v);
        Ok(())
    };
    push("vs", machine.vs)?;
    if machine.ireg > 0 {
        push("ireg", machine.ireg as f64)?;
    }
    push("zr", machine.zr)?;
    push("zx", machine.zx)?;
    push("rt", machine.rt)?;
    push("xt", machine.xt)?;
    push("gtap", machine.gtap)?;
    push("rmpct", machine.rmpct)?;
    push("qg", machine.qg)?;
    push("wmod", machine.wmod as f64)?;
    push("wpf", machine.wpf)?;
    Ok(())
}

fn build_generators_batch(
    generators: &[models::Generator],
    dyr_lookup: &HashMap<(u32, String), &models::DyrGeneratorData>,
    ibr_subtype_by_gen: &HashMap<(u32, String), String>,
) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_GENERATORS).expect("generators schema must exist"));

    let map_field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };

    let mut generator_id = Int32Builder::new();
    let mut bus_id = Int32Builder::new();
    let mut name_b = StringBuilder::new();
    let mut unit_type = StringBuilder::new();
    let mut hierarchy_level = StringBuilder::new();
    let mut parent_generator_id = Int32Builder::new();
    let mut aggregation_count = Int32Builder::new();
    let mut status = BooleanBuilder::new();
    let mut is_ibr = BooleanBuilder::new();
    let mut ibr_subtype = StringBuilder::new();
    let mut p_sched_mw = Float64Builder::new();
    let mut p_min_mw = Float64Builder::new();
    let mut p_max_mw = Float64Builder::new();
    let mut q_min_mvar = Float64Builder::new();
    let mut q_max_mvar = Float64Builder::new();
    let mut mbase_mva = Float64Builder::new();
    let mut uol_mw = Float64Builder::new();
    let mut lol_mw = Float64Builder::new();
    let mut ramp_rate_up_mw_min = Float64Builder::new();
    let mut ramp_rate_down_mw_min = Float64Builder::new();
    let mut owner_id = Int32Builder::new();
    let mut market_resource_id = StringBuilder::new();
    let mut params = MapBuilder::new(
        Some(map_field_names),
        StringBuilder::new(),
        Float64Builder::new(),
    );

    for (idx, generator) in generators.iter().enumerate() {
        let key = (generator.i, generator.id.to_string());
        let subtype = ibr_subtype_by_gen.get(&key).cloned();

        generator_id.append_value((idx + 1) as i32);
        bus_id.append_value(generator.i as i32);
        name_b.append_null();
        unit_type.append_value("unit");
        hierarchy_level.append_value("unit");
        parent_generator_id.append_null();
        aggregation_count.append_null();
        status.append_value(generator.stat != 0);
        is_ibr.append_value(subtype.is_some());
        if let Some(value) = subtype {
            ibr_subtype.append_value(value.as_str());
        } else {
            ibr_subtype.append_null();
        }
        p_sched_mw.append_value(generator.pg);
        p_min_mw.append_value(generator.pb);
        p_max_mw.append_value(generator.pt);
        q_min_mvar.append_value(generator.qb);
        q_max_mvar.append_value(generator.qt);
        mbase_mva.append_value(generator.mbase);
        uol_mw.append_null();
        lol_mw.append_null();
        ramp_rate_up_mw_min.append_null();
        ramp_rate_down_mw_min.append_null();
        if generator.o1 > 0 {
            owner_id.append_value(generator.o1 as i32);
        } else {
            owner_id.append_null();
        }
        market_resource_id.append_null();

        append_psse_generator_raw_params(&mut params, generator)
            .context("PSS/E generator params")?;
        if let Some(dyn_data) = dyr_lookup.get(&(generator.i, generator.id.to_string())) {
            if dyn_data.h.is_finite() {
                params.keys().append_value("H");
                params.values().append_value(dyn_data.h);
            }
            if dyn_data.xd_prime.is_finite() {
                params.keys().append_value("xd_prime");
                params.values().append_value(dyn_data.xd_prime);
            }
            if dyn_data.d.is_finite() {
                params.keys().append_value("D");
                params.values().append_value(dyn_data.d);
            }
        }
        params
            .append(true)
            .context("building generators.params map entry")?;
    }

    let params_arr = params.finish();
    let params_target_type = schema
        .field_with_name("params")
        .expect("params field must exist in generators schema")
        .data_type()
        .clone();
    let params_cast = arrow::compute::cast(&params_arr, &params_target_type)
        .context("casting generators params")?;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(generator_id.finish()),
            Arc::new(bus_id.finish()),
            Arc::new(name_b.finish()),
            Arc::new(unit_type.finish()),
            Arc::new(hierarchy_level.finish()),
            Arc::new(parent_generator_id.finish()),
            Arc::new(aggregation_count.finish()),
            Arc::new(status.finish()),
            Arc::new(is_ibr.finish()),
            Arc::new(ibr_subtype.finish()),
            Arc::new(p_sched_mw.finish()),
            Arc::new(p_min_mw.finish()),
            Arc::new(p_max_mw.finish()),
            Arc::new(q_min_mvar.finish()),
            Arc::new(q_max_mvar.finish()),
            Arc::new(mbase_mva.finish()),
            Arc::new(uol_mw.finish()),
            Arc::new(lol_mw.finish()),
            Arc::new(ramp_rate_up_mw_min.finish()),
            Arc::new(ramp_rate_down_mw_min.finish()),
            Arc::new(owner_id.finish()),
            Arc::new(market_resource_id.finish()),
            params_cast,
        ],
    )
    .context("building generators batch")
}

fn canonical_ibr_subtype(device_type: &str) -> &'static str {
    match device_type {
        "bess" => "battery",
        "solar_pv" => "solar",
        "wind_type3" | "wind_type4" => "wind",
        _ => "generic_ibr",
    }
}

fn build_loads_batch(loads: &[models::Load], base_mva: f64) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_LOADS).expect("loads schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut id = StringDictionaryBuilder::<Int32Type>::new();
    let mut status = BooleanBuilder::new();
    let mut p_pu = Float64Builder::new();
    let mut q_pu = Float64Builder::new();
    let mut p_i_pu = Float64Builder::new();
    let mut q_i_pu = Float64Builder::new();
    let mut p_y_pu = Float64Builder::new();
    let mut q_y_pu = Float64Builder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();

    for load in loads {
        bus_id.append_value(load.i as i32);
        id.append_value(load.id.as_ref());
        status.append_value(load.status != 0);
        p_pu.append_value(load.pl / base_mva);
        q_pu.append_value(load.ql / base_mva);
        if load.ip_available {
            p_i_pu.append_value(load.ip / base_mva);
        } else {
            p_i_pu.append_null();
        }
        if load.iq_available {
            q_i_pu.append_value(load.iq / base_mva);
        } else {
            q_i_pu.append_null();
        }
        if load.yp_available {
            p_y_pu.append_value(load.yp / base_mva);
        } else {
            p_y_pu.append_null();
        }
        if load.yq_available {
            q_y_pu.append_value(load.yq / base_mva);
        } else {
            q_y_pu.append_null();
        }
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
            Arc::new(p_i_pu.finish()),
            Arc::new(q_i_pu.finish()),
            Arc::new(p_y_pu.finish()),
            Arc::new(q_y_pu.finish()),
            Arc::new(name_b.finish()),
        ],
    )
    .context("building loads batch")
}

fn classify_loads_zip_fidelity_presence(loads: &[models::Load]) -> &'static str {
    if loads.is_empty() {
        return "not_available";
    }
    let complete_rows = loads
        .iter()
        .filter(|l| l.ip_available && l.iq_available && l.yp_available && l.yq_available)
        .count();
    if complete_rows == 0 {
        "not_available"
    } else if complete_rows == loads.len() {
        "complete"
    } else {
        "partial"
    }
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
            if step_mvar > 0.0 {
                step_values_pu.push(step_mvar / base_mva);
            }
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

fn build_switched_shunt_banks_batch(rows: &[models::SwitchedShuntBank]) -> Result<RecordBatch> {
    let schema = Arc::new(
        table_schema(TABLE_SWITCHED_SHUNT_BANKS).expect("switched_shunt_banks schema must exist"),
    );

    let mut shunt_id = Int32Builder::new();
    let mut bank_id = Int32Builder::new();
    let mut b_mvar = Float64Builder::new();
    let mut status = BooleanBuilder::new();
    let mut step = Int32Builder::new();

    for row in rows {
        shunt_id.append_value(row.shunt_id);
        bank_id.append_value(row.bank_id);
        b_mvar.append_value(row.b_mvar);
        status.append_value(row.status);
        step.append_value(row.step);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(shunt_id.finish()),
            Arc::new(bank_id.finish()),
            Arc::new(b_mvar.finish()),
            Arc::new(status.finish()),
            Arc::new(step.finish()),
        ],
    )
    .context("building switched_shunt_banks batch")
}

fn build_multi_section_lines_batch(rows: &[models::MultiSectionLine]) -> Result<RecordBatch> {
    let schema = Arc::new(
        table_schema(TABLE_MULTI_SECTION_LINES).expect("multi_section_lines schema must exist"),
    );

    let mut line_id = Int32Builder::new();
    let mut from_bus_id = Int32Builder::new();
    let mut to_bus_id = Int32Builder::new();
    let mut ckt = StringBuilder::new();
    let section_item_field = Arc::new(arrow::datatypes::Field::new(
        "item",
        arrow::datatypes::DataType::Int32,
        false,
    ));
    let mut section_branch_ids =
        ListBuilder::new(Int32Builder::new()).with_field(section_item_field);
    let mut total_r_pu = Float64Builder::new();
    let mut total_x_pu = Float64Builder::new();
    let mut total_b_pu = Float64Builder::new();
    let mut rate_a_mva = Float64Builder::new();
    let mut rate_b_mva = Float64Builder::new();
    let mut status = BooleanBuilder::new();
    let mut name_b = StringBuilder::new();

    for row in rows {
        line_id.append_value(row.line_id);
        from_bus_id.append_value(row.from_bus_id as i32);
        to_bus_id.append_value(row.to_bus_id as i32);
        ckt.append_value(row.ckt.as_ref());
        for section_id in &row.section_branch_ids {
            section_branch_ids.values().append_value(*section_id);
        }
        section_branch_ids.append(true);
        total_r_pu.append_value(row.total_r_pu);
        total_x_pu.append_value(row.total_x_pu);
        total_b_pu.append_value(row.total_b_pu);
        rate_a_mva.append_value(row.rate_a_mva);
        rate_b_mva.append_option(row.rate_b_mva);
        status.append_value(row.status);
        name_b.append_option(row.name.as_deref());
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(line_id.finish()),
            Arc::new(from_bus_id.finish()),
            Arc::new(to_bus_id.finish()),
            Arc::new(ckt.finish()),
            Arc::new(section_branch_ids.finish()),
            Arc::new(total_r_pu.finish()),
            Arc::new(total_x_pu.finish()),
            Arc::new(total_b_pu.finish()),
            Arc::new(rate_a_mva.finish()),
            Arc::new(rate_b_mva.finish()),
            Arc::new(status.finish()),
            Arc::new(name_b.finish()),
        ],
    )
    .context("building multi_section_lines batch")
}

fn build_dc_lines_2w_batch(rows: &[models::DcLine2W]) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_DC_LINES_2W).expect("dc_lines_2w schema must exist"));

    let mut dc_line_id = Int32Builder::new();
    let mut from_bus_id = Int32Builder::new();
    let mut to_bus_id = Int32Builder::new();
    let mut ckt = StringBuilder::new();
    let mut r_ohm = Float64Builder::new();
    let mut l_henry = Float64Builder::new();
    let mut control_mode = StringBuilder::new();
    let mut p_setpoint_mw = Float64Builder::new();
    let mut i_setpoint_ka = Float64Builder::new();
    let mut v_setpoint_kv = Float64Builder::new();
    let mut q_from_mvar = Float64Builder::new();
    let mut q_to_mvar = Float64Builder::new();
    let mut status = BooleanBuilder::new();
    let mut name_b = StringBuilder::new();
    let mut converter_type = StringBuilder::new();

    for row in rows {
        dc_line_id.append_value(row.dc_line_id);
        from_bus_id.append_value(row.from_bus_id as i32);
        to_bus_id.append_value(row.to_bus_id as i32);
        ckt.append_value(row.ckt.as_ref());
        r_ohm.append_value(row.r_ohm);
        l_henry.append_option(row.l_henry);
        control_mode.append_value(row.control_mode.as_ref());
        p_setpoint_mw.append_option(row.p_setpoint_mw);
        i_setpoint_ka.append_option(row.i_setpoint_ka);
        v_setpoint_kv.append_option(row.v_setpoint_kv);
        q_from_mvar.append_option(row.q_from_mvar);
        q_to_mvar.append_option(row.q_to_mvar);
        status.append_value(row.status);
        name_b.append_option(row.name.as_deref());
        converter_type.append_value(row.converter_type.as_ref());
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(dc_line_id.finish()),
            Arc::new(from_bus_id.finish()),
            Arc::new(to_bus_id.finish()),
            Arc::new(ckt.finish()),
            Arc::new(r_ohm.finish()),
            Arc::new(l_henry.finish()),
            Arc::new(control_mode.finish()),
            Arc::new(p_setpoint_mw.finish()),
            Arc::new(i_setpoint_ka.finish()),
            Arc::new(v_setpoint_kv.finish()),
            Arc::new(q_from_mvar.finish()),
            Arc::new(q_to_mvar.finish()),
            Arc::new(status.finish()),
            Arc::new(name_b.finish()),
            Arc::new(converter_type.finish()),
        ],
    )
    .context("building dc_lines_2w batch")
}

/// Build optional `scenario_context` batch (v0.9.0). Used only when the write path can emit
/// that optional root; otherwise the export errors before this runs.
#[allow(dead_code)]
fn build_scenario_context_batch(rows: &[ScenarioContextRow]) -> Result<RecordBatch> {
    let schema =
        Arc::new(table_schema(TABLE_SCENARIO_CONTEXT).expect("scenario_context schema must exist"));

    let map_field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };

    let mut scenario_context_id = Int32Builder::new();
    let mut case_id = StringBuilder::new();
    let mut source_type = StringBuilder::new();
    let mut priority = StringBuilder::new();
    let mut violation_type = StringBuilder::new();
    let mut nerc_recovery_status = StringBuilder::new();
    let mut recovery_time_min = Float64Builder::new();
    let mut cleared_by_reserves = BooleanBuilder::new();
    let mut planning_feedback_flag = BooleanBuilder::new();
    let mut planning_assumption_violated = StringBuilder::new();
    let mut recommended_action = StringBuilder::new();
    let mut investigation_summary = StringBuilder::new();
    let mut load_forecast_error_pct = Float64Builder::new();
    let mut created_timestamp_utc = StringBuilder::new();
    let mut params = MapBuilder::new(
        Some(map_field_names),
        StringBuilder::new(),
        Float64Builder::new(),
    );

    for row in rows {
        scenario_context_id.append_value(row.scenario_context_id);
        case_id.append_value(row.case_id.as_str());
        source_type.append_value(row.source_type.as_str());
        priority.append_value(row.priority.as_str());
        violation_type.append_option(row.violation_type.as_deref());
        nerc_recovery_status.append_option(row.nerc_recovery_status.as_deref());
        recovery_time_min.append_option(row.recovery_time_min);
        cleared_by_reserves.append_option(row.cleared_by_reserves);
        planning_feedback_flag.append_value(row.planning_feedback_flag);
        planning_assumption_violated.append_option(row.planning_assumption_violated.as_deref());
        recommended_action.append_option(row.recommended_action.as_deref());
        investigation_summary.append_option(row.investigation_summary.as_deref());
        load_forecast_error_pct.append_option(row.load_forecast_error_pct);
        created_timestamp_utc.append_value(row.created_timestamp_utc.as_str());
        if row.params.is_empty() {
            params
                .append(false)
                .context("building scenario_context.params null entry")?;
        } else {
            for (k, v) in &row.params {
                params.keys().append_value(k.as_str());
                params.values().append_value(*v);
            }
            params
                .append(true)
                .context("building scenario_context.params entry")?;
        }
    }

    let params_arr = params.finish();
    let params_target_type = schema
        .field_with_name("params")
        .expect("params field must exist in scenario_context schema")
        .data_type()
        .clone();
    let params_cast = arrow::compute::cast(&params_arr, &params_target_type)
        .context("casting scenario_context params")?;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(scenario_context_id.finish()),
            Arc::new(case_id.finish()),
            Arc::new(source_type.finish()),
            Arc::new(priority.finish()),
            Arc::new(violation_type.finish()),
            Arc::new(nerc_recovery_status.finish()),
            Arc::new(recovery_time_min.finish()),
            Arc::new(cleared_by_reserves.finish()),
            Arc::new(planning_feedback_flag.finish()),
            Arc::new(planning_assumption_violated.finish()),
            Arc::new(recommended_action.finish()),
            Arc::new(investigation_summary.finish()),
            Arc::new(load_forecast_error_pct.finish()),
            Arc::new(created_timestamp_utc.finish()),
            params_cast,
        ],
    )
    .context("building scenario_context batch")
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
        winding1_r.append_value(0.0); // Placeholder: see psse-mapping for 3W export notes
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
    // The schema requires a non-null value, so use an explicit placeholder rather
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
    let mut name = StringBuilder::new();
    let mut short_name = StringBuilder::new();
    let mut owner_type = StringBuilder::new();
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

    for owner in owners {
        owner_id.append_value(owner.i as i32);
        name.append_value(owner.ownam.as_ref());
        short_name.append_null();
        owner_type.append_null();
        params
            .append(false)
            .context("building owners.params null entry")?;
    }

    let params_arr = params.finish();
    let params_target_type = schema
        .field_with_name("params")
        .expect("params field must exist in owners schema")
        .data_type()
        .clone();
    let params_cast =
        arrow::compute::cast(&params_arr, &params_target_type).context("casting owners params")?;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(owner_id.finish()),
            Arc::new(name.finish()),
            Arc::new(short_name.finish()),
            Arc::new(owner_type.finish()),
            params_cast,
        ],
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
