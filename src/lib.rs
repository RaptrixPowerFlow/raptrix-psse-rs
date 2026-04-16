// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! `raptrix-psse-rs` â€” High-performance PSS/E (`.raw` + `.dyr`) â†’
//! Raptrix PowerFlow Interchange v0.8.6 converter.
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
pub use raptrix_cim_arrow::{read_rpf_tables, summarize_rpf, RpfSummary, TableSummary};

use std::{collections::{HashMap, HashSet}, hash::{Hash, Hasher}, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::{
        new_null_array, BooleanBuilder, Float64Builder, Int32Builder,
        Int8Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
        StringDictionaryBuilder,
    },
    datatypes::{Int32Type, UInt32Type},
    record_batch::RecordBatch,
};
use raptrix_cim_arrow::{
    table_schema, write_root_rpf_with_metadata,
    METADATA_KEY_CASE_FINGERPRINT, METADATA_KEY_CASE_MODE, METADATA_KEY_SOLVED_STATE_PRESENCE,
    METADATA_KEY_VALIDATION_MODE, RootWriteOptions,
    TABLE_AREAS, TABLE_BRANCHES, TABLE_BUSES, TABLE_CONTINGENCIES,
    TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS, TABLE_GENERATORS, TABLE_INTERFACES,
    TABLE_LOADS, TABLE_METADATA, TABLE_OWNERS, TABLE_SWITCHED_SHUNTS,
    TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, TABLE_ZONES,
};

use crate::models::Network;

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
/// - Implement dynamics-model mapping from a paired `.dyr` file.
/// Parse `raw_path` (and optional `dyr_path`) and write an `.rpf` file.
///
/// Pass `dyr_path = None` when no dynamic data file is available.
pub fn write_psse_to_rpf(raw_path: &str, dyr_path: Option<&str>, output: &str) -> Result<()> {
    let mut network = parser::parse_raw(std::path::Path::new(raw_path))
        .with_context(|| format!("failed to parse RAW file: {raw_path}"))?;

    if let Some(dyr) = dyr_path {
        network.dyr_generators = parser::parse_dyr(std::path::Path::new(dyr))
            .with_context(|| format!("failed to parse DYR file: {dyr}"))?;
    }

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
    // v0.8.5: detect warm vs flat start from RAW bus voltage state.
    let case_mode = detect_case_mode(&network);

    table_batches.insert(TABLE_METADATA, build_metadata_batch(&network, &case_fingerprint, case_mode)?);
    table_batches.insert(TABLE_BUSES, build_buses_batch(&network.buses, &bus_aggregates)?);
    table_batches.insert(
        TABLE_BRANCHES,
        build_branches_batch(&network.branches, &bus_nominal_kv, base_mva)?,
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
    table_batches.insert(TABLE_TRANSFORMERS_3W, empty_table(TABLE_TRANSFORMERS_3W)?);
    table_batches.insert(TABLE_AREAS, build_areas_batch(&network.areas)?);
    table_batches.insert(TABLE_ZONES, build_zones_batch(&network.zones)?);
    table_batches.insert(TABLE_OWNERS, build_owners_batch(&network.owners)?);
    table_batches.insert(TABLE_CONTINGENCIES, empty_table(TABLE_CONTINGENCIES)?);
    table_batches.insert(TABLE_INTERFACES, empty_table(TABLE_INTERFACES)?);
    let dynamics_batch = if network.dyr_generators.is_empty() {
        empty_table(TABLE_DYNAMICS_MODELS)?
    } else {
        build_dynamics_models_batch(&network.dyr_generators)?
    };
    table_batches.insert(TABLE_DYNAMICS_MODELS, dynamics_batch);

    let root_options = RootWriteOptions {
        contingencies_are_stub: true,
        dynamics_are_stub: network.dyr_generators.is_empty(),
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
    additional_root_metadata.insert(
        METADATA_KEY_CASE_MODE.to_string(),
        case_mode.to_string(),
    );
    // v0.8.5: solved_state_presence — this converter never produces solved data.
    additional_root_metadata.insert(
        METADATA_KEY_SOLVED_STATE_PRESENCE.to_string(),
        "not_computed".to_string(),
    );

    write_root_rpf_with_metadata(output, &table_batches, &root_options, &additional_root_metadata)
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
        warnings.push("RAW produced 0 branches; network may be disconnected or incomplete".to_string());
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

        let mut in_service_gen_keys: HashSet<(u32, &str)> = HashSet::with_capacity(network.generators.len());
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

        let mut dyr_keys: HashSet<(u32, &str)> = HashSet::with_capacity(network.dyr_generators.len());
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
    let schema = table_schema(name)
        .ok_or_else(|| anyhow::anyhow!("unknown canonical table: {name}"))?;
    Ok(RecordBatch::new_empty(Arc::new(schema)))
}

fn build_bus_nominal_kv_map(network: &Network) -> HashMap<u32, f64> {
    network
        .buses
        .iter()
        .map(|b| (b.i, b.baskv))
        .collect::<HashMap<_, _>>()
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
    network
        .buses
        .iter()
        .for_each(|b| {
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
    let is_flat = network.buses.iter().all(|b| {
        (b.vm - 1.0).abs() < 1.0e-4 && b.va.abs() < 1.0e-4
    });
    if is_flat {
        "flat_start_planning"
    } else {
        "warm_start_planning"
    }
}

// ---------------------------------------------------------------------------
// Table builders
// ---------------------------------------------------------------------------

fn build_metadata_batch(network: &Network, case_fingerprint_value: &str, case_mode: &str) -> Result<RecordBatch> {
    let schema = Arc::new(
        table_schema(TABLE_METADATA).expect("metadata schema must exist"),
    );

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
    timestamp_utc.append_value("2026-01-01T00:00:00Z"); // TODO: real system time
    let mut snapshot_timestamp_utc = StringBuilder::new();
    snapshot_timestamp_utc.append_value("2026-01-01T00:00:00Z");
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
        let mut agg = BusAggregate::default();
        agg.g_shunt = bus.gl / base_mva;
        agg.b_shunt = bus.bl / base_mva;
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
        let mut q_min_val = agg.q_min;
        let mut q_max_val = agg.q_max;
        if q_min_val > q_max_val {
            std::mem::swap(&mut q_min_val, &mut q_max_val);
        }

        bus_id.append_value(bus.i as i32);
        name.append_value(bus.name.as_ref());
        bus_type.append_value(bus.ide as i8);
        p_sched.append_value(agg.p_sched);
        q_sched.append_value(agg.q_sched);
        // v0.8.5: preserve RAW voltage setpoints for warm-start parity.
        // Use generator VS for regulated buses; fallback to RAW bus VM so
        // solved NYISO/external snapshots retain their initial conditions.
        v_mag_set.append_value(agg.v_mag_set_override.unwrap_or(bus.vm));
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

    for (idx, branch) in branches.iter().enumerate() {
        branch_id.append_value((idx + 1) as i32);
        from_bus_id.append_value(branch.i as i32);
        to_bus_id.append_value(branch.j as i32);
        ckt.append_value(branch.ckt.as_ref());
        r.append_value(branch.r);
        x.append_value(branch.x);
        b_shunt.append_value(branch.b);
        tap.append_value(1.0);   // PSS/E lines always have tap = 1.0
        phase.append_value(0.0); // no phase shift on line branches
        rate_a.append_value(branch.ratea / base_mva);
        rate_b.append_value(branch.rateb / base_mva);
        rate_c.append_value(branch.ratec / base_mva);
        status.append_value(branch.st != 0);
        name_b.append_null(); // branches have no name in RAW
        from_nominal_kv.append_option(bus_nominal_kv.get(&branch.i).copied());
        to_nominal_kv.append_option(bus_nominal_kv.get(&branch.j).copied());
    }

    let n_rows = branches.len();
    let dict_int32_utf8 = arrow::datatypes::DataType::Dictionary(
        Box::new(arrow::datatypes::DataType::Int32),
        Box::new(arrow::datatypes::DataType::Utf8),
    );
    let map_str_f64 = arrow::datatypes::DataType::Map(
        std::sync::Arc::new(arrow::datatypes::Field::new(
            "entries",
            arrow::datatypes::DataType::Struct(
                vec![
                    arrow::datatypes::Field::new("key", arrow::datatypes::DataType::Utf8, false),
                    arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );

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
            // v0.8.6: FACTS control metadata — all null for non-FACTS branches
            new_null_array(&dict_int32_utf8, n_rows),
            new_null_array(&dict_int32_utf8, n_rows),
            new_null_array(&arrow::datatypes::DataType::Float64, n_rows),
            new_null_array(&arrow::datatypes::DataType::Float64, n_rows),
            new_null_array(&arrow::datatypes::DataType::Float64, n_rows),
            new_null_array(&arrow::datatypes::DataType::Float64, n_rows),
            new_null_array(&arrow::datatypes::DataType::Float64, n_rows),
            new_null_array(&map_str_f64, n_rows),
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
    let schema = Arc::new(table_schema(TABLE_FIXED_SHUNTS).expect("fixed_shunts schema must exist"));

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

fn build_switched_shunts_batch(shunts: &[models::SwitchedShunt], base_mva: f64) -> Result<RecordBatch> {
    let schema = Arc::new(
        table_schema(TABLE_SWITCHED_SHUNTS).expect("switched_shunts schema must exist"),
    );

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
    let mut bus_shunt_counter: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();

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
    let schema = Arc::new(
        table_schema(TABLE_TRANSFORMERS_2W).expect("transformers_2w schema must exist"),
    );

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
        nominal_tap_ratio.append_value(1.0); // TODO: derive from NOMV1/NOMV2
        phase_shift.append_value(t.ang1.to_radians());
        vector_group.append_value("Yy0"); // TODO: derive from CW/CZ
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

fn build_dynamics_models_batch(records: &[models::DyrGeneratorData]) -> Result<RecordBatch> {
    let schema = Arc::new(
        table_schema(TABLE_DYNAMICS_MODELS).expect("dynamics_models schema must exist"),
    );

    let mut bus_id = Int32Builder::new();
    let mut gen_id = StringDictionaryBuilder::<Int32Type>::new();
    let mut model_type = StringDictionaryBuilder::<Int32Type>::new();
    // Map<Utf8, Float64> — field names must match the schema: "entries" / "key" / "value"
    let map_field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut params = MapBuilder::new(Some(map_field_names), StringBuilder::new(), Float64Builder::new());

    for rec in records {
        bus_id.append_value(rec.bus_id as i32);
        gen_id.append_value(rec.id.as_ref());
        model_type.append_value(rec.model.as_ref());

        params.keys().append_value("H");
        params.values().append_value(rec.h);
        params.keys().append_value("D");
        params.values().append_value(rec.d);
        params.keys().append_value("xd_prime");
        params.values().append_value(rec.xd_prime);
        params.append(true).context("building dynamics params map entry")?;
    }

    let params_arr = params.finish();
    // Cast to the exact schema type to align nullability (Float64Builder emits
    // nullable values; the canonical schema requires non-null Float64 values).
    let params_target_type = schema
        .field_with_name("params")
        .expect("params field must exist in dynamics_models schema")
        .data_type()
        .clone();
    let params_cast =
        arrow::compute::cast(&params_arr, &params_target_type).context("casting dynamics params map")?;

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
