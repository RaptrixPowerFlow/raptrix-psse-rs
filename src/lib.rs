// Raptrix CIM-Arrow â€” High-performance open CIM profile by Musto Technologies LLC
// Copyright (c) 2026 Musto Technologies LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! `raptrix-psse-rs` â€” High-performance PSS/E (`.raw` + `.dyr`) â†’
//! Raptrix PowerFlow Interchange v0.6.0 converter.
//!
//! # Crate layout
//! * [`models`] â€” PSS/E data structures.
//! * [`parser`] â€” PSS/E `.raw` / `.dyr` parser.
//!
//! Serialisation to `.rpf` is delegated to the [`raptrix_cim_arrow`] crate.
//!
//! # Branding
//! Raptrix CIM-Arrow â€” High-performance open CIM profile by Musto Technologies LLC
//! Copyright (c) 2026 Musto Technologies LLC

pub mod models;
pub mod parser;

// Re-export reader utilities so tests and tools can use them directly.
pub use raptrix_cim_arrow::{read_rpf_tables, summarize_rpf, RpfSummary, TableSummary};

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::{
        new_null_array, BooleanBuilder, Float64Builder, Int32Builder,
        Int8Builder, ListBuilder, StringBuilder, StringDictionaryBuilder,
    },
    datatypes::{Int32Type, UInt32Type},
    record_batch::RecordBatch,
};
use raptrix_cim_arrow::{
    table_schema, write_root_rpf, RootWriteOptions,
    TABLE_AREAS, TABLE_BRANCHES, TABLE_BUSES, TABLE_CONTINGENCIES,
    TABLE_DYNAMICS_MODELS, TABLE_FIXED_SHUNTS, TABLE_GENERATORS, TABLE_INTERFACES,
    TABLE_LOADS, TABLE_METADATA, TABLE_OWNERS, TABLE_SWITCHED_SHUNTS,
    TABLE_TRANSFORMERS_2W, TABLE_TRANSFORMERS_3W, TABLE_ZONES,
};

use crate::models::Network;

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

    let mut table_batches: HashMap<&'static str, RecordBatch> = HashMap::new();

    table_batches.insert(TABLE_METADATA, build_metadata_batch(&network)?);
    table_batches.insert(TABLE_BUSES, build_buses_batch(&network.buses)?);
    table_batches.insert(TABLE_BRANCHES, build_branches_batch(&network.branches)?);
    table_batches.insert(TABLE_GENERATORS, build_generators_batch(&network.generators)?);
    table_batches.insert(TABLE_LOADS, build_loads_batch(&network.loads)?);
    table_batches.insert(TABLE_FIXED_SHUNTS, build_fixed_shunts_batch(&network.fixed_shunts)?);
    table_batches.insert(TABLE_SWITCHED_SHUNTS, build_switched_shunts_batch(&network.switched_shunts)?);
    table_batches.insert(TABLE_TRANSFORMERS_2W, build_transformers_2w_batch(&network.transformers)?);
    table_batches.insert(TABLE_TRANSFORMERS_3W, empty_table(TABLE_TRANSFORMERS_3W)?);
    table_batches.insert(TABLE_AREAS, build_areas_batch(&network.areas)?);
    table_batches.insert(TABLE_ZONES, build_zones_batch(&network.zones)?);
    table_batches.insert(TABLE_OWNERS, build_owners_batch(&network.owners)?);
    table_batches.insert(TABLE_CONTINGENCIES, empty_table(TABLE_CONTINGENCIES)?);
    table_batches.insert(TABLE_INTERFACES, empty_table(TABLE_INTERFACES)?);
    table_batches.insert(TABLE_DYNAMICS_MODELS, empty_table(TABLE_DYNAMICS_MODELS)?);

    write_root_rpf(output, &table_batches, &RootWriteOptions::default())
        .with_context(|| format!("failed to write RPF file: {output}"))?;

    eprintln!("[converter] wrote {output}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: empty 0-row table
// ---------------------------------------------------------------------------

fn empty_table(name: &'static str) -> Result<RecordBatch> {
    let schema = table_schema(name)
        .ok_or_else(|| anyhow::anyhow!("unknown canonical table: {name}"))?;
    Ok(RecordBatch::new_empty(Arc::new(schema)))
}

// ---------------------------------------------------------------------------
// Table builders
// ---------------------------------------------------------------------------

fn build_metadata_batch(network: &Network) -> Result<RecordBatch> {
    let schema = Arc::new(
        table_schema(TABLE_METADATA).expect("metadata schema must exist"),
    );

    // simple scalar columns
    let base_mva = arrow::array::Float64Array::from(vec![network.case_id.sbase]);
    let frequency_hz = arrow::array::Float64Array::from(vec![network.case_id.basfrq]);
    let psse_version = arrow::array::Int32Array::from(vec![network.case_id.rev as i32]);
    let is_planning_case = arrow::array::BooleanArray::from(vec![false]);

    // dict string columns
    let mut study_name = StringDictionaryBuilder::<Int32Type>::new();
    study_name.append_value(network.case_id.title.as_ref());

    // plain string columns
    let mut timestamp_utc = StringBuilder::new();
    timestamp_utc.append_value("2026-01-01T00:00:00Z"); // TODO: real system time
    let mut raptrix_version = StringBuilder::new();
    raptrix_version.append_value(env!("CARGO_PKG_VERSION"));

    // custom_metadata is nullable â€” emit a single null value.
    let custom_meta_type = schema
        .field_with_name("custom_metadata")
        .expect("custom_metadata field must exist in metadata schema")
        .data_type()
        .clone();
    let custom_metadata = new_null_array(&custom_meta_type, 1);

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
            custom_metadata,
        ],
    )
    .context("building metadata batch")
}

fn build_buses_batch(buses: &[models::Bus]) -> Result<RecordBatch> {
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

    for bus in buses {
        bus_id.append_value(bus.i as i32);
        name.append_value(bus.name.as_ref());
        bus_type.append_value(bus.ide as i8);
        p_sched.append_value(0.0); // TODO: aggregate from generators/loads
        q_sched.append_value(0.0);
        v_mag_set.append_value(bus.vm);
        v_ang_set.append_value(bus.va);
        q_min.append_value(0.0); // TODO: aggregate from generators
        q_max.append_value(0.0);
        g_shunt.append_value(0.0); // TODO: aggregate from fixed_shunts by bus
        b_shunt.append_value(0.0);
        area.append_value(bus.area as i32);
        zone.append_value(bus.zone as i32);
        owner.append_value(bus.owner as i32);
        v_min.append_value(bus.nvlo);
        v_max.append_value(bus.nvhi);
        p_min_agg.append_value(0.0); // TODO: aggregate from generators
        p_max_agg.append_value(0.0);
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
        ],
    )
    .context("building buses batch")
}

fn build_branches_batch(branches: &[models::Branch]) -> Result<RecordBatch> {
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
        rate_a.append_value(branch.ratea);
        rate_b.append_value(branch.rateb);
        rate_c.append_value(branch.ratec);
        status.append_value(branch.st != 0);
        name_b.append_null(); // branches have no name in RAW
    }

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
        ],
    )
    .context("building branches batch")
}

fn build_generators_batch(generators: &[models::Generator]) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_GENERATORS).expect("generators schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut id = StringDictionaryBuilder::<Int32Type>::new();
    let mut p_sched_mw = Float64Builder::new();
    let mut p_min_mw = Float64Builder::new();
    let mut p_max_mw = Float64Builder::new();
    let mut q_min_mvar = Float64Builder::new();
    let mut q_max_mvar = Float64Builder::new();
    let mut status = BooleanBuilder::new();
    let mut mbase_mva = Float64Builder::new();
    let mut h = Float64Builder::new();
    let mut xd_prime = Float64Builder::new();
    let mut d = Float64Builder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();

    for gen in generators {
        bus_id.append_value(gen.i as i32);
        id.append_value(gen.id.as_ref());
        p_sched_mw.append_value(gen.pg);
        p_min_mw.append_value(gen.pb);
        p_max_mw.append_value(gen.pt);
        q_min_mvar.append_value(gen.qb);
        q_max_mvar.append_value(gen.qt);
        status.append_value(gen.stat != 0);
        mbase_mva.append_value(gen.mbase);
        h.append_value(0.0);        // TODO: from DYR GENSAL/GENROU
        xd_prime.append_value(gen.zx); // stub: ZX as xd'
        d.append_value(0.0);        // TODO: from DYR
        name_b.append_null();
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(id.finish()),
            Arc::new(p_sched_mw.finish()),
            Arc::new(p_min_mw.finish()),
            Arc::new(p_max_mw.finish()),
            Arc::new(q_min_mvar.finish()),
            Arc::new(q_max_mvar.finish()),
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

fn build_loads_batch(loads: &[models::Load]) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_LOADS).expect("loads schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut id = StringDictionaryBuilder::<Int32Type>::new();
    let mut status = BooleanBuilder::new();
    let mut p_mw = Float64Builder::new();
    let mut q_mvar = Float64Builder::new();
    let mut name_b = StringDictionaryBuilder::<UInt32Type>::new();

    for load in loads {
        bus_id.append_value(load.i as i32);
        id.append_value(load.id.as_ref());
        status.append_value(load.status != 0);
        p_mw.append_value(load.pl);
        q_mvar.append_value(load.ql);
        name_b.append_null();
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(id.finish()),
            Arc::new(status.finish()),
            Arc::new(p_mw.finish()),
            Arc::new(q_mvar.finish()),
            Arc::new(name_b.finish()),
        ],
    )
    .context("building loads batch")
}

fn build_fixed_shunts_batch(shunts: &[models::FixedShunt]) -> Result<RecordBatch> {
    let schema = Arc::new(table_schema(TABLE_FIXED_SHUNTS).expect("fixed_shunts schema must exist"));

    let mut bus_id = Int32Builder::new();
    let mut id = StringDictionaryBuilder::<Int32Type>::new();
    let mut status = BooleanBuilder::new();
    let mut g_mw = Float64Builder::new();
    let mut b_mvar = Float64Builder::new();

    for shunt in shunts {
        bus_id.append_value(shunt.i as i32);
        id.append_value(shunt.id.as_ref());
        status.append_value(shunt.status != 0);
        g_mw.append_value(shunt.gl);
        b_mvar.append_value(shunt.bl);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(bus_id.finish()),
            Arc::new(id.finish()),
            Arc::new(status.finish()),
            Arc::new(g_mw.finish()),
            Arc::new(b_mvar.finish()),
        ],
    )
    .context("building fixed_shunts batch")
}

fn build_switched_shunts_batch(shunts: &[models::SwitchedShunt]) -> Result<RecordBatch> {
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

    for shunt in shunts {
        bus_id.append_value(shunt.i as i32);
        status.append_value(shunt.stat != 0);
        v_low.append_value(shunt.vswlo);
        v_high.append_value(shunt.vswhi);
        // Append each step value to the inner list
        for &step in &shunt.steps {
            b_steps.values().append_value(step);
        }
        b_steps.append(true);
        // Estimate current step count from BINIT / first non-zero step
        let step_count = if let Some(&s) = shunt.steps.first() {
            if s != 0.0 {
                (shunt.binit / s).round() as i32
            } else {
                0
            }
        } else {
            0
        };
        current_step.append_value(step_count);
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
        ],
    )
    .context("building switched_shunts batch")
}

fn build_transformers_2w_batch(
    transformers: &[models::TwoWindingTransformer],
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
        phase_shift.append_value(t.ang1);
        vector_group.append_value("Yy0"); // TODO: derive from CW/CZ
        rate_a.append_value(t.rata1);
        rate_b.append_value(t.ratb1);
        rate_c.append_value(t.ratc1);
        status.append_value(t.stat != 0);
        name_b.append_null();
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
