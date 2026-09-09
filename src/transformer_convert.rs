// raptrix-psse-rs
// Copyright (c) 2026 Raptrix PowerFlow
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// https://mozilla.org/MPL/2.0/.

//! PSS/E transformer CW / CZ / CM → RPF system-base units.
//!
//! RPF `transformers_2w.r` / `x` are pu on system SBASE. `tap_ratio` is the
//! from-side off-nominal turns ratio (winding-1 pu of BASKV over winding-2).
//! RAW stores those quantities in the coding given by CW / CZ / CM.
//!
//! PSS®E Program Operation Manual (winding / impedance / magnetizing modes):
//!
//! * **CW = 1** (default): WINDV is pu of the connected-bus BASKV.
//! * **CW = 2**: WINDV is winding voltage in kV. pu = WINDV / BASKV.
//! * **CW = 3**: WINDV is pu of winding NOMV. pu of BASKV = WINDV × NOMV / BASKV
//!   (NOMV = 0 means use BASKV).
//! * **CZ = 1** (default): R, X already pu on system SBASE.
//! * **CZ = 2**: R, X pu on winding SBASE. `Z_sys = Z × SBASE / SBASE_w`.
//! * **CZ = 3**: R is load loss in watts; X is |Z| pu on winding SBASE.
//!   `R_w = R / (SBASE_w × 1e6)`, `X_w = sign(X) × sqrt(X² − R_w²)`, then
//!   scale like CZ = 2. This is **not** ohms.
//! * **CM = 1** (default): MAG1 / MAG2 already pu on system SBASE.
//! * **CM = 2**: rejected — this crate has no in-repo no-load-loss / exciting-
//!   current formula.
//!
//! A missing / zero code is treated as 1 (PSS/E default). Unknown codes error
//! with I, J, K so a deck is not silently written in the wrong base.

use anyhow::{Result, bail};

/// Identifies a RAW transformer record in convert errors.
#[derive(Debug, Clone, Copy)]
pub struct TransformerId {
    pub i: u32,
    pub j: u32,
    pub k: u32,
}

impl std::fmt::Display for TransformerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "I={} J={} K={}", self.i, self.j, self.k)
    }
}

/// PSS/E default for a missing CW / CZ / CM token is 1.
#[inline]
pub fn normalize_code(code: u8) -> u8 {
    if code == 0 { 1 } else { code }
}

/// Reject unknown CW / CZ and CM ≠ 1. Call at parse so a bad deck fails before
/// 3W star expansion or RPF write.
pub fn validate_transformer_codes(id: TransformerId, cw: u8, cz: u8, cm: u8) -> Result<()> {
    let cw_n = normalize_code(cw);
    let cz_n = normalize_code(cz);
    let cm_n = normalize_code(cm);
    if !(1..=3).contains(&cw_n) || !(1..=3).contains(&cz_n) || !(1..=2).contains(&cm_n) {
        bail!("RAW TRANSFORMER {id} unsupported codes CW={cw} CZ={cz} CM={cm}");
    }
    if cm_n != 1 {
        bail!(
            "RAW TRANSFORMER {id} CM={cm} unsupported (no in-repo MAG CM=2 watts / exciting-current formula)"
        );
    }
    Ok(())
}

/// Winding voltage in pu of the connected-bus BASKV.
pub fn winding_pu_of_baskv(
    id: TransformerId,
    cw: u8,
    windv: f64,
    nomv_kv: f64,
    baskv: f64,
) -> Result<f64> {
    match normalize_code(cw) {
        1 => Ok(windv),
        2 => {
            if !(baskv > 1.0e-9) {
                bail!("RAW TRANSFORMER {id} CW=2 BASKV<=0");
            }
            Ok(windv / baskv)
        }
        3 => {
            let vnom = if nomv_kv > 1.0e-9 { nomv_kv } else { baskv };
            if !(baskv > 1.0e-9) {
                bail!("RAW TRANSFORMER {id} CW=3 BASKV<=0");
            }
            Ok(windv * vnom / baskv)
        }
        other => bail!("RAW TRANSFORMER {id} unsupported CW={other}"),
    }
}

/// From-side off-nominal tap = (winding-1 pu of BASKV) / (winding-2 pu of BASKV).
pub fn tap_ratio_from_cw(
    id: TransformerId,
    cw: u8,
    windv1: f64,
    windv2: f64,
    nomv1: f64,
    nomv2: f64,
    baskv1: f64,
    baskv2: f64,
) -> Result<f64> {
    let t1 = winding_pu_of_baskv(id, cw, windv1, nomv1, baskv1)?;
    let t2 = winding_pu_of_baskv(id, cw, windv2, nomv2, baskv2)?;
    if t2.abs() <= 1.0e-12 {
        bail!("RAW TRANSFORMER {id} winding-2 pu tap is zero");
    }
    Ok(t1 / t2)
}

/// Series R, X in pu on system SBASE.
pub fn convert_z_to_system(
    id: TransformerId,
    cz: u8,
    r: f64,
    x: f64,
    sbase_winding: f64,
    sbase_system: f64,
) -> Result<(f64, f64)> {
    match normalize_code(cz) {
        1 => Ok((r, x)),
        2 => {
            if !(sbase_winding > 1.0e-9) {
                bail!("RAW TRANSFORMER {id} CZ=2 winding SBASE<=0");
            }
            if !(sbase_system > 1.0e-9) {
                bail!("RAW TRANSFORMER {id} CZ=2 system SBASE<=0");
            }
            let sc = sbase_system / sbase_winding;
            Ok((r * sc, x * sc))
        }
        3 => {
            if !(sbase_winding > 1.0e-9) {
                bail!("RAW TRANSFORMER {id} CZ=3 winding SBASE<=0");
            }
            if !(sbase_system > 1.0e-9) {
                bail!("RAW TRANSFORMER {id} CZ=3 system SBASE<=0");
            }
            // R = load loss (W); X = |Z| pu on winding MVA base. Not ohms.
            let r_w = r / (sbase_winding * 1.0e6);
            let zmag = x.abs();
            let inner = zmag * zmag - r_w * r_w;
            if inner < 0.0 {
                bail!(
                    "RAW TRANSFORMER {id} CZ=3 |Z|^2 < R_w^2 (watts={r}, |Z|={x}, SBASE_w={sbase_winding})"
                );
            }
            let sign = if x < 0.0 { -1.0 } else { 1.0 };
            let x_w = sign * inner.sqrt();
            let sc = sbase_system / sbase_winding;
            Ok((r_w * sc, x_w * sc))
        }
        other => bail!("RAW TRANSFORMER {id} unsupported CZ={other}"),
    }
}

/// MAG1 / MAG2 in pu on system SBASE (CM = 1 only).
pub fn convert_mag_to_system(
    id: TransformerId,
    cm: u8,
    mag1: f64,
    mag2: f64,
) -> Result<(f64, f64)> {
    validate_transformer_codes(id, 1, 1, cm)?;
    Ok((mag1, mag2))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: TransformerId = TransformerId { i: 4, j: 7, k: 0 };

    #[test]
    fn missing_code_is_one() {
        assert_eq!(normalize_code(0), 1);
        assert_eq!(normalize_code(2), 2);
    }

    #[test]
    fn cw1_tap_is_windv_ratio() {
        let tap = tap_ratio_from_cw(ID, 1, 0.978, 1.0, 138.0, 18.0, 138.0, 18.0).unwrap();
        assert!((tap - 0.978).abs() < 1e-12);
    }

    #[test]
    fn cw2_uses_baskv_not_nomv() {
        // WINDV in kV; NOMV deliberately different from BASKV so a NOMV divide fails.
        let tap = tap_ratio_from_cw(ID, 2, 241.5, 115.0, 220.0, 110.0, 230.0, 115.0).unwrap();
        assert!((tap - (241.5 / 230.0)).abs() < 1e-12);
    }

    #[test]
    fn cw3_scales_by_nomv_over_baskv() {
        let tap = tap_ratio_from_cw(ID, 3, 1.05, 1.0, 241.5, 115.0, 230.0, 115.0).unwrap();
        let want = (1.05 * 241.5 / 230.0) / (1.0 * 115.0 / 115.0);
        assert!((tap - want).abs() < 1e-12);
    }

    #[test]
    fn cz2_scales_by_sbase_ratio() {
        let (r, x) = convert_z_to_system(ID, 2, 0.0, 0.20912, 50.0, 100.0).unwrap();
        assert!((r - 0.0).abs() < 1e-15);
        assert!((x - 0.41824).abs() < 1e-12);
    }

    #[test]
    fn cz1_is_identity() {
        let (r, x) = convert_z_to_system(ID, 1, 0.01, 0.10, 50.0, 100.0).unwrap();
        assert!((r - 0.01).abs() < 1e-15);
        assert!((x - 0.10).abs() < 1e-15);
    }

    #[test]
    fn cz3_is_watts_and_zmag_not_ohms() {
        // 500 kW load loss on 50 MVA → R_w = 0.01 pu. |Z| = 0.10 pu on 50 MVA.
        // System SBASE = 100 → scale 2.
        let (r, x) = convert_z_to_system(ID, 3, 500_000.0, 0.10, 50.0, 100.0).unwrap();
        let r_w: f64 = 500_000.0 / (50.0 * 1.0e6);
        let x_w = (0.10_f64.powi(2) - r_w.powi(2)).sqrt();
        assert!((r - r_w * 2.0).abs() < 1e-12);
        assert!((x - x_w * 2.0).abs() < 1e-12);
        // The ohms mis-read (R * SBASE / V^2) is orders of magnitude larger.
        assert!(r < 0.1);
    }

    #[test]
    fn unknown_cw_is_rejected() {
        let err = validate_transformer_codes(ID, 9, 1, 1).unwrap_err();
        assert!(err.to_string().contains("CW=9"));
    }

    #[test]
    fn cm2_is_rejected() {
        let err = validate_transformer_codes(ID, 1, 1, 2).unwrap_err();
        assert!(err.to_string().contains("CM=2"));
    }
}
