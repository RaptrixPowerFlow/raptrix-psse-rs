#!/usr/bin/env python3
"""
robust_harness.py - tiny wrapper around the same kpf APIs used by
raptrix-core's pv_pq_harness, but resilient to parse-time crashes.

If kpf.parse_rpf / kpf.parse_psse_raw raises (e.g. "266 disconnected
islands detected" topology errors), the case is logged as
converged=false / status="parse_error" and the run continues. This is
strictly a delta-report convenience tool - the canonical harness still
exists upstream.

Outputs the same JSONL schema as pv_pq_harness.py for the fields the
parse_log.py formatter cares about: timestamp, run_key, case, format,
mode, solve_phase, converged, iterations, tolerance, q_violations,
q_switches, status, time_sec, raptrix_version_info, raptrix_version,
pyd_info, pyd_path.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from python_tests._raptrix_env import ensure_local_build
except ImportError:
    ensure_local_build = None

if ensure_local_build is not None:
    try:
        ensure_local_build()
    except Exception as exc:
        print(f"ERROR: ensure_local_build failed: {exc}", file=sys.stderr)
        sys.exit(1)

import raptrix_powerflow as kpf  # noqa: E402
from raptrix_powerflow import NewtonRaphson  # noqa: E402


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = -1) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _extract_tolerance(result: Dict[str, Any]) -> float:
    mh = result.get("mismatch_history")
    if mh is not None:
        try:
            if len(mh) > 0:
                return _to_float(mh[-1], 1e9)
        except Exception:
            pass
    return _to_float(result.get("final_tolerance", 1e9), 1e9)


def _pyd_info() -> Tuple[str, str]:
    try:
        core_mod = getattr(kpf, "_core", None)
        if core_mod is not None and getattr(core_mod, "__file__", None):
            pyd_path = Path(core_mod.__file__)
        else:
            pyd_path = Path(kpf.__file__)
        mtime = datetime.datetime.fromtimestamp(pyd_path.stat().st_mtime)
        with open(pyd_path, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()[:16]
        return f"{mtime.isoformat()} (sha256:{digest})", str(pyd_path)
    except Exception:
        return "unknown", "unknown"


def discover(raw_dir: Path, rpf_dir: Path) -> Dict[str, List[Path]]:
    raws = sorted(set(list(raw_dir.glob("*.raw")) + list(raw_dir.glob("*.RAW"))))
    rpfs = sorted(set(list(rpf_dir.glob("*.rpf")) + list(rpf_dir.glob("*.RPF"))))
    return {"raw": raws, "rpf": rpfs}


def solve_one(network: Any, mode: str) -> Dict[str, Any]:
    solver = NewtonRaphson(
        network,
        tol=1e-6,
        max_iters=75,
        enable_lm_damping=True,
        enable_dc_init=True,
        max_q_switch_per_iter=5,
    )
    start = time.perf_counter()
    result = solver.solve(
        hot_start=False,
        apply_q_limits=(mode == "pq"),
        return_mismatch_history=True,
    )
    elapsed = time.perf_counter() - start
    return {
        "converged": bool(result.get("converged", False)),
        "iterations": _to_int(result.get("iters_taken", result.get("iterations", -1))),
        "tolerance": _extract_tolerance(result),
        "q_violations": len(result.get("q_violations", [])),
        "q_switches": _to_int(result.get("q_switches_count", 0), 0),
        "max_mismatch_mw": _to_float(result.get("max_mismatch_mw", 0.0)),
        "status": str(result.get("status", "unknown")),
        "time_sec": round(elapsed, 6),
    }


def parse_one(case_path: Path) -> Any:
    suffix = case_path.suffix.lower()
    if suffix == ".raw":
        return kpf.parse_psse_raw(str(case_path))
    if suffix == ".rpf":
        return kpf.parse_rpf(str(case_path))
    raise ValueError(f"Unsupported extension: {case_path}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--raw-dir", required=True, type=Path)
    p.add_argument("--rpf-dir", required=True, type=Path)
    p.add_argument("--modes", default="pv,pq")
    p.add_argument("--log", required=True, type=Path)
    p.add_argument("--label", default="")
    args = p.parse_args()

    args.log.parent.mkdir(parents=True, exist_ok=True)
    args.log.write_text("", encoding="utf-8")

    raptrix_version_info = str(getattr(kpf, "__version__", "unknown")).strip('"').strip("'")
    pyd_str, pyd_path = _pyd_info()
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    cases = discover(args.raw_dir, args.rpf_dir)
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    print(
        f"robust_harness: {len(cases['raw'])} raw + {len(cases['rpf'])} rpf, modes={modes}, "
        f"label={args.label!r}"
    )

    errors = 0
    for fmt in ("raw", "rpf"):
        for case_path in cases[fmt]:
            for mode in modes:
                run_key = f"{fmt}|{case_path.name}|{mode}|cold"
                row: Dict[str, Any] = {
                    "timestamp": timestamp,
                    "run_key": run_key,
                    "raptrix_version_info": raptrix_version_info,
                    "raptrix_version": raptrix_version_info,
                    "pyd_info": pyd_str,
                    "pyd_path": pyd_path,
                    "case": case_path.name,
                    "format": fmt,
                    "mode": mode,
                    "solve_phase": "cold",
                    "hot_start_used": False,
                    "label": args.label,
                }
                try:
                    network = parse_one(case_path)
                except Exception as exc:
                    errors += 1
                    row.update({
                        "converged": False,
                        "iterations": -1,
                        "tolerance": float("inf"),
                        "q_violations": -1,
                        "q_switches": -1,
                        "status": f"parse_error:{type(exc).__name__}",
                        "parse_error_message": str(exc).split("\n", 1)[0],
                        "time_sec": 0.0,
                    })
                    print(f"  PARSE_ERROR {fmt}/{case_path.name}/{mode}: {exc}".replace("\n", " "), flush=True)
                    with open(args.log, "a", encoding="utf-8") as f:
                        f.write(json.dumps(row) + "\n")
                    continue

                try:
                    metrics = solve_one(network, mode)
                    row.update(metrics)
                    sym = "OK" if metrics["converged"] else "FAIL"
                    print(
                        f"  {sym} {fmt}/{case_path.name}/{mode} iters={metrics['iterations']} "
                        f"tol={metrics['tolerance']:.3e} time={metrics['time_sec']:.3f}s",
                        flush=True,
                    )
                except Exception as exc:
                    errors += 1
                    row.update({
                        "converged": False,
                        "iterations": -1,
                        "tolerance": float("inf"),
                        "q_violations": -1,
                        "q_switches": -1,
                        "status": f"solve_error:{type(exc).__name__}",
                        "solve_error_message": str(exc).split("\n", 1)[0],
                        "time_sec": 0.0,
                    })
                    print(
                        f"  SOLVE_ERROR {fmt}/{case_path.name}/{mode}: {exc}".replace("\n", " "),
                        flush=True,
                    )

                with open(args.log, "a", encoding="utf-8") as f:
                    f.write(json.dumps(row) + "\n")

    print(f"robust_harness done. cases_seen={len(cases['raw']) + len(cases['rpf'])}, errors={errors}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
