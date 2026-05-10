#!/usr/bin/env python3
"""
parse_log.py — Compact RAW vs RPF delta-report formatter.

Reads:
  --regression-log    JSONL produced by python_tests/regression/pv_pq_harness.py
  --converter-stderr  stderr captured from raptrix-psse-rs convert runs
  --harness-stderr    stderr captured from the harness (carries importer warnings)

Writes:
  --output            Markdown delta report

Pairs RAW vs RPF runs by case stem (filename without extension; trailing
"_static" stripped on the RPF side). Computes per-case deltas for converged,
iterations, tolerance, q_switches, q_violations across pv/pq modes and
solve phases (cold/hot when present). Tallies importer warning counts by
class (sanitized v_mag, auto-assigned slack, structural PV->PQ demotion,
seed_only seeded buses, etc.).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Stem normalization
# ---------------------------------------------------------------------------

def case_stem(name: str) -> str:
    """Lowercase stem with trailing _static / _dynamic suffix removed."""
    # Strip extension
    base = name
    for ext in (".rpf", ".RPF", ".raw", ".RAW"):
        if base.endswith(ext):
            base = base[: -len(ext)]
            break
    base = base.lower()
    for suffix in ("_static", "_dynamic"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
    return base


# ---------------------------------------------------------------------------
# JSONL parsing
# ---------------------------------------------------------------------------

@dataclass
class RunMetric:
    case: str
    fmt: str
    mode: str
    solve_phase: str
    converged: bool
    iterations: int
    tolerance: float
    q_violations: int
    q_switches: int
    timestamp: str


def load_regression_log(path: Path) -> List[RunMetric]:
    if not path.exists():
        return []
    out: List[RunMetric] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            try:
                out.append(
                    RunMetric(
                        case=case_stem(str(row.get("case", ""))),
                        fmt=str(row.get("format", "")).lower(),
                        mode=str(row.get("mode", "")).lower(),
                        solve_phase=str(row.get("solve_phase", "")).lower(),
                        converged=bool(row.get("converged", False)),
                        iterations=int(row.get("iterations", -1) or -1),
                        tolerance=float(row.get("tolerance", float("inf")) or float("inf")),
                        q_violations=int(row.get("q_violations", 0) or 0),
                        q_switches=int(row.get("q_switches", 0) or 0),
                        timestamp=str(row.get("timestamp", "")),
                    )
                )
            except Exception:
                continue
    return out


# ---------------------------------------------------------------------------
# stderr warning tally
# ---------------------------------------------------------------------------

WARNING_PATTERNS = [
    ("sanitized_v_mag",
     re.compile(r"sanitized\s+(\d+)\s+invalid bus voltage magnitudes", re.IGNORECASE)),
    ("auto_assigned_slack",
     re.compile(r"Auto-assigned bus\s+\d+\s+as slack", re.IGNORECASE)),
    ("structural_pv_to_pq",
     re.compile(r"structural guard demoted\s+(\d+)\s+PV buses to PQ", re.IGNORECASE)),
    ("ibr_zero_q_demoted",
     re.compile(r"auto-demoted\s+(\d+)\s+zero-Q-span PV buses to PQ", re.IGNORECASE)),
    ("ibr_zero_q_kept",
     re.compile(r"detected\s+(\d+)\s+zero-Q-span PV buses .* kept as type-2",
                re.IGNORECASE)),
    ("seed_only_seeded",
     re.compile(r"seeded initial v_mag/v_ang from buses_solved for\s+(\d+)\s+buses",
                re.IGNORECASE)),
    ("converter_voltage_sanitized",
     re.compile(r"\[converter\] sanitized invalid bus voltage setpoints", re.IGNORECASE)),
    ("converter_q_swapped",
     re.compile(r"\[converter\] sanitized generator Q-limits", re.IGNORECASE)),
    ("converter_orphan_slack_demoted",
     re.compile(r"\[converter\].*orphan IDE=3 bus", re.IGNORECASE)),
    ("converter_seed_emitted",
     re.compile(r"\[converter\] emitted buses_solved warm-start seed", re.IGNORECASE)),
]


def tally_warnings_for_case(text: str, case_stem_lc: str) -> Dict[str, int]:
    """
    Tally warnings per case from a freeform stderr blob. We track raw counts
    of pattern hits anywhere in lines that mention the case stem; the importer
    typically prefixes warnings with the file name.
    """
    counts: Dict[str, int] = defaultdict(int)
    if not text:
        return dict(counts)
    for line in text.splitlines():
        low = line.lower()
        if case_stem_lc and case_stem_lc not in low:
            # The harness prints "Running X.RAW | format=raw ..." then the
            # importer warnings follow. We can't always pin warnings to a
            # specific case from a single line; fall through and count the
            # warning generically when no stem matches.
            pass
        for label, pattern in WARNING_PATTERNS:
            m = pattern.search(line)
            if not m:
                continue
            # If the pattern captured a count, use it; otherwise count 1.
            if m.groups():
                try:
                    counts[label] += int(m.group(1))
                except Exception:
                    counts[label] += 1
            else:
                counts[label] += 1
    return dict(counts)


def tally_global_warnings(text: str) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    if not text:
        return dict(counts)
    for line in text.splitlines():
        for label, pattern in WARNING_PATTERNS:
            m = pattern.search(line)
            if not m:
                continue
            if m.groups():
                try:
                    counts[label] += int(m.group(1))
                except Exception:
                    counts[label] += 1
            else:
                counts[label] += 1
    return dict(counts)


# ---------------------------------------------------------------------------
# Pairing + delta computation
# ---------------------------------------------------------------------------

def latest_runs_by_key(rows: Iterable[RunMetric]) -> Dict[Tuple[str, str, str, str], RunMetric]:
    """For each (case, fmt, mode, phase) keep the latest row by timestamp."""
    out: Dict[Tuple[str, str, str, str], RunMetric] = {}
    for r in rows:
        key = (r.case, r.fmt, r.mode, r.solve_phase)
        prev = out.get(key)
        if prev is None or r.timestamp >= prev.timestamp:
            out[key] = r
    return out


def fmt_tol(value: float) -> str:
    if value is None or value != value or value == float("inf"):
        return "    inf"
    return f"{value:.2e}"


def fmt_bool(b: bool) -> str:
    return "Y" if b else "N"


def render_table(latest: Dict[Tuple[str, str, str, str], RunMetric]) -> str:
    """
    Build a fixed-width Markdown table grouped by case, listing RAW vs RPF
    metrics side-by-side per (mode, phase). Cases with both RAW and RPF entries
    are shown first, then RAW-only or RPF-only cases.
    """
    cases: Dict[str, Dict[Tuple[str, str], Dict[str, RunMetric]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for (case, fmt, mode, phase), row in latest.items():
        cases[case][(mode, phase)][fmt] = row

    case_keys = sorted(cases.keys())

    lines: List[str] = []
    lines.append("| Case | Mode | Phase | RAW conv | RPF conv | Δ iter | Δ qsw | Δ qv | RAW tol | RPF tol |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")

    has_diff = False
    for case in case_keys:
        for (mode, phase), per_fmt in sorted(cases[case].items()):
            raw = per_fmt.get("raw")
            rpf = per_fmt.get("rpf")
            raw_conv = fmt_bool(raw.converged) if raw else "-"
            rpf_conv = fmt_bool(rpf.converged) if rpf else "-"
            d_iter = (
                f"{(rpf.iterations - raw.iterations):+d}"
                if raw and rpf
                else "-"
            )
            d_qsw = (
                f"{(rpf.q_switches - raw.q_switches):+d}"
                if raw and rpf
                else "-"
            )
            d_qv = (
                f"{(rpf.q_violations - raw.q_violations):+d}"
                if raw and rpf
                else "-"
            )
            raw_tol = fmt_tol(raw.tolerance) if raw else "    -   "
            rpf_tol = fmt_tol(rpf.tolerance) if rpf else "    -   "
            lines.append(
                f"| {case} | {mode} | {phase} | {raw_conv} | {rpf_conv} | "
                f"{d_iter} | {d_qsw} | {d_qv} | {raw_tol} | {rpf_tol} |"
            )
            if raw and rpf and raw.converged != rpf.converged:
                has_diff = True
    if not lines:
        lines.append("(no rows)")
    if has_diff:
        lines.append("")
        lines.append("**Convergence asymmetry detected** — see rows where RAW conv != RPF conv.")
    return "\n".join(lines)


def render_warnings(global_counts: Dict[str, int]) -> str:
    if not global_counts:
        return "(no importer / converter warnings detected)\n"
    lines = ["| Warning class | Count |", "| --- | ---: |"]
    label_order = [p[0] for p in WARNING_PATTERNS]
    for label in label_order:
        count = global_counts.get(label, 0)
        if count > 0:
            lines.append(f"| {label} | {count} |")
    extra = [
        f"| {label} | {count} |"
        for label, count in sorted(global_counts.items())
        if label not in label_order and count > 0
    ]
    lines.extend(extra)
    return "\n".join(lines) + "\n"


def render_summary(latest: Dict[Tuple[str, str, str, str], RunMetric]) -> str:
    rpf_runs = [r for r in latest.values() if r.fmt == "rpf"]
    raw_runs = [r for r in latest.values() if r.fmt == "raw"]
    rpf_converged = sum(1 for r in rpf_runs if r.converged)
    raw_converged = sum(1 for r in raw_runs if r.converged)
    paired_keys = {
        (r.case, r.mode, r.solve_phase) for r in rpf_runs
    } & {(r.case, r.mode, r.solve_phase) for r in raw_runs}
    asymm = 0
    for key in paired_keys:
        case, mode, phase = key
        raw = latest.get((case, "raw", mode, phase))
        rpf = latest.get((case, "rpf", mode, phase))
        if raw and rpf and raw.converged != rpf.converged:
            asymm += 1
    return (
        f"- RAW runs: {len(raw_runs)} (converged: {raw_converged})\n"
        f"- RPF runs: {len(rpf_runs)} (converged: {rpf_converged})\n"
        f"- Paired cases: {len(paired_keys)}\n"
        f"- Convergence asymmetries (RAW != RPF): {asymm}\n"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="RAW vs RPF delta-report formatter")
    p.add_argument("--regression-log", required=True, type=Path)
    p.add_argument("--converter-stderr", required=True, type=Path)
    p.add_argument("--harness-stderr", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--label", default="", help="Optional label tag for header")
    args = p.parse_args()

    rows = load_regression_log(args.regression_log)
    latest = latest_runs_by_key(rows)

    converter_stderr = (
        args.converter_stderr.read_text(encoding="utf-8", errors="ignore")
        if args.converter_stderr.exists() else ""
    )
    harness_stderr = (
        args.harness_stderr.read_text(encoding="utf-8", errors="ignore")
        if args.harness_stderr.exists() else ""
    )
    combined_stderr = converter_stderr + "\n" + harness_stderr
    global_counts = tally_global_warnings(combined_stderr)

    table_md = render_table(latest)
    warnings_md = render_warnings(global_counts)
    summary_md = render_summary(latest)

    label_line = f" — {args.label}" if args.label else ""
    out_md = []
    out_md.append(f"# RAW vs RPF delta report{label_line}\n")
    out_md.append("## Summary\n")
    out_md.append(summary_md)
    out_md.append("\n## Per-case deltas\n")
    out_md.append(table_md + "\n")
    out_md.append("\n## Importer / converter warnings\n")
    out_md.append(warnings_md)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("".join(out_md), encoding="utf-8")

    print(summary_md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
