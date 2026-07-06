#!/usr/bin/env python3
"""Config-driven Master Orchestrator (ADR-007 D7.3). Reads pipeline/orchestrate_config.yml
(dependency graph + per-source cadence), runs each stage only after every stage in its
`depends_on` has succeeded, and writes a (stage, status, error, timestamp) run-status row
into the SAME control-plane store `pipeline/common/watermark.py` already uses for
watermarks — `pipeline/gold/mart_pipeline_health.py` (BQ-10) reads this alongside its
existing row-count reconciliation.

This is NOT Airflow (D-10 forbids a private scheduler in this repo) and does not compete
with the control-plane contract this repo exposes for the separate
`airflow_dag_running_pipeline` project to adopt later (journey/07_PIPELINE_SPEC.md
"Orchestration") — it is the local dev-loop sequencer, dependency-aware instead of the
existing flat Makefile target list, same spirit either way.

Every stage module conforms to the same `def main() -> int` entrypoint (0 = success) —
extractors, promotion_gate, the 5 Silver domain pipelines, and every Gold builder all
already follow this convention, so the orchestrator invokes them uniformly, never branching
on stage type.

Not executed against a live pipeline this session (no Spark/cloud connection here, per
owner instruction) — written and py_compile-checked; live-run verification is pending the
dedicated Codespace (BUILD_REPORT.md).

Run:  python pipeline/orchestrate.py [--config pipeline/orchestrate_config.yml]
                                      [--only STAGE [STAGE ...]]
"""

from __future__ import annotations

import argparse
import importlib
from pathlib import Path

import yaml

from pipeline.common.watermark import write_run_status

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = REPO_ROOT / "pipeline" / "orchestrate_config.yml"


def load_stages(config_path: Path) -> list[dict]:
    with config_path.open(encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["stages"]


def topological_order(stages: list[dict]) -> list[dict]:
    """Kahn's algorithm. Raises on a cycle — that is a `orchestrate_config.yml` authoring
    bug, not a runtime condition to paper over."""
    by_name = {s["name"]: s for s in stages}
    remaining_deps = {s["name"]: list(s["depends_on"]) for s in stages}
    dependents: dict[str, list[str]] = {s["name"]: [] for s in stages}
    for s in stages:
        for dep in s["depends_on"]:
            if dep not in by_name:
                raise ValueError(f"stage '{s['name']}' depends_on unknown stage '{dep}'")
            dependents[dep].append(s["name"])

    ready = [name for name, deps in remaining_deps.items() if not deps]
    ordered: list[dict] = []
    while ready:
        name = ready.pop(0)
        ordered.append(by_name[name])
        for dependent in dependents[name]:
            remaining_deps[dependent].remove(name)
            if not remaining_deps[dependent]:
                ready.append(dependent)

    if len(ordered) != len(stages):
        stuck = sorted(set(by_name) - {s["name"] for s in ordered})
        raise ValueError(f"orchestrate_config.yml has a dependency cycle involving: {stuck}")
    return ordered


def _closure(names: list[str], stages: list[dict]) -> set[str]:
    """--only STAGE expands to STAGE plus everything it transitively depends on, so a
    targeted run still respects the DAG instead of assuming its upstreams already ran."""
    deps_by_name = {s["name"]: s["depends_on"] for s in stages}
    wanted: set[str] = set()
    stack = list(names)
    while stack:
        name = stack.pop()
        if name in wanted:
            continue
        wanted.add(name)
        stack.extend(deps_by_name.get(name, []))
    return wanted


def run_stage(stage: dict) -> bool:
    module = importlib.import_module(stage["module"])
    try:
        exit_code = module.main()
        ok = exit_code == 0
        write_run_status(stage["name"], "success" if ok else "failed",
                          error=None if ok else f"main() returned exit code {exit_code}")
        return ok
    except Exception as e:  # noqa: BLE001 — a stage's own exception must land in run-status, not crash the orchestrator run for every OTHER independent stage
        write_run_status(stage["name"], "failed", error=str(e))
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ap.add_argument("--only", nargs="+", default=None,
                     help="run only these stage(s) plus their transitive dependencies")
    args = ap.parse_args()

    stages = load_stages(args.config)
    ordered = topological_order(stages)
    if args.only:
        wanted = _closure(args.only, stages)
        ordered = [s for s in ordered if s["name"] in wanted]

    failed: set[str] = set()
    for stage in ordered:
        blocking = [dep for dep in stage["depends_on"] if dep in failed]
        if blocking:
            write_run_status(stage["name"], "skipped", error=f"upstream failed: {blocking}")
            failed.add(stage["name"])
            print(f"SKIPPED  {stage['name']} (upstream failed: {blocking})")
            continue
        ok = run_stage(stage)
        print(f"{'OK      ' if ok else 'FAILED  '} {stage['name']}")
        if not ok:
            failed.add(stage["name"])

    if failed:
        print(f"\norchestrate.py: {len(failed)} stage(s) failed or were skipped: {sorted(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
