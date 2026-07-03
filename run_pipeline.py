#!/usr/bin/env python3
"""Execute the Consums processing pipeline according to consums_config tasks."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "consums_config.json"

# task_name -> list of (human label, relative script path)
TASK_SCRIPT_MAP: Dict[str, Sequence[Tuple[str, str]]] = {
    "fetch_api_data": (
        ("Download minute data", "adquisicion/download_minute_data.py"),
    ),
    "save_to_csv": (
        ("Combine & save minute dataset", "adquisicion/run_compute_for_minutes.py"),
    ),
    "compute_consumption": (
        (
            "Compute minute consumption & anomalies",
            "procesado/run_compute_consumption.py",
        ),
        ("Aggregate hourly consumption", "procesado/run_hourly_aggregation.py"),
    ),
    "push_to_pg_datalake": (
        ("Persist hourly consumption", "persistencia/run_save_hourly.py"),
    ),
    "save_to_sqlserver": (
        ("Save daily summary to SQL Server", "persistencia/run_save_to_sqlserver.py"),
    ),
    "update_from_old": (
        ("Update specific counters from Consums_dia_old", "persistencia/run_update_from_old.py"),
    ),
}


def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def available_scripts(entries: Sequence[Tuple[str, str]]):
    for label, rel_path in entries:
        script_path = ROOT / rel_path
        if not script_path.exists():
            raise FileNotFoundError(
                f"Script not found for step '{label}': {script_path}"
            )
        yield label, script_path


def run_step(label: str, script_path: Path, dry_run: bool = False) -> None:
    cmd = [sys.executable, str(script_path)]
    print(f"\n>>> {label}\n    CMD: {' '.join(cmd)}")
    if dry_run:
        print("    (dry-run) Skipping execution")
        return

    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"Step '{label}' failed with exit code {result.returncode}")


def execute_tasks(
    tasks: Iterable[dict], *, continue_on_error: bool, dry_run: bool
) -> None:
    for task in tasks:
        name = task.get("name")
        enabled = task.get("enabled", False)
        if not name:
            print("Skipping unnamed task entry", task)
            continue

        scripts = TASK_SCRIPT_MAP.get(name)
        if not scripts:
            print(f"[WARN] No script mapping for task '{name}', skipping")
            continue

        if not enabled:
            print(f"[SKIP] Task '{name}' disabled in config")
            continue

        print(f"\n=== Executing task '{name}' ===")
        for label, script_path in available_scripts(scripts):
            try:
                run_step(label, script_path, dry_run=dry_run)
            except Exception as exc:  # noqa: BLE001
                print(f"[ERROR] {exc}")
                if not continue_on_error:
                    raise
                print("[WARN] continue_on_error=True -> proceeding with next step")


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consums pipeline orchestrator")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to consums_config.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands that would run without executing them",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep executing remaining steps even if one fails",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    config_path = args.config
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        return 1

    cfg = load_config(config_path)
    tasks = cfg.get("tasks", [])
    if not tasks:
        print("No tasks configured. Nothing to do.")
        return 0

    try:
        execute_tasks(
            tasks, continue_on_error=args.continue_on_error, dry_run=args.dry_run
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Pipeline stopped: {exc}")
        return 2

    print("\nPipeline completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
