"""Verify Layer-6C volume-profile engine outputs."""

import json
from collections import deque
from pathlib import Path
from typing import Any

from volume_profile_contracts import get_volume_profile_contract


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
DNA_FILE = DATA_DIR / "volume_profile_dna.jsonl"
EVENTS_FILE = DATA_DIR / "volume_profile_events.jsonl"
MEMORY_FILE = DATA_DIR / "volume_memory_zones.json"
HEALTH_FILE = DATA_DIR / "volume_profile_health.json"
REPORT_FILE = DATA_DIR / "volume_profile_engine_verification_report.json"


def read_jsonl(path: Path):
    if not path.exists(): return
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, 1):
            try: row = json.loads(line)
            except json.JSONDecodeError: yield line_number, None; continue
            yield line_number, row if isinstance(row, dict) else None


def verify() -> dict[str, Any]:
    errors: list[str] = []
    snapshots: list[dict[str, Any]] = []
    last_events: deque[tuple[int, dict[str, Any] | None]] = deque(maxlen=100)
    if not DNA_FILE.exists(): errors.append("volume_profile_dna.jsonl is missing")
    else:
        for line_number, row in read_jsonl(DNA_FILE):
            if row is None: errors.append(f"snapshot line {line_number}: invalid JSON object")
            else: snapshots.append(row)
    if EVENTS_FILE.exists():
        for item in read_jsonl(EVENTS_FILE): last_events.append(item)
    if not MEMORY_FILE.exists(): errors.append("volume_memory_zones.json is missing")
    if not HEALTH_FILE.exists(): errors.append("volume_profile_health.json is missing")

    for index, row in enumerate(snapshots, 1):
        prefix = f"snapshot {index}"
        if row.get("calibration_status") != "uncalibrated": errors.append(f"{prefix}: invalid calibration_status")
        scores = row.get("scores") if isinstance(row.get("scores"), dict) else {}
        for field in ("confidence", "strength_score", "profile_score", "threshold"):
            if scores.get(field) is not None: errors.append(f"{prefix}: scores.{field} must be null")
        quality = row.get("data_quality") if isinstance(row.get("data_quality"), dict) else {}
        if quality.get("volume_allocation") not in {"footprint_levels", "close_price_fallback"}:
            errors.append(f"{prefix}: data_quality.volume_allocation is missing or invalid")

    checked_events = 0
    for line_number, row in last_events:
        if row is None:
            errors.append(f"event line {line_number}: invalid JSON object"); continue
        checked_events += 1
        prefix = f"event line {line_number}"
        if get_volume_profile_contract(str(row.get("event_type"))) is None:
            errors.append(f"{prefix}: event_type not found in registry")
        if row.get("calibration_status") != "uncalibrated": errors.append(f"{prefix}: invalid calibration_status")
        for field in ("confidence", "strength_score", "thresholds"):
            if row.get(field) is not None: errors.append(f"{prefix}: {field} must be null")
        validation = row.get("validation") if isinstance(row.get("validation"), dict) else {}
        if validation.get("contract_found") is not True: errors.append(f"{prefix}: contract_found must be true")

    memory_count = 0
    if MEMORY_FILE.exists():
        try: memory = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError: memory = None; errors.append("volume_memory_zones.json is invalid JSON")
        if not isinstance(memory, list): errors.append("volume_memory_zones.json must contain a list")
        else:
            memory_count = len(memory)
            for index, zone in enumerate(memory, 1):
                if not isinstance(zone, dict): errors.append(f"memory zone {index}: invalid object"); continue
                if zone.get("calibration_status") != "uncalibrated": errors.append(f"memory zone {index}: invalid calibration_status")
                scores = zone.get("scores") if isinstance(zone.get("scores"), dict) else {}
                for field in ("confidence", "strength_score", "memory_score", "threshold"):
                    if scores.get(field) is not None: errors.append(f"memory zone {index}: scores.{field} must be null")

    if HEALTH_FILE.exists():
        try: health = json.loads(HEALTH_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError: health = {}; errors.append("volume_profile_health.json is invalid JSON")
        if health.get("registry_validation_passed") is not True:
            errors.append("health.registry_validation_passed must be true")

    report = {"checked_snapshots": len(snapshots), "checked_events": checked_events,
              "memory_zones": memory_count, "failed": len(errors), "errors": errors,
              "test_passed": not errors}
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("VOLUME PROFILE ENGINE VERIFICATION COMPLETE")
    print(f"checked_snapshots={report['checked_snapshots']}")
    print(f"checked_events={report['checked_events']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/volume_profile_engine_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
