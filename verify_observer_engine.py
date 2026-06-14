"""Verify Layer-7 Observer runtime outputs."""

import importlib
import json
from collections import deque
from pathlib import Path
from typing import Any

from observer_contracts import get_observer_contract


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
STATES_FILE = DATA_DIR / "observer_states.jsonl"
EVENTS_FILE = DATA_DIR / "observer_events.jsonl"
HEALTH_FILE = DATA_DIR / "observer_health.json"
REPORT_FILE = DATA_DIR / "observer_engine_verification_report.json"


def tail_jsonl(path: Path, count: int = 100) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=count)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, 1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                rows.append({"_invalid_json_line": line_number})
                continue
            if isinstance(row, dict):
                rows.append(row)
    return list(rows)


def verify() -> dict[str, Any]:
    errors: list[str] = []
    try:
        importlib.import_module("observer_engine")
    except Exception as exc:
        errors.append(f"observer_engine import failed: {exc}")

    if not STATES_FILE.exists():
        errors.append("observer_states.jsonl is missing")
    state_rows = tail_jsonl(STATES_FILE)
    event_rows = tail_jsonl(EVENTS_FILE)
    for index, state in enumerate(state_rows):
        prefix = f"state[{index}]"
        if "_invalid_json_line" in state:
            errors.append(f"{prefix}: invalid JSON")
            continue
        if state.get("calibration_status") != "uncalibrated":
            errors.append(f"{prefix}: calibration_status must be uncalibrated")
        scores = state.get("scores") if isinstance(state.get("scores"), dict) else {}
        for field in ("confidence", "strength_score", "observer_score", "threshold"):
            if scores.get(field) is not None:
                errors.append(f"{prefix}: scores.{field} must be null")
        readiness = state.get("decision_readiness") if isinstance(state.get("decision_readiness"), dict) else {}
        if readiness.get("ready_for_setup") is not False:
            errors.append(f"{prefix}: ready_for_setup must be false")

    for index, event in enumerate(event_rows):
        prefix = f"event[{index}]"
        if "_invalid_json_line" in event:
            errors.append(f"{prefix}: invalid JSON")
            continue
        if get_observer_contract(str(event.get("event_type"))) is None:
            errors.append(f"{prefix}: event_type is not registered")
        if event.get("calibration_status") != "uncalibrated":
            errors.append(f"{prefix}: calibration_status must be uncalibrated")
        for field in ("confidence", "strength_score", "thresholds"):
            if event.get(field) is not None:
                errors.append(f"{prefix}: {field} must be null")

    health: dict[str, Any] = {}
    if not HEALTH_FILE.exists():
        errors.append("observer_health.json is missing")
    else:
        try:
            health = json.loads(HEALTH_FILE.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            errors.append(f"observer_health.json is invalid: {exc}")
        if health.get("registry_validation_passed") is not True:
            errors.append("registry_validation_passed must be true")

    report = {
        "checked_states": len(state_rows),
        "checked_events": len(event_rows),
        "observer_states_exists": STATES_FILE.exists(),
        "observer_events_exists": EVENTS_FILE.exists(),
        "observer_health_exists": HEALTH_FILE.exists(),
        "registry_validation_passed": health.get("registry_validation_passed") is True,
        "failed": len(errors),
        "errors": errors,
        "test_passed": not errors,
    }
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("OBSERVER ENGINE VERIFICATION COMPLETE")
    print(f"checked_states={report['checked_states']}")
    print(f"checked_events={report['checked_events']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/observer_engine_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
