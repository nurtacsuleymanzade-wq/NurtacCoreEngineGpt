"""Verify Smart Money runtime outputs."""

import json
from collections import deque
from pathlib import Path
from typing import Any

from smart_money_contracts import get_smart_money_contract


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
REPORT_FILE = DATA_DIR / "smart_money_engine_verification_report.json"


def last_rows(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=limit)
    if not path.exists(): return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try: row = json.loads(line)
            except json.JSONDecodeError: continue
            if isinstance(row, dict): rows.append(row)
    return list(rows)


def verify() -> dict[str, Any]:
    errors: list[str] = []
    dna = DATA_DIR / "smart_money_dna.jsonl"
    events = DATA_DIR / "structure_events.jsonl"
    health_path = DATA_DIR / "smart_money_health.json"
    if not dna.exists(): errors.append("smart_money_dna.jsonl is missing")
    checked_events = 0
    for index, event in enumerate(last_rows(events, 100)):
        checked_events += 1
        label = f"event[{index}]"
        if get_smart_money_contract(str(event.get("event_type"))) is None: errors.append(f"{label}: contract not found")
        if event.get("confidence") is not None: errors.append(f"{label}: confidence must be null")
        if event.get("strength_score") is not None: errors.append(f"{label}: strength_score must be null")
        if event.get("thresholds") is not None: errors.append(f"{label}: thresholds must be null")
        if event.get("calibration_status") != "uncalibrated": errors.append(f"{label}: calibration_status invalid")
    health = None
    if not health_path.exists(): errors.append("smart_money_health.json is missing")
    else:
        try: health = json.loads(health_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError: errors.append("smart_money_health.json is invalid")
        if isinstance(health, dict) and health.get("registry_validation_passed") is not True: errors.append("registry_validation_passed must be true")
    report = {"smart_money_dna_exists": dna.exists(), "checked_events": checked_events, "health_exists": health_path.exists(), "failed": len(errors), "errors": errors, "test_passed": not errors}
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("SMART MONEY ENGINE VERIFICATION COMPLETE")
    print(f"checked_events={report['checked_events']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/smart_money_engine_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
