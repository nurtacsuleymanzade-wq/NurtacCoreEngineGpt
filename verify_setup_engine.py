"""Verify Layer-8 setup candidate runtime outputs."""

import importlib
import json
from collections import deque
from pathlib import Path
from typing import Any

from setup_contracts import get_setup_contract


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
CANDIDATES_FILE = DATA_DIR / "setup_candidates.jsonl"
HEALTH_FILE = DATA_DIR / "setup_health.json"
REPORT_FILE = DATA_DIR / "setup_engine_verification_report.json"
PROHIBITED = {"entry", "entry_price", "sl", "stop_loss", "tp", "take_profit", "leverage", "position_size", "position_sizing"}


def tail_jsonl(path: Path, count: int = 100) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=count)
    if not path.exists(): return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, 1):
            try: row = json.loads(line)
            except json.JSONDecodeError: rows.append({"_invalid": line_number}); continue
            if isinstance(row, dict): rows.append(row)
    return list(rows)


def verify() -> dict[str, Any]:
    errors: list[str] = []
    try: importlib.import_module("setup_engine")
    except Exception as exc: errors.append(f"setup_engine import failed: {exc}")
    if not CANDIDATES_FILE.exists(): errors.append("setup_candidates.jsonl is missing")
    rows = tail_jsonl(CANDIDATES_FILE)
    for index, row in enumerate(rows):
        prefix = f"setup[{index}]"
        if "_invalid" in row: errors.append(f"{prefix}: invalid JSON"); continue
        if get_setup_contract(str(row.get("setup_name"))) is None: errors.append(f"{prefix}: setup_name is not registered")
        if row.get("setup_status") != "candidate_not_trade_signal": errors.append(f"{prefix}: setup_status invalid")
        scores = row.get("scores") if isinstance(row.get("scores"), dict) else {}
        for field in ("confidence", "strength_score", "setup_score", "edge_score", "probability_score", "threshold"):
            if scores.get(field) is not None: errors.append(f"{prefix}: scores.{field} must be null")
        if row.get("execution_readiness", {}).get("ready_for_execution_plan") is not False: errors.append(f"{prefix}: execution readiness must be false")
        if row.get("risk_readiness", {}).get("ready_for_position_sizing") is not False: errors.append(f"{prefix}: risk readiness must be false")
        prohibited = sorted(PROHIBITED & row.keys())
        if prohibited: errors.append(f"{prefix}: prohibited fields: {', '.join(prohibited)}")
    health: dict[str, Any] = {}
    if not HEALTH_FILE.exists(): errors.append("setup_health.json is missing")
    else:
        try: health = json.loads(HEALTH_FILE.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc: errors.append(f"setup_health.json invalid: {exc}")
        if health.get("registry_validation_passed") is not True: errors.append("registry_validation_passed must be true")
    report = {"checked_setups": len(rows), "failed": len(errors), "errors": errors, "test_passed": not errors}
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("SETUP ENGINE VERIFICATION COMPLETE")
    print(f"checked_setups={report['checked_setups']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/setup_engine_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
