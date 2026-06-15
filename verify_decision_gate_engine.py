"""Verify Layer-11 Decision Gate candidate events."""

import json
from collections import deque
from pathlib import Path
from typing import Any

from decision_gate_contracts import get_decision_gate_contract


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
EVENTS_FILE = DATA_DIR / "decision_gate_events.jsonl"
HEALTH_FILE = DATA_DIR / "decision_gate_health.json"
REPORT_FILE = DATA_DIR / "decision_gate_engine_verification_report.json"
ALLOWED_DECISIONS = {"allow_paper_trade", "reject", "wait", "manual_review", "execution_plan_required"}
SCORE_FIELDS = {"confidence", "strength_score", "decision_score", "threshold"}
PROHIBITED_FIELDS = {
    "order", "market_order", "limit_order", "entry_execution", "sl_execution",
    "tp_execution", "leverage", "position_size", "position_sizing",
}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    rows: deque[tuple[int, dict[str, Any]]] = deque(maxlen=100)
    if not EVENTS_FILE.exists():
        errors.append("decision_gate_events.jsonl is missing")
    else:
        with EVENTS_FILE.open("r", encoding="utf-8", errors="replace") as handle:
            for number, line in enumerate(handle, 1):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append((number, row))
    if not rows:
        errors.append("no readable decision events")

    for number, row in rows:
        label = f"line {number}"
        name = str(row.get("decision_name") or "")
        if get_decision_gate_contract(name) is None:
            errors.append(f"{label}: decision_name missing from registry")
        if row.get("decision") not in ALLOWED_DECISIONS:
            errors.append(f"{label}: invalid decision")
        readiness = row.get("order_readiness") if isinstance(row.get("order_readiness"), dict) else {}
        if readiness.get("ready_for_order") is not False:
            errors.append(f"{label}: ready_for_order must be false")
        scores = row.get("scores") if isinstance(row.get("scores"), dict) else {}
        for field in SCORE_FIELDS:
            if field not in scores or scores.get(field) is not None:
                errors.append(f"{label}: scores.{field} must be null")
        paper = row.get("paper_trade_readiness") if isinstance(row.get("paper_trade_readiness"), dict) else {}
        expected_paper = name == "allow_paper_trade_candidate"
        if paper.get("ready_for_paper_trade") is not expected_paper:
            errors.append(f"{label}: invalid paper trade readiness")
        if expected_paper and row.get("decision") != "allow_paper_trade":
            errors.append(f"{label}: paper trade candidate has invalid decision")
        prohibited = sorted(find_prohibited_fields(row))
        if prohibited:
            errors.append(f"{label}: prohibited fields: {', '.join(prohibited)}")

    if not HEALTH_FILE.exists():
        errors.append("decision_gate_health.json is missing")
    else:
        try:
            health = json.loads(HEALTH_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            health = {}
            errors.append("decision_gate_health.json is invalid")
        if health.get("registry_validation_passed") is not True:
            errors.append("registry_validation_passed must be true")

    report = {
        "checked_decisions": len(rows), "failed": len(errors),
        "errors": errors, "test_passed": not errors,
    }
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def find_prohibited_fields(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key).lower()
            if key_text in PROHIBITED_FIELDS:
                found.add(key_text)
            found.update(find_prohibited_fields(child))
    elif isinstance(value, list):
        for child in value:
            found.update(find_prohibited_fields(child))
    return found


def main() -> int:
    report = verify()
    print("DECISION GATE ENGINE VERIFICATION COMPLETE")
    print(f"checked_decisions={report['checked_decisions']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/decision_gate_engine_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
