"""Verify Smart Money contract registry."""

import json
from pathlib import Path
from typing import Any

import smart_money_contracts as registry


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "smart_money_contract_verification_report.json"
EXPECTED = {"fractal_high_candidate", "fractal_low_candidate", "HH_candidate", "HL_candidate", "LH_candidate", "LL_candidate", "BOS_candidate", "CHoCH_candidate", "MSB_candidate", "order_block_candidate", "breaker_block_candidate", "imbalance_candidate", "mitigation_candidate", "equal_high_candidate", "equal_low_candidate"}
REQUIRED = {"concept_name", "concept_family", "input_sources", "required_fields", "optional_fields", "measurement_formula", "calibration_status", "confidence", "strength_score", "thresholds", "output_schema", "validation_invariants", "forbidden_behavior"}
OUTPUT_REQUIRED = {"layer", "engine", "record_type", "event_id", "symbol", "timeframe", "window_start_ts", "window_end_ts", "event_type", "side", "direction", "calibration_status", "confidence", "strength_score", "thresholds", "measurements", "reason", "source_refs", "data_quality", "validation"}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(registry, "SMART_MONEY_CONTRACTS", None)
    if not isinstance(contracts, list):
        errors.append("SMART_MONEY_CONTRACTS must be a list")
        contracts = []
    names = [c.get("concept_name") for c in contracts if isinstance(c, dict)]
    if len(contracts) != 15:
        errors.append(f"expected 15 contracts, found {len(contracts)}")
    if len(names) != len(set(names)):
        errors.append("concept_name values must be unique")
    missing_names = sorted(EXPECTED - set(names))
    if missing_names:
        errors.append("missing concepts: " + ", ".join(missing_names))
    passed = 0
    for contract in contracts:
        name = str(contract.get("concept_name"))
        current: list[str] = []
        missing = sorted(REQUIRED - contract.keys())
        if missing:
            current.append("missing fields: " + ", ".join(missing))
        if contract.get("calibration_status") != "uncalibrated": current.append("calibration_status must be uncalibrated")
        if contract.get("confidence") is not None: current.append("confidence must be None")
        if contract.get("strength_score") is not None: current.append("strength_score must be None")
        if contract.get("thresholds") is not None: current.append("thresholds must be None")
        schema = contract.get("output_schema", {})
        if not isinstance(schema, dict) or not OUTPUT_REQUIRED.issubset(schema): current.append("output_schema is incomplete")
        forbidden = " ".join(str(x).lower() for x in contract.get("forbidden_behavior", []))
        for semantic in ("confidence", "strength score", "threshold", "trade decision"):
            if semantic not in forbidden: current.append(f"forbidden behavior missing {semantic}")
        if current:
            errors.extend(f"{name}: {error}" for error in current)
        else:
            passed += 1
    report = {"checked_contracts": len(contracts), "passed": passed, "failed": len(contracts) - passed, "errors": errors, "test_passed": len(contracts) == 15 and passed == 15 and not errors}
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("SMART MONEY CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/smart_money_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
