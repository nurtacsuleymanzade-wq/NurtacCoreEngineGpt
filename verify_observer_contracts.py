"""Verify the Layer-7 Observer contract registry."""

import json
from pathlib import Path
from typing import Any

import observer_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "observer_contract_verification_report.json"
EXPECTED_NAMES = {
    "long_watch_candidate",
    "short_watch_candidate",
    "neutral_watch_candidate",
    "long_condition_satisfied_candidate",
    "short_condition_satisfied_candidate",
    "invalidation_candidate",
    "wait_for_trigger_candidate",
}
REQUIRED_FIELDS = {
    "observer_name",
    "observer_family",
    "input_sources",
    "required_fields",
    "optional_fields",
    "observation_logic",
    "calibration_status",
    "confidence",
    "strength_score",
    "thresholds",
    "output_schema",
    "validation_invariants",
    "forbidden_behavior",
}
FORBIDDEN_PHRASES = {
    "no trade decision",
    "no setup",
    "no confidence",
    "no strength score",
    "no threshold",
}


def semantic_text(values: Any) -> str:
    return " ".join(str(value).lower() for value in values) if isinstance(values, list) else str(values).lower()


def verify() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(observer_contracts, "OBSERVER_CONTRACTS", None)
    if not isinstance(contracts, list):
        contracts = []
        errors.append("OBSERVER_CONTRACTS is missing or is not a list")
    names = [contract.get("observer_name") for contract in contracts if isinstance(contract, dict)]
    if len(contracts) != 7:
        errors.append(f"expected 7 contracts, found {len(contracts)}")
    if len(names) != len(set(names)):
        errors.append("observer_name values are not unique")
    missing_names = sorted(EXPECTED_NAMES - set(names))
    if missing_names:
        errors.append("missing contracts: " + ", ".join(missing_names))

    for contract in contracts:
        if not isinstance(contract, dict):
            errors.append("contract is not a dictionary")
            continue
        name = str(contract.get("observer_name"))
        missing = sorted(REQUIRED_FIELDS - contract.keys())
        if missing:
            errors.append(f"{name}: missing fields: {', '.join(missing)}")
        if contract.get("calibration_status") != "uncalibrated":
            errors.append(f"{name}: calibration_status is not uncalibrated")
        for field in ("confidence", "strength_score", "thresholds"):
            if contract.get(field) is not None:
                errors.append(f"{name}: {field} must be null")
        forbidden_text = semantic_text(contract.get("forbidden_behavior", []))
        semantic_checks = {
            "no trade decision": "trade decision" in forbidden_text,
            "no setup": "no setup" in forbidden_text,
            "no confidence": "no confidence" in forbidden_text,
            "no strength score": "no strength score" in forbidden_text,
            "no threshold": "threshold" in forbidden_text,
        }
        for phrase, present in semantic_checks.items():
            if not present:
                errors.append(f"{name}: forbidden behavior lacks semantic equivalent of {phrase}")

    report = {
        "checked_contracts": len(contracts),
        "passed": len(contracts) if not errors else 0,
        "failed": 0 if not errors else len(contracts),
        "missing_contracts": missing_names,
        "errors": errors,
        "test_passed": not errors,
    }
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("OBSERVER CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/observer_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
