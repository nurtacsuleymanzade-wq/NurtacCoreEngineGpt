"""Verify Layer-6B calibration contracts."""

import json
from pathlib import Path
from typing import Any

import calibration_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "calibration_contract_verification_report.json"
EXPECTED = {
    "detector_event_outcome_observation", "evidence_packet_outcome_observation",
    "structure_event_outcome_observation", "observer_event_outcome_observation",
    "composite_pattern_outcome_observation", "calibration_profile_summary",
    "insufficient_sample_profile", "data_quality_outcome_annotation",
}
REQUIRED = {"contract_name", "contract_family", "input_sources", "required_fields", "optional_fields", "observation_formula", "forward_horizons", "calibration_status", "confidence", "strength_score", "thresholds", "output_schema", "validation_invariants", "forbidden_behavior"}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(calibration_contracts, "CALIBRATION_CONTRACTS", None)
    if not isinstance(contracts, list):
        contracts = []
        errors.append("CALIBRATION_CONTRACTS missing or invalid")
    names = [item.get("contract_name") for item in contracts if isinstance(item, dict)]
    if len(contracts) != 8: errors.append(f"expected 8 contracts, found {len(contracts)}")
    if len(names) != len(set(names)): errors.append("contract_name values are not unique")
    missing_names = sorted(EXPECTED - set(names))
    if missing_names: errors.append("missing contracts: " + ", ".join(missing_names))
    for item in contracts:
        if not isinstance(item, dict):
            errors.append("contract is not a dictionary"); continue
        name = str(item.get("contract_name"))
        missing = sorted(REQUIRED - item.keys())
        if missing: errors.append(f"{name}: missing fields: {', '.join(missing)}")
        if item.get("forward_horizons") != calibration_contracts.FORWARD_HORIZONS: errors.append(f"{name}: forward horizons invalid")
        if item.get("calibration_status") != "observed_not_scored": errors.append(f"{name}: calibration status invalid")
        for field in ("confidence", "strength_score", "thresholds"):
            if item.get(field) is not None: errors.append(f"{name}: {field} must be null")
        text = " ".join(str(value).lower() for value in item.get("forbidden_behavior", []))
        for phrase in ("trade decision", "no setup", "no confidence", "no strength score", "threshold"):
            if phrase not in text: errors.append(f"{name}: forbidden behavior missing {phrase}")
    report = {"checked_contracts": len(contracts), "passed": len(contracts) if not errors else 0, "failed": 0 if not errors else len(contracts), "missing_contracts": missing_names, "errors": errors, "test_passed": not errors}
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("CALIBRATION CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/calibration_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
