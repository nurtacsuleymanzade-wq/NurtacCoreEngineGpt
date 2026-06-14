"""Verify the Layer-8 setup contract registry."""

import json
from pathlib import Path
from typing import Any

import setup_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "setup_contract_verification_report.json"
EXPECTED_NAMES = {
    "initiative_continuation_candidate", "absorption_reversal_candidate",
    "sweep_reclaim_candidate", "breakout_continuation_candidate",
    "pullback_mitigation_candidate", "trap_reversal_candidate",
    "premium_alignment_candidate",
}
REQUIRED_FIELDS = {
    "setup_name", "setup_family", "input_sources", "required_inputs",
    "optional_inputs", "setup_logic", "allowed_timeframes", "calibration_status",
    "confidence", "strength_score", "thresholds", "output_schema",
    "validation_invariants", "forbidden_behavior",
}
OUTPUT_FIELDS = {
    "layer", "engine", "record_type", "setup_id", "setup_name", "setup_family",
    "symbol", "timeframe", "window_start_ts", "window_end_ts", "side",
    "setup_status", "supporting_evidence", "blocking_evidence", "observer_refs",
    "structure_refs", "context_refs", "historical_outcome_refs",
    "calibration_status", "scores", "execution_readiness", "risk_readiness",
    "validation",
}
FORBIDDEN = [
    "no trade decision", "no execution", "no entry", "no stop loss",
    "no take profit", "no leverage", "no position sizing", "no confidence",
    "no strength score", "no setup score", "no edge score",
    "no probability score", "no numeric threshold",
]
PROHIBITED_OUTPUT_FIELDS = {
    "entry", "entry_price", "stop_loss", "sl", "take_profit", "tp",
    "leverage", "position_size", "position_sizing",
}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(setup_contracts, "SETUP_CONTRACTS", None)
    if not isinstance(contracts, list):
        contracts = []
        errors.append("SETUP_CONTRACTS is missing or is not a list")
    names = [item.get("setup_name") for item in contracts if isinstance(item, dict)]
    if len(contracts) != 7:
        errors.append(f"expected 7 contracts, found {len(contracts)}")
    if len(names) != len(set(names)):
        errors.append("setup_name values are not unique")
    missing_names = sorted(EXPECTED_NAMES - set(names))
    if missing_names:
        errors.append("missing contracts: " + ", ".join(missing_names))

    for contract in contracts:
        if not isinstance(contract, dict):
            errors.append("contract is not a dictionary")
            continue
        name = str(contract.get("setup_name"))
        missing = sorted(REQUIRED_FIELDS - contract.keys())
        if missing:
            errors.append(f"{name}: missing fields: {', '.join(missing)}")
        for field in ("input_sources", "required_inputs", "optional_inputs", "allowed_timeframes"):
            if not isinstance(contract.get(field), list):
                errors.append(f"{name}: {field} must be a list")
        if contract.get("calibration_status") != "uncalibrated":
            errors.append(f"{name}: calibration_status must be uncalibrated")
        for field in ("confidence", "strength_score", "thresholds"):
            if contract.get(field) is not None:
                errors.append(f"{name}: {field} must be null")
        missing_timeframes = sorted(set(setup_contracts.ALLOWED_TIMEFRAMES) - set(contract.get("allowed_timeframes", [])))
        if missing_timeframes:
            errors.append(f"{name}: missing timeframes: {', '.join(missing_timeframes)}")
        schema = contract.get("output_schema") if isinstance(contract.get("output_schema"), dict) else {}
        missing_output = sorted(OUTPUT_FIELDS - schema.keys())
        if missing_output:
            errors.append(f"{name}: output_schema missing: {', '.join(missing_output)}")
        prohibited = sorted(PROHIBITED_OUTPUT_FIELDS & schema.keys())
        if prohibited:
            errors.append(f"{name}: prohibited output fields: {', '.join(prohibited)}")
        scores = schema.get("scores") if isinstance(schema.get("scores"), dict) else {}
        for score in ("confidence", "strength_score", "setup_score", "edge_score", "probability_score", "threshold"):
            if scores.get(score) is not None:
                errors.append(f"{name}: scores.{score} must be null")
        if schema.get("execution_readiness", {}).get("ready_for_execution_plan") is not False:
            errors.append(f"{name}: ready_for_execution_plan must be false")
        if schema.get("risk_readiness", {}).get("ready_for_position_sizing") is not False:
            errors.append(f"{name}: ready_for_position_sizing must be false")
        forbidden_text = " ".join(str(value).lower() for value in contract.get("forbidden_behavior", []))
        for phrase in FORBIDDEN:
            if phrase not in forbidden_text:
                errors.append(f"{name}: forbidden_behavior missing {phrase}")

    report = {
        "checked_contracts": len(contracts),
        "passed": len(contracts) if not errors else 0,
        "failed": 0 if not errors else len(contracts),
        "errors": errors,
        "test_passed": not errors,
    }
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("SETUP CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/setup_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
