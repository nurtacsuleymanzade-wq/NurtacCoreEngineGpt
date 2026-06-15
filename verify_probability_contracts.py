"""Verify the Layer-10 probability contract registry."""

import json
from pathlib import Path
from typing import Any

import probability_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "probability_contract_verification_report.json"
EXPECTED_NAMES = {
    "setup_probability_candidate",
    "event_probability_candidate",
    "pattern_probability_candidate",
    "timeframe_probability_candidate",
    "side_probability_candidate",
    "contradiction_probability_candidate",
    "insufficient_data_probability_candidate",
    "probability_context_annotation",
}
REQUIRED_FIELDS = {
    "probability_name", "probability_family", "input_sources", "required_inputs",
    "optional_inputs", "probability_logic", "learned_from", "allowed_timeframes",
    "calibration_dependency", "output_schema", "validation_invariants",
    "forbidden_behavior",
}
OUTPUT_FIELDS = {
    "layer", "engine", "record_type", "probability_id", "probability_name",
    "probability_family", "symbol", "timeframe", "window_start_ts",
    "window_end_ts", "source_setup_id", "source_event_id", "pattern_signature",
    "side", "probability", "outcome_profile", "expected_value_context",
    "contradiction_context", "calibration_refs", "setup_refs", "evidence_refs",
    "structure_refs", "volume_profile_refs", "context_refs", "decision_readiness",
    "validation",
}
FORBIDDEN = set(probability_contracts.FORBIDDEN_BEHAVIOR)
PROHIBITED_FIELDS = {
    "trade_decision", "order", "execution", "entry", "stop_loss", "take_profit",
    "leverage", "position_size", "position_sizing", "hardcoded_probability",
    "hardcoded_confidence", "hardcoded_threshold", "manual_probability",
    "heuristic_probability",
}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(probability_contracts, "PROBABILITY_CONTRACTS", None)
    if not isinstance(contracts, list):
        contracts = []
        errors.append("PROBABILITY_CONTRACTS is missing or is not a list")

    names = [item.get("probability_name") for item in contracts if isinstance(item, dict)]
    if len(contracts) != 8:
        errors.append(f"expected 8 contracts, found {len(contracts)}")
    if len(names) != len(set(names)):
        errors.append("probability_name values are not unique")
    missing_names = sorted(EXPECTED_NAMES - set(names))
    if missing_names:
        errors.append("missing contracts: " + ", ".join(missing_names))

    expected_timeframes = set(probability_contracts.ALLOWED_TIMEFRAMES)
    for contract in contracts:
        if not isinstance(contract, dict):
            errors.append("contract is not a dictionary")
            continue
        name = str(contract.get("probability_name") or "")
        if not name:
            errors.append("probability_name must not be empty")
        if not contract.get("probability_family"):
            errors.append(f"{name}: probability_family must not be empty")
        missing = sorted(REQUIRED_FIELDS - contract.keys())
        if missing:
            errors.append(f"{name}: missing fields: {', '.join(missing)}")
        for field in ("input_sources", "required_inputs", "optional_inputs", "allowed_timeframes"):
            if not isinstance(contract.get(field), list):
                errors.append(f"{name}: {field} must be a list")
        learned_from = contract.get("learned_from")
        learned_values = set(learned_from) if isinstance(learned_from, list) else {str(learned_from)}
        if not learned_values & {"calibration_profiles", "historical_outcomes"}:
            errors.append(f"{name}: learned_from must depend on measured sources")
        if not contract.get("calibration_dependency"):
            errors.append(f"{name}: calibration_dependency must not be empty")
        missing_timeframes = sorted(expected_timeframes - set(contract.get("allowed_timeframes", [])))
        if missing_timeframes:
            errors.append(f"{name}: missing timeframes: {', '.join(missing_timeframes)}")
        schema = contract.get("output_schema") if isinstance(contract.get("output_schema"), dict) else {}
        missing_output = sorted(OUTPUT_FIELDS - schema.keys())
        if missing_output:
            errors.append(f"{name}: output_schema missing: {', '.join(missing_output)}")
        probability = schema.get("probability") if isinstance(schema.get("probability"), dict) else {}
        if probability.get("source") != "calibration_profiles":
            errors.append(f"{name}: probability source must be calibration_profiles")
        if probability.get("method") != "measured_from_historical_outcomes":
            errors.append(f"{name}: probability method must be measured from outcomes")
        if probability.get("hardcoded") is not False:
            errors.append(f"{name}: probability.hardcoded must be false")
        readiness = schema.get("decision_readiness") if isinstance(schema.get("decision_readiness"), dict) else {}
        if readiness.get("ready_for_decision_gate") is not False:
            errors.append(f"{name}: ready_for_decision_gate must be false")
        forbidden = contract.get("forbidden_behavior")
        if not isinstance(forbidden, list):
            errors.append(f"{name}: forbidden_behavior must be a list")
        else:
            missing_forbidden = sorted(FORBIDDEN - {str(value).lower() for value in forbidden})
            if missing_forbidden:
                errors.append(f"{name}: forbidden_behavior missing: {', '.join(missing_forbidden)}")
        prohibited = sorted(PROHIBITED_FIELDS & contract.keys())
        if prohibited:
            errors.append(f"{name}: prohibited fields: {', '.join(prohibited)}")

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
    print("PROBABILITY CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/probability_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
