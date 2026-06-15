"""Verify the Layer-11 Decision Gate contract registry."""

import json
from pathlib import Path
from typing import Any

import decision_gate_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "decision_gate_contract_verification_report.json"
EXPECTED_NAMES = {
    "allow_paper_trade_candidate",
    "reject_trade_candidate",
    "wait_for_confirmation_candidate",
    "manual_review_candidate",
    "insufficient_data_reject_candidate",
    "contradiction_reject_candidate",
    "data_quality_reject_candidate",
    "risk_reward_review_candidate",
    "execution_plan_required_candidate",
}
REQUIRED_FIELDS = {
    "decision_name",
    "decision_family",
    "input_sources",
    "required_inputs",
    "optional_inputs",
    "decision_logic",
    "allowed_timeframes",
    "calibration_dependency",
    "probability_dependency",
    "output_schema",
    "validation_invariants",
    "forbidden_behavior",
}
OUTPUT_FIELDS = {
    "layer",
    "engine",
    "record_type",
    "decision_id",
    "decision_name",
    "decision_family",
    "symbol",
    "timeframe",
    "window_start_ts",
    "window_end_ts",
    "side",
    "decision",
    "reason",
    "probability_refs",
    "setup_refs",
    "execution_plan_refs",
    "evidence_refs",
    "structure_refs",
    "volume_profile_refs",
    "context_refs",
    "calibration_refs",
    "gate_checks",
    "scores",
    "order_readiness",
    "paper_trade_readiness",
    "validation",
}
REQUIRED_GATE_CHECKS = {
    "probability_available",
    "setup_available",
    "execution_plan_available",
    "historical_sample_available",
    "data_quality_ok",
    "contradiction_detected",
    "risk_reward_review_required",
}
REQUIRED_SCORES = {"confidence", "strength_score", "decision_score", "threshold"}
FORBIDDEN = set(decision_gate_contracts.FORBIDDEN_BEHAVIOR)
PROHIBITED_CONTRACT_FIELDS = {
    "live_execution",
    "real_order",
    "market_order",
    "limit_order",
    "leverage",
    "position_size",
    "position_sizing",
    "hardcoded_confidence",
    "hardcoded_strength_score",
    "hardcoded_threshold",
    "manual_probability",
    "heuristic_probability",
}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(decision_gate_contracts, "DECISION_GATE_CONTRACTS", None)
    if not isinstance(contracts, list):
        contracts = []
        errors.append("DECISION_GATE_CONTRACTS is missing or is not a list")

    names = [item.get("decision_name") for item in contracts if isinstance(item, dict)]
    if len(contracts) != 9:
        errors.append(f"expected 9 contracts, found {len(contracts)}")
    if len(names) != len(set(names)):
        errors.append("decision_name values are not unique")
    missing_names = sorted(EXPECTED_NAMES - set(names))
    if missing_names:
        errors.append("missing contracts: " + ", ".join(missing_names))

    expected_timeframes = set(decision_gate_contracts.ALLOWED_TIMEFRAMES)
    for contract in contracts:
        if not isinstance(contract, dict):
            errors.append("contract is not a dictionary")
            continue
        name = str(contract.get("decision_name") or "")
        if not name:
            errors.append("decision_name must not be empty")
        if not contract.get("decision_family"):
            errors.append(f"{name}: decision_family must not be empty")
        missing = sorted(REQUIRED_FIELDS - contract.keys())
        if missing:
            errors.append(f"{name}: missing fields: {', '.join(missing)}")
        for field in ("input_sources", "required_inputs", "optional_inputs", "allowed_timeframes"):
            if not isinstance(contract.get(field), list):
                errors.append(f"{name}: {field} must be a list")
        if not contract.get("decision_logic"):
            errors.append(f"{name}: decision_logic must not be empty")
        if not contract.get("calibration_dependency"):
            errors.append(f"{name}: calibration_dependency must not be empty")
        if not contract.get("probability_dependency"):
            errors.append(f"{name}: probability_dependency must not be empty")
        missing_timeframes = sorted(expected_timeframes - set(contract.get("allowed_timeframes", [])))
        if missing_timeframes:
            errors.append(f"{name}: missing timeframes: {', '.join(missing_timeframes)}")

        schema = contract.get("output_schema") if isinstance(contract.get("output_schema"), dict) else {}
        missing_output = sorted(OUTPUT_FIELDS - schema.keys())
        if missing_output:
            errors.append(f"{name}: output_schema missing: {', '.join(missing_output)}")
        gate_checks = schema.get("gate_checks") if isinstance(schema.get("gate_checks"), dict) else {}
        missing_checks = sorted(REQUIRED_GATE_CHECKS - gate_checks.keys())
        if missing_checks:
            errors.append(f"{name}: gate_checks missing: {', '.join(missing_checks)}")
        scores = schema.get("scores") if isinstance(schema.get("scores"), dict) else {}
        missing_scores = sorted(REQUIRED_SCORES - scores.keys())
        if missing_scores:
            errors.append(f"{name}: scores missing: {', '.join(missing_scores)}")
        for score_name in REQUIRED_SCORES:
            if scores.get(score_name) is not None:
                errors.append(f"{name}: scores.{score_name} must be null")
        readiness = schema.get("order_readiness") if isinstance(schema.get("order_readiness"), dict) else {}
        if readiness.get("ready_for_order") is not False:
            errors.append(f"{name}: order_readiness.ready_for_order must be false")

        forbidden = contract.get("forbidden_behavior")
        if not isinstance(forbidden, list):
            errors.append(f"{name}: forbidden_behavior must be a list")
        else:
            missing_forbidden = sorted(FORBIDDEN - {str(value).lower() for value in forbidden})
            if missing_forbidden:
                errors.append(f"{name}: forbidden_behavior missing: {', '.join(missing_forbidden)}")
        prohibited = sorted(PROHIBITED_CONTRACT_FIELDS & contract.keys())
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
    print("DECISION GATE CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/decision_gate_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
