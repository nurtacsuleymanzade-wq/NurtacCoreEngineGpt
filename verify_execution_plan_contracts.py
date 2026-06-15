"""Verify the Layer-9 execution-plan contract registry."""

import json
from pathlib import Path
from typing import Any

import execution_plan_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "execution_plan_contract_verification_report.json"
EXPECTED_NAMES = {
    "market_follow_plan_candidate",
    "reclaim_entry_plan_candidate",
    "pullback_retest_plan_candidate",
    "mitigation_zone_plan_candidate",
    "breakout_retest_plan_candidate",
    "sweep_reversal_plan_candidate",
    "premium_alignment_plan_candidate",
}
REQUIRED_FIELDS = {
    "plan_name", "plan_family", "input_sources", "required_inputs",
    "optional_inputs", "plan_logic", "allowed_setup_families",
    "allowed_timeframes", "entry_model", "stop_model", "target_model",
    "invalidation_model", "calibration_status", "confidence",
    "strength_score", "thresholds", "output_schema",
    "validation_invariants", "forbidden_behavior",
}
OUTPUT_FIELDS = {
    "layer", "engine", "record_type", "plan_id", "plan_name", "plan_family",
    "setup_id", "setup_name", "setup_family", "symbol", "timeframe",
    "window_start_ts", "window_end_ts", "side", "entry", "stop_loss",
    "take_profit", "invalidation", "supporting_evidence", "blocking_evidence",
    "setup_refs", "structure_refs", "context_refs", "historical_outcome_refs",
    "calibration_status", "scores", "order_readiness", "risk_readiness",
    "validation",
}
FORBIDDEN = {
    "no trade decision", "no execution", "no order", "no market order",
    "no limit order", "no entry execution", "no stop loss execution",
    "no take profit execution", "no leverage", "no position sizing",
    "no confidence", "no strength score", "no execution score",
    "no edge score", "no probability score", "no numeric threshold",
}
PROHIBITED_CONTRACT_FIELDS = {
    "order", "market_order", "limit_order", "leverage", "position_size",
    "position_sizing", "risk_amount", "execution_command",
}


def _verify_schema(name: str, schema: dict[str, Any], errors: list[str]) -> None:
    missing = sorted(OUTPUT_FIELDS - schema.keys())
    if missing:
        errors.append(f"{name}: output_schema missing: {', '.join(missing)}")
    if schema.get("layer") != "Layer-9":
        errors.append(f"{name}: output_schema.layer must be Layer-9")
    if schema.get("engine") != "ExecutionPlanEngine":
        errors.append(f"{name}: output_schema.engine must be ExecutionPlanEngine")
    if schema.get("record_type") != "execution_plan_candidate":
        errors.append(f"{name}: invalid output record_type")
    scores = schema.get("scores") if isinstance(schema.get("scores"), dict) else {}
    for score in ("confidence", "strength_score", "execution_score", "edge_score", "probability_score", "threshold"):
        if scores.get(score) is not None:
            errors.append(f"{name}: scores.{score} must be null")
    if schema.get("order_readiness", {}).get("ready_for_order") is not False:
        errors.append(f"{name}: ready_for_order must be false")
    if schema.get("risk_readiness", {}).get("ready_for_risk_engine") is not False:
        errors.append(f"{name}: ready_for_risk_engine must be false")
    entry = schema.get("entry") if isinstance(schema.get("entry"), dict) else {}
    stop = schema.get("stop_loss") if isinstance(schema.get("stop_loss"), dict) else {}
    invalidation = schema.get("invalidation") if isinstance(schema.get("invalidation"), dict) else {}
    if entry.get("entry_price_candidate") is not None or entry.get("entry_zone_candidate") is not None:
        errors.append(f"{name}: entry candidates must be null")
    if stop.get("sl_price_candidate") is not None or stop.get("sl_zone_candidate") is not None:
        errors.append(f"{name}: stop candidates must be null")
    if invalidation.get("invalidation_price_candidate") is not None:
        errors.append(f"{name}: invalidation price candidate must be null")
    if entry.get("entry_status") != "candidate_not_order":
        errors.append(f"{name}: entry_status must be candidate_not_order")
    if stop.get("sl_status") != "candidate_not_order":
        errors.append(f"{name}: sl_status must be candidate_not_order")
    if schema.get("take_profit", {}).get("tp_status") != "candidate_not_order":
        errors.append(f"{name}: tp_status must be candidate_not_order")


def verify() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(execution_plan_contracts, "EXECUTION_PLAN_CONTRACTS", None)
    if not isinstance(contracts, list):
        contracts = []
        errors.append("EXECUTION_PLAN_CONTRACTS is missing or is not a list")

    names = [item.get("plan_name") for item in contracts if isinstance(item, dict)]
    if len(contracts) != 7:
        errors.append(f"expected 7 contracts, found {len(contracts)}")
    if len(names) != len(set(names)):
        errors.append("plan_name values are not unique")
    missing_names = sorted(EXPECTED_NAMES - set(names))
    if missing_names:
        errors.append("missing contracts: " + ", ".join(missing_names))

    for contract in contracts:
        if not isinstance(contract, dict):
            errors.append("contract is not a dictionary")
            continue
        name = str(contract.get("plan_name") or "")
        if not name:
            errors.append("plan_name must not be empty")
        if not contract.get("plan_family"):
            errors.append(f"{name}: plan_family must not be empty")
        missing = sorted(REQUIRED_FIELDS - contract.keys())
        if missing:
            errors.append(f"{name}: missing fields: {', '.join(missing)}")
        for field in ("input_sources", "required_inputs", "optional_inputs", "allowed_setup_families", "allowed_timeframes"):
            if not isinstance(contract.get(field), list):
                errors.append(f"{name}: {field} must be a list")
        if not contract.get("allowed_setup_families"):
            errors.append(f"{name}: allowed_setup_families must not be empty")
        missing_timeframes = sorted(set(execution_plan_contracts.ALLOWED_TIMEFRAMES) - set(contract.get("allowed_timeframes", [])))
        if missing_timeframes:
            errors.append(f"{name}: missing timeframes: {', '.join(missing_timeframes)}")
        for field in ("entry_model", "stop_model", "target_model", "invalidation_model"):
            if not contract.get(field):
                errors.append(f"{name}: {field} must be defined")
        if contract.get("calibration_status") != "uncalibrated":
            errors.append(f"{name}: calibration_status must be uncalibrated")
        for field in ("confidence", "strength_score", "thresholds"):
            if contract.get(field) is not None:
                errors.append(f"{name}: {field} must be null")
        prohibited = sorted(PROHIBITED_CONTRACT_FIELDS & contract.keys())
        if prohibited:
            errors.append(f"{name}: prohibited generation fields: {', '.join(prohibited)}")
        forbidden = contract.get("forbidden_behavior")
        if not isinstance(forbidden, list):
            errors.append(f"{name}: forbidden_behavior must be a list")
        else:
            forbidden_text = {str(value).lower() for value in forbidden}
            missing_forbidden = sorted(FORBIDDEN - forbidden_text)
            if missing_forbidden:
                errors.append(f"{name}: forbidden_behavior missing: {', '.join(missing_forbidden)}")
        schema = contract.get("output_schema") if isinstance(contract.get("output_schema"), dict) else {}
        _verify_schema(name, schema, errors)

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
    print("EXECUTION PLAN CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/execution_plan_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
