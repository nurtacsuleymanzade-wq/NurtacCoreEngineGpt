"""Validate the declarative Layer-5 evidence contract registry."""

import json
from pathlib import Path
from typing import Any

import evidence_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_PATH = ROOT_DIR / "data" / "evidence_contract_verification_report.json"
EXPECTED_CONTRACT_NAME = "evidence_packet_contract"

REQUIRED_CONTRACT_FIELDS = {
    "contract_name",
    "contract_layer",
    "input_sources",
    "grouping_key",
    "required_fields",
    "optional_fields",
    "packet_schema",
    "calibration_status",
    "confidence",
    "strength_score",
    "scores",
    "validation_invariants",
    "forbidden_behavior",
}

REQUIRED_PACKET_FIELDS = {
    "layer",
    "engine",
    "record_type",
    "symbol",
    "timeframe",
    "window_start_ts",
    "window_end_ts",
    "packet_version",
    "calibration_status",
    "evidence_summary",
    "evidence_events",
    "measurement_refs",
    "detector_event_refs",
    "context_refs",
    "data_quality",
    "decision_readiness",
    "scores",
    "validation",
}

REQUIRED_SUMMARY_FIELDS = {
    "total_events",
    "event_types",
    "buy_side_events",
    "sell_side_events",
    "neutral_events",
    "unknown_side_events",
}

REQUIRED_FORBIDDEN_BEHAVIOR = {
    "no trade decision",
    "no long short signal",
    "no setup",
    "no entry",
    "no stop loss",
    "no take profit",
    "no confidence score",
    "no strength score",
    "no directional score",
    "no bias score",
    "no numeric threshold",
}


def normalized_statements(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {" ".join(str(item).lower().split()) for item in value}


def verify_contract(contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    name = str(contract.get("contract_name", "unknown_contract"))
    missing_fields = sorted(REQUIRED_CONTRACT_FIELDS - contract.keys())
    if missing_fields:
        errors.append(f"{name}: missing contract fields: {', '.join(missing_fields)}")

    if contract.get("contract_name") != EXPECTED_CONTRACT_NAME:
        errors.append(f"{name}: unexpected contract_name")
    if contract.get("contract_layer") != "Layer-5":
        errors.append(f"{name}: contract_layer must be Layer-5")
    if contract.get("calibration_status") != "uncalibrated":
        errors.append(f"{name}: calibration_status must be uncalibrated")
    if contract.get("confidence") is not None:
        errors.append(f"{name}: confidence must be None")
    if contract.get("strength_score") is not None:
        errors.append(f"{name}: strength_score must be None")
    if contract.get("scores") is not None:
        errors.append(f"{name}: scores must be None")

    packet_schema = contract.get("packet_schema")
    if not isinstance(packet_schema, dict):
        errors.append(f"{name}: packet_schema must be a dict")
    else:
        missing_packet_fields = sorted(REQUIRED_PACKET_FIELDS - packet_schema.keys())
        if missing_packet_fields:
            errors.append(f"{name}: missing packet_schema fields: {', '.join(missing_packet_fields)}")

        summary_schema = packet_schema.get("evidence_summary")
        if not isinstance(summary_schema, dict):
            errors.append(f"{name}: evidence_summary schema must be a dict")
        else:
            missing_summary_fields = sorted(REQUIRED_SUMMARY_FIELDS - summary_schema.keys())
            if missing_summary_fields:
                errors.append(f"{name}: missing evidence_summary fields: {', '.join(missing_summary_fields)}")

        readiness = packet_schema.get("decision_readiness")
        expected_readiness = {
            "ready_for_decision": False,
            "reason": "uncalibrated_evidence",
        }
        if readiness != expected_readiness:
            errors.append(f"{name}: decision_readiness must remain fixed and uncalibrated")

        score_schema = packet_schema.get("scores")
        expected_scores = {
            "confidence": None,
            "strength_score": None,
            "directional_score": None,
            "bias_score": None,
        }
        if score_schema != expected_scores:
            errors.append(f"{name}: all packet score fields must be None")

    forbidden = normalized_statements(contract.get("forbidden_behavior"))
    missing_forbidden = sorted(REQUIRED_FORBIDDEN_BEHAVIOR - forbidden)
    if missing_forbidden:
        errors.append(f"{name}: missing forbidden behavior: {', '.join(missing_forbidden)}")

    invariants = " ".join(normalized_statements(contract.get("validation_invariants")))
    if "scores" not in invariants or "null" not in invariants:
        errors.append(f"{name}: validation invariants must require null scores")
    if "ready_for_decision" not in invariants or "false" not in invariants:
        errors.append(f"{name}: validation invariants must require ready_for_decision=false")
    return errors


def verify_registry() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(evidence_contracts, "EVIDENCE_CONTRACTS", None)
    if not isinstance(contracts, list):
        errors.append("EVIDENCE_CONTRACTS must exist and be a list")
        contracts = []

    checked_contracts = len(contracts)
    names = [contract.get("contract_name") for contract in contracts if isinstance(contract, dict)]
    if EXPECTED_CONTRACT_NAME not in names:
        errors.append("evidence_packet_contract is missing")

    passed = 0
    for index, contract in enumerate(contracts):
        if not isinstance(contract, dict):
            errors.append(f"contract[{index}] must be a dict")
            continue
        contract_errors = verify_contract(contract)
        if contract_errors:
            errors.extend(contract_errors)
        else:
            passed += 1

    failed = checked_contracts - passed
    test_passed = checked_contracts == 1 and passed == 1 and not errors
    return {
        "checked_contracts": checked_contracts,
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "test_passed": test_passed,
    }


def main() -> int:
    report = verify_registry()
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print("EVIDENCE CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/evidence_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
