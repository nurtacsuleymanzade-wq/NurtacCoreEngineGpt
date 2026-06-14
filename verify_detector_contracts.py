"""Validate the declarative Layer-4 detector contract registry."""

import json
from pathlib import Path
from typing import Any

import detector_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_PATH = ROOT_DIR / "data" / "detector_contract_verification_report.json"

EXPECTED_DETECTORS = {
    "absorption_candidate",
    "sweep_candidate",
    "exhaustion_candidate",
    "iceberg_candidate",
    "trapped_trader_candidate",
    "initiative_flow_candidate",
    "delta_imbalance_candidate",
    "momentum_candidate",
    "aggression_burst_candidate",
    "responsive_buyer_candidate",
    "responsive_seller_candidate",
}

REQUIRED_CONTRACT_FIELDS = {
    "detector_name",
    "detector_family",
    "input_sources",
    "required_fields",
    "optional_fields",
    "measurement_formula",
    "calibration_status",
    "confidence",
    "strength_score",
    "thresholds",
    "output_schema",
    "validation_invariants",
    "forbidden_behavior",
}

REQUIRED_OUTPUT_FIELDS = {
    "layer",
    "engine",
    "record_type",
    "detector_name",
    "detector_family",
    "symbol",
    "timeframe",
    "window_start_ts",
    "window_end_ts",
    "event_type",
    "side",
    "direction",
    "calibration_status",
    "confidence",
    "strength_score",
    "thresholds",
    "measurements",
    "reason",
    "source_refs",
    "context_refs",
    "data_quality",
    "validation",
}

FORBIDDEN_SEMANTICS = {
    "no confidence": ("confidence",),
    "no strength score": ("strength score", "strength_score"),
    "no threshold": ("threshold",),
    "no trade decision": ("trade decision", "trading decision"),
}


def _has_forbidden_semantic(items: Any, terms: tuple[str, ...]) -> bool:
    if not isinstance(items, list):
        return False
    text = " ".join(str(item).lower() for item in items)
    return any(term in text for term in terms)


def verify_registry() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(detector_contracts, "DETECTOR_CONTRACTS", None)

    if not isinstance(contracts, list):
        errors.append("DETECTOR_CONTRACTS must exist and be a list")
        contracts = []

    checked_contracts = len(contracts)
    if checked_contracts != len(EXPECTED_DETECTORS):
        errors.append(f"expected 11 contracts, found {checked_contracts}")

    names = [contract.get("detector_name") for contract in contracts if isinstance(contract, dict)]
    if len(names) != len(set(names)):
        errors.append("detector_name values must be unique")

    missing_contracts = sorted(EXPECTED_DETECTORS - set(names))
    unexpected_contracts = sorted(set(names) - EXPECTED_DETECTORS)
    if missing_contracts:
        errors.append(f"missing contracts: {', '.join(missing_contracts)}")
    if unexpected_contracts:
        errors.append(f"unexpected contracts: {', '.join(unexpected_contracts)}")

    passed = 0
    for index, contract in enumerate(contracts):
        contract_errors: list[str] = []
        if not isinstance(contract, dict):
            errors.append(f"contract[{index}] must be a dict")
            continue

        name = str(contract.get("detector_name", f"contract[{index}]"))
        missing_fields = sorted(REQUIRED_CONTRACT_FIELDS - contract.keys())
        if missing_fields:
            contract_errors.append(f"missing contract fields: {', '.join(missing_fields)}")

        if contract.get("calibration_status") != "uncalibrated":
            contract_errors.append("calibration_status must be uncalibrated")
        if contract.get("confidence") is not None:
            contract_errors.append("confidence must be None")
        if contract.get("strength_score") is not None:
            contract_errors.append("strength_score must be None")
        if contract.get("thresholds") is not None:
            contract_errors.append("thresholds must be None")

        output_schema = contract.get("output_schema")
        if not isinstance(output_schema, dict):
            contract_errors.append("output_schema must be a dict")
        else:
            missing_output_fields = sorted(REQUIRED_OUTPUT_FIELDS - output_schema.keys())
            if missing_output_fields:
                contract_errors.append(f"missing output_schema fields: {', '.join(missing_output_fields)}")
            if output_schema.get("calibration_status") != "uncalibrated":
                contract_errors.append("output_schema calibration_status must be uncalibrated")
            if output_schema.get("confidence") is not None:
                contract_errors.append("output_schema confidence must be None")
            if output_schema.get("strength_score") is not None:
                contract_errors.append("output_schema strength_score must be None")
            if output_schema.get("thresholds") is not None:
                contract_errors.append("output_schema thresholds must be None")

        forbidden_behavior = contract.get("forbidden_behavior")
        for semantic, terms in FORBIDDEN_SEMANTICS.items():
            if not _has_forbidden_semantic(forbidden_behavior, terms):
                contract_errors.append(f"forbidden_behavior missing semantic: {semantic}")

        if contract_errors:
            errors.extend(f"{name}: {error}" for error in contract_errors)
        else:
            passed += 1

    failed = checked_contracts - passed
    test_passed = (
        checked_contracts == len(EXPECTED_DETECTORS)
        and passed == len(EXPECTED_DETECTORS)
        and not missing_contracts
        and not errors
    )
    return {
        "checked_contracts": checked_contracts,
        "passed": passed,
        "failed": failed,
        "missing_contracts": missing_contracts,
        "errors": errors,
        "test_passed": test_passed,
    }


def main() -> int:
    report = verify_registry()
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print("DETECTOR CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/detector_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
