"""Verify Layer-6C volume-profile contracts."""

import json
from pathlib import Path
from typing import Any

import volume_profile_contracts


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "volume_profile_contract_verification_report.json"
EXPECTED = {
    "volume_profile_snapshot", "poc_level_candidate", "value_area_candidate",
    "hvn_candidate", "lvn_candidate", "balance_zone_candidate",
    "acceptance_zone_candidate", "rejection_zone_candidate",
    "volume_memory_zone_candidate", "poc_shift_candidate",
    "value_area_touch_candidate", "hvn_touch_candidate", "lvn_touch_candidate",
    "p_shape_profile_candidate", "b_shape_profile_candidate",
    "d_shape_profile_candidate", "b_distribution_profile_candidate",
    "trend_profile_candidate", "failed_auction_candidate",
    "failed_action_return_to_value_candidate",
}
REQUIRED = {"concept_name", "concept_family", "input_sources", "required_fields",
            "optional_fields", "measurement_formula", "calibration_status", "confidence",
            "strength_score", "thresholds", "output_schema", "validation_invariants",
            "forbidden_behavior"}
SNAPSHOT_FIELDS = {"layer", "engine", "record_type", "symbol", "timeframe",
                   "window_start_ts", "window_end_ts", "profile", "location", "auction",
                   "memory_refs", "calibration_status", "scores", "data_quality", "validation"}
EVENT_FIELDS = {"layer", "engine", "record_type", "event_id", "symbol", "timeframe",
                "window_start_ts", "window_end_ts", "event_type", "side", "direction",
                "level", "zone", "reason", "source_refs", "calibration_status", "confidence",
                "strength_score", "thresholds", "validation"}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    contracts = getattr(volume_profile_contracts, "VOLUME_PROFILE_CONTRACTS", None)
    if not isinstance(contracts, list):
        contracts = []; errors.append("VOLUME_PROFILE_CONTRACTS is missing or invalid")
    names = [item.get("concept_name") for item in contracts if isinstance(item, dict)]
    if len(contracts) != 20: errors.append(f"expected 20 contracts, found {len(contracts)}")
    if len(names) != len(set(names)): errors.append("concept_name values are not unique")
    missing_names = sorted(EXPECTED - set(names))
    if missing_names: errors.append("missing contracts: " + ", ".join(missing_names))
    for item in contracts:
        if not isinstance(item, dict): errors.append("contract is not a dictionary"); continue
        name = str(item.get("concept_name") or "")
        missing = sorted(REQUIRED - item.keys())
        if missing: errors.append(f"{name}: missing fields: {', '.join(missing)}")
        if item.get("calibration_status") != "uncalibrated": errors.append(f"{name}: calibration_status must be uncalibrated")
        for field in ("confidence", "strength_score", "thresholds"):
            if item.get(field) is not None: errors.append(f"{name}: {field} must be null")
        schema = item.get("output_schema") if isinstance(item.get("output_schema"), dict) else {}
        expected_fields = SNAPSHOT_FIELDS if name == "volume_profile_snapshot" else EVENT_FIELDS
        schema_missing = sorted(expected_fields - schema.keys())
        if schema_missing: errors.append(f"{name}: output_schema missing: {', '.join(schema_missing)}")
        forbidden = " ".join(str(value).lower() for value in item.get("forbidden_behavior", []))
        for phrase in ("no trade decision", "no setup", "no confidence", "no strength score", "no threshold"):
            if phrase not in forbidden: errors.append(f"{name}: forbidden_behavior missing {phrase}")
    report = {"checked_contracts": len(contracts), "passed": len(contracts) if not errors else 0,
              "failed": 0 if not errors else len(contracts), "errors": errors,
              "test_passed": not errors}
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("VOLUME PROFILE CONTRACT VERIFICATION COMPLETE")
    print(f"checked_contracts={report['checked_contracts']}")
    print(f"passed={report['passed']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/volume_profile_contract_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
