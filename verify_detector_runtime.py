"""Verify detector runtime events against the contract registry."""

import json
from pathlib import Path
from typing import Any

from detector_contracts import get_detector_contract, validate_detector_contracts


ROOT_DIR = Path(__file__).resolve().parent
EVENTS_PATH = ROOT_DIR / "data" / "detector_events.jsonl"
REPORT_PATH = ROOT_DIR / "data" / "detector_runtime_verification_report.json"
REQUIRED_RUNTIME_FIELDS = {
    "contract_name",
    "contract_version",
    "calibration_status",
    "validation_passed",
    "event_type",
}
MAX_ERROR_SAMPLES = 100


def verify_runtime() -> dict[str, Any]:
    errors: list[str] = []
    validation_errors = 0
    events_produced = 0
    integrated_events = 0
    legacy_events = 0
    events_registry_matched = 0
    detectors: set[str] = set()

    def add_error(message: str) -> None:
        nonlocal validation_errors
        validation_errors += 1
        if len(errors) < MAX_ERROR_SAMPLES:
            errors.append(message)

    for registry_error in validate_detector_contracts():
        add_error(f"registry: {registry_error}")

    if not EVENTS_PATH.exists():
        add_error("data/detector_events.jsonl does not exist")
    else:
        with EVENTS_PATH.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                events_produced += 1
                try:
                    event = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    add_error(f"line {line_number}: invalid JSON: {exc.msg}")
                    continue
                if not isinstance(event, dict):
                    add_error(f"line {line_number}: event must be an object")
                    continue
                missing = sorted(REQUIRED_RUNTIME_FIELDS - event.keys())
                if missing:
                    legacy_events += 1
                    add_error(f"line {line_number}: missing runtime fields: {', '.join(missing)}")
                    continue
                integrated_events += 1
                contract_name = event["contract_name"]
                contract = get_detector_contract(contract_name)
                if contract is None:
                    add_error(f"line {line_number}: undefined contract: {contract_name}")
                    continue
                allowed = contract["output_schema"]["event_type"]
                allowed_events = set(allowed if isinstance(allowed, list) else [allowed])
                event_errors: list[str] = []
                if event["event_type"] not in allowed_events:
                    event_errors.append(f"event_type {event['event_type']} is not allowed")
                if event["contract_version"] != contract["contract_version"]:
                    event_errors.append("contract_version mismatch")
                if event["calibration_status"] != contract["calibration_status"]:
                    event_errors.append("calibration_status mismatch")
                if event["validation_passed"] is not True:
                    event_errors.append("validation_passed is not true")
                if event_errors:
                    for event_error in event_errors:
                        add_error(f"line {line_number}: {event_error}")
                    continue
                events_registry_matched += 1
                detectors.add(contract_name)

    report = {
        "detectors_run": len(detectors),
        "detector_names": sorted(detectors),
        "events_produced": events_produced,
        "integrated_events": integrated_events,
        "legacy_events": legacy_events,
        "events_registry_matched": events_registry_matched,
        "validation_errors": validation_errors,
        "error_samples": errors,
        "error_samples_truncated": validation_errors > len(errors),
        "test_passed": events_produced == events_registry_matched and validation_errors == 0,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify_runtime()
    print("DETECTOR RUNTIME VERIFICATION COMPLETE")
    print(f"detectors_run={report['detectors_run']}")
    print(f"events_produced={report['events_produced']}")
    print(f"events_registry_matched={report['events_registry_matched']}")
    print(f"validation_errors={report['validation_errors']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/detector_runtime_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
