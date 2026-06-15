"""Verify production supervisor registration including Volume Profile Engine."""

import importlib
import json
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
REPORT_FILE = DATA_DIR / "production_supervisor_verification_report.json"
PRODUCTION_HEALTH_FILE = DATA_DIR / "production_supervisor_health.json"
VOLUME_OUTPUTS = {
    "volume_profile_dna.jsonl", "volume_profile_events.jsonl",
    "volume_memory_zones.json", "volume_profile_health.json",
}
VOLUME_REQUIRED_OUTPUTS = {
    "volume_profile_dna.jsonl", "volume_memory_zones.json", "volume_profile_health.json",
}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    try:
        supervisor = importlib.import_module("production_supervisor")
    except Exception as exc:
        supervisor = None
        errors.append(f"production_supervisor import failed: {exc}")

    engine_count = 0
    registered = False
    health_path_ok = False
    outputs_registered = False
    start_order_ok = False
    events_noncritical = False
    events_not_globally_required = False
    health_contains_volume = False

    optional_scripts = ("observer_engine.py", "historical_outcome_engine.py", "setup_engine.py")
    expected_engine_count = 9 + sum((ROOT_DIR / script).exists() for script in optional_scripts)
    if supervisor is not None:
        specs = tuple(getattr(supervisor, "ENGINE_SPECS", ()))
        engine_count = len(specs)
        matches = [spec for spec in specs if getattr(spec, "script", None) == "volume_profile_engine.py"]
        registered = len(matches) == 1
        if registered:
            spec = matches[0]
            health_path_ok = getattr(spec, "health_file", None) == "volume_profile_health.json"
            outputs_registered = VOLUME_OUTPUTS == set(getattr(spec, "output_files", ()))
            events_noncritical = "volume_profile_events.jsonl" in set(getattr(spec, "noncritical_outputs", ()))
            scripts = [item.script for item in specs]
            start_order_ok = scripts.index("volume_profile_engine.py") == scripts.index("smart_money_engine.py") + 1
        required = set(getattr(supervisor, "REQUIRED_OUTPUTS", ()))
        if not VOLUME_REQUIRED_OUTPUTS.issubset(required):
            errors.append("Volume Profile required outputs are incomplete")
        events_not_globally_required = "volume_profile_events.jsonl" not in required

    if engine_count != expected_engine_count:
        errors.append(f"expected {expected_engine_count} engines, found {engine_count}")
    if not registered: errors.append("volume_profile_engine.py is not registered exactly once")
    if not health_path_ok: errors.append("Volume Profile health path is incorrect")
    if not outputs_registered: errors.append("Volume Profile expected outputs are incomplete")
    if not start_order_ok: errors.append("Volume Profile must start immediately after Smart Money")
    if not events_noncritical: errors.append("volume_profile_events.jsonl must be noncritical")
    if not events_not_globally_required: errors.append("volume_profile_events.jsonl must not be globally required")

    if PRODUCTION_HEALTH_FILE.exists():
        try:
            health = json.loads(PRODUCTION_HEALTH_FILE.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            errors.append(f"production supervisor health is invalid: {exc}")
        else:
            health_contains_volume = "volume_profile_engine.py" in health.get("engines", {})
            if not health_contains_volume:
                errors.append("production health lacks volume_profile_engine.py")
            health_required = set(health.get("required_outputs", {}))
            if not VOLUME_REQUIRED_OUTPUTS.issubset(health_required):
                errors.append("production health required_outputs lacks Volume Profile outputs")
            if "volume_profile_events.jsonl" in health_required:
                errors.append("volume_profile_events.jsonl is incorrectly critical in production health")

    report = {
        "checked": True,
        "engine_count_expected": expected_engine_count,
        "engine_count_actual": engine_count,
        "volume_profile_engine_registered": registered,
        "volume_profile_health_path_ok": health_path_ok,
        "volume_profile_outputs_registered": outputs_registered,
        "volume_profile_start_order_ok": start_order_ok,
        "volume_profile_events_noncritical": events_noncritical and events_not_globally_required,
        "production_health_contains_volume_profile": health_contains_volume,
        "errors": errors,
        "test_passed": not errors,
    }
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("PRODUCTION SUPERVISOR VERIFICATION COMPLETE")
    print(f"engine_count_expected={report['engine_count_expected']}")
    print(f"engine_count_actual={report['engine_count_actual']}")
    print(f"volume_profile_engine_registered={str(report['volume_profile_engine_registered']).lower()}")
    print(f"volume_profile_events_noncritical={str(report['volume_profile_events_noncritical']).lower()}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/production_supervisor_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
