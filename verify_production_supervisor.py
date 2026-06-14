"""Verify Smart Money and Observer registration in the production supervisor."""

import importlib
import json
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
REPORT_FILE = DATA_DIR / "production_supervisor_verification_report.json"
PRODUCTION_HEALTH_FILE = DATA_DIR / "production_supervisor_health.json"
SMART_OUTPUTS = {
    "smart_money_dna.jsonl",
    "structure_events.jsonl",
    "smart_money_health.json",
}
OBSERVER_OUTPUTS = {
    "observer_states.jsonl",
    "observer_events.jsonl",
    "observer_health.json",
}


def verify() -> dict[str, Any]:
    errors: list[str] = []
    supervisor = None
    try:
        supervisor = importlib.import_module("production_supervisor")
    except Exception as exc:
        errors.append(f"production_supervisor import failed: {exc}")

    engine_count = 0
    registered = False
    health_path_ok = False
    outputs_registered = False
    structure_noncritical = False
    health_contains_smart_money = False
    observer_registered = False
    observer_health_path_ok = False
    observer_outputs_registered = False
    health_contains_observer = False

    if supervisor is not None:
        specs = getattr(supervisor, "ENGINE_SPECS", ())
        engine_count = len(specs)
        smart_specs = [spec for spec in specs if getattr(spec, "script", None) == "smart_money_engine.py"]
        registered = len(smart_specs) == 1
        if registered:
            spec = smart_specs[0]
            health_path_ok = getattr(spec, "health_file", None) == "smart_money_health.json"
            outputs_registered = SMART_OUTPUTS.issubset(set(getattr(spec, "output_files", ())))
            structure_noncritical = "structure_events.jsonl" in set(getattr(spec, "noncritical_outputs", ()))
        required_outputs = set(getattr(supervisor, "REQUIRED_OUTPUTS", ()))
        if not SMART_OUTPUTS.issubset(required_outputs):
            errors.append("Smart Money outputs are missing from REQUIRED_OUTPUTS")
        noncritical = set(getattr(supervisor, "NONCRITICAL_REQUIRED_OUTPUTS", ()))
        if "structure_events.jsonl" not in noncritical:
            errors.append("structure_events.jsonl must be noncritical")
        if getattr(supervisor, "SMART_MONEY_STRUCTURE_WARNING_SECONDS", None) != 300:
            errors.append("Smart Money structure warning grace must be 300 seconds")
        observer_specs = [spec for spec in specs if getattr(spec, "script", None) == "observer_engine.py"]
        observer_registered = len(observer_specs) == 1
        if observer_registered:
            observer_spec = observer_specs[0]
            observer_health_path_ok = getattr(observer_spec, "health_file", None) == "observer_health.json"
            observer_outputs_registered = OBSERVER_OUTPUTS.issubset(
                set(getattr(observer_spec, "output_files", ()))
            )
        if not OBSERVER_OUTPUTS.issubset(required_outputs):
            errors.append("Observer outputs are missing from REQUIRED_OUTPUTS")

    if engine_count != 9:
        errors.append(f"expected 9 engines, found {engine_count}")
    if not registered:
        errors.append("smart_money_engine.py is not registered exactly once")
    if not health_path_ok:
        errors.append("Smart Money health path is incorrect")
    if not outputs_registered:
        errors.append("Smart Money expected outputs are incomplete")
    if not structure_noncritical:
        errors.append("structure_events.jsonl is not configured as noncritical")
    if not observer_registered:
        errors.append("observer_engine.py is not registered exactly once")
    if not observer_health_path_ok:
        errors.append("Observer health path is incorrect")
    if not observer_outputs_registered:
        errors.append("Observer expected outputs are incomplete")

    if PRODUCTION_HEALTH_FILE.exists():
        try:
            health = json.loads(PRODUCTION_HEALTH_FILE.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            errors.append(f"production supervisor health is invalid: {exc}")
        else:
            health_contains_smart_money = "smart_money_engine.py" in health.get("engines", {})
            health_contains_observer = "observer_engine.py" in health.get("engines", {})
            required_health_outputs = set(health.get("required_outputs", {}))
            if not SMART_OUTPUTS.issubset(required_health_outputs):
                errors.append("production health required_outputs lacks Smart Money outputs")
            if not OBSERVER_OUTPUTS.issubset(required_health_outputs):
                errors.append("production health required_outputs lacks Observer outputs")

    report = {
        "checked": True,
        "engine_count_expected": 9,
        "smart_money_engine_registered": registered,
        "smart_money_health_path_ok": health_path_ok,
        "smart_money_outputs_registered": outputs_registered,
        "production_health_contains_smart_money": health_contains_smart_money,
        "observer_engine_registered": observer_registered,
        "observer_health_path_ok": observer_health_path_ok,
        "observer_outputs_registered": observer_outputs_registered,
        "production_health_contains_observer": health_contains_observer,
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
    print(f"smart_money_engine_registered={str(report['smart_money_engine_registered']).lower()}")
    print(f"observer_engine_registered={str(report['observer_engine_registered']).lower()}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/production_supervisor_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
