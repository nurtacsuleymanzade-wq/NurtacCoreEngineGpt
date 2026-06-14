"""Verify Layer-6 calibration observation outputs."""

import importlib
import json
from collections import deque
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
REPORT_FILE = ROOT_DIR / "data" / "calibration_verification_report.json"
EXPECTED_HORIZONS = [30000, 60000, 180000, 300000]
EXPECTED_OUTPUTS = {
    "OBSERVATIONS_FILE": "calibration_observations.jsonl",
    "PROFILES_FILE": "calibration_profiles.json",
    "HEALTH_FILE": "calibration_health.json",
}
SCORE_FIELDS = ("confidence", "strength_score", "edge_score", "threshold")


def check_scores(scores: Any, label: str, errors: list[str]) -> None:
    if not isinstance(scores, dict):
        errors.append(f"{label}: scores must be an object")
        return
    for field in SCORE_FIELDS:
        if field not in scores:
            errors.append(f"{label}: missing score field {field}")
        elif scores[field] is not None:
            errors.append(f"{label}: {field} must be null")


def load_last_rows(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=limit)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return list(rows)


def verify() -> dict[str, Any]:
    errors: list[str] = []
    checked_observations = 0
    checked_profiles = 0
    try:
        engine = importlib.import_module("calibration_engine")
    except Exception as exc:
        errors.append(f"calibration_engine import failed: {exc}")
        engine = None

    if engine is not None:
        if getattr(engine, "HORIZONS", None) != EXPECTED_HORIZONS:
            errors.append("HORIZONS do not match the required observation horizons")
        for attribute, filename in EXPECTED_OUTPUTS.items():
            path = getattr(engine, attribute, None)
            if not isinstance(path, Path) or path.name != filename:
                errors.append(f"{attribute} must point to {filename}")

        observations = load_last_rows(engine.OBSERVATIONS_FILE, 100)
        checked_observations = len(observations)
        for index, observation in enumerate(observations):
            label = f"observation[{index}]"
            check_scores(observation.get("scores"), label, errors)
            if observation.get("calibration_status") != "observed_not_scored":
                errors.append(f"{label}: invalid calibration_status")
            validation = observation.get("validation", {})
            if validation.get("all_horizons_measured") is not True:
                errors.append(f"{label}: all_horizons_measured must be true")
            outcomes = observation.get("outcomes", {})
            if set(outcomes) != {"30s", "60s", "180s", "300s"}:
                errors.append(f"{label}: all horizon outcomes are required")

        if engine.PROFILES_FILE.exists():
            try:
                profile = json.loads(engine.PROFILES_FILE.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                errors.append(f"profile file invalid: {exc}")
            else:
                check_scores(profile.get("scores"), "profile", errors)
                if profile.get("calibration_status") != "observed_not_scored":
                    errors.append("profile: invalid calibration_status")
                groups = profile.get("groups")
                if not isinstance(groups, list):
                    errors.append("profile: groups must be a list")
                else:
                    checked_profiles = len(groups)
                    for index, group in enumerate(groups):
                        check_scores(group.get("scores"), f"profile_group[{index}]", errors)

    report = {
        "checked_observations": checked_observations,
        "checked_profiles": checked_profiles,
        "failed": len(errors),
        "errors": errors,
        "test_passed": not errors,
    }
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = verify()
    print("CALIBRATION ENGINE VERIFICATION COMPLETE")
    print(f"checked_observations={report['checked_observations']}")
    print(f"checked_profiles={report['checked_profiles']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/calibration_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
