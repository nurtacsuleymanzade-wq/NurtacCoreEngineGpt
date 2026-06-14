"""Verify Layer-6B historical outcome runtime outputs."""

import importlib
import json
from collections import deque
from pathlib import Path
from typing import Any

from calibration_contracts import FORWARD_HORIZONS


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
OBSERVATIONS_FILE = DATA_DIR / "historical_outcome_observations.jsonl"
PROFILES_FILE = DATA_DIR / "calibration_profiles.json"
HEALTH_FILE = DATA_DIR / "historical_outcome_health.json"
REPORT_FILE = DATA_DIR / "historical_outcome_verification_report.json"
SCORE_FIELDS = ("confidence", "strength_score", "edge_score", "probability_score", "threshold", "decision_score")


def tail_jsonl(path: Path, count: int = 100) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=count)
    if not path.exists(): return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, 1):
            try: row = json.loads(line)
            except json.JSONDecodeError: rows.append({"_invalid": line_number}); continue
            if isinstance(row, dict): rows.append(row)
    return list(rows)


def null_scores(scores: Any) -> bool:
    return isinstance(scores, dict) and all(scores.get(field) is None for field in SCORE_FIELDS)


def verify() -> dict[str, Any]:
    errors: list[str] = []
    try: importlib.import_module("historical_outcome_engine")
    except Exception as exc: errors.append(f"historical_outcome_engine import failed: {exc}")
    observations = tail_jsonl(OBSERVATIONS_FILE)
    for index, row in enumerate(observations):
        prefix = f"observation[{index}]"
        if "_invalid" in row: errors.append(f"{prefix}: invalid JSON"); continue
        if row.get("calibration_status") != "observed_not_scored": errors.append(f"{prefix}: calibration status invalid")
        if not null_scores(row.get("scores")): errors.append(f"{prefix}: scores must be null")
        validation = row.get("validation") if isinstance(row.get("validation"), dict) else {}
        if validation.get("all_horizons_measured") is not True: errors.append(f"{prefix}: all_horizons_measured must be true")
        if validation.get("future_leakage_detected") is not False: errors.append(f"{prefix}: future leakage flag invalid")
        event_ts = row.get("event_window_start_ts"); reference_ts = nested(row, "reference", "price_ts")
        if not isinstance(event_ts, int) or not isinstance(reference_ts, int) or reference_ts > event_ts: errors.append(f"{prefix}: invalid reference timestamp")
        for label, horizon in FORWARD_HORIZONS.items():
            future_ts = nested(row, "outcomes", label, "future_price_ts")
            if not isinstance(future_ts, int) or not isinstance(event_ts, int) or future_ts < event_ts + horizon:
                errors.append(f"{prefix}: {label} future timestamp precedes target")
    profile_groups: list[dict[str, Any]] = []
    if PROFILES_FILE.exists():
        try: profile = json.loads(PROFILES_FILE.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc: errors.append(f"calibration_profiles.json invalid: {exc}"); profile = {}
        if profile and not null_scores(profile.get("scores")): errors.append("profile root scores must be null")
        profile_groups = profile.get("groups", []) if isinstance(profile, dict) else []
        for index, group in enumerate(profile_groups):
            if not null_scores(group.get("scores")): errors.append(f"profile[{index}]: scores must be null")
            if group.get("sample_status") not in ("insufficient_data", "observed_sample"): errors.append(f"profile[{index}]: invalid sample_status")
    if not HEALTH_FILE.exists(): errors.append("historical_outcome_health.json is missing")
    report = {"checked_observations": len(observations), "checked_profiles": len(profile_groups), "failed": len(errors), "errors": errors, "test_passed": not errors}
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def nested(value: Any, *path: str) -> Any:
    for key in path:
        if not isinstance(value, dict): return None
        value = value.get(key)
    return value


def main() -> int:
    report = verify()
    print("HISTORICAL OUTCOME ENGINE VERIFICATION COMPLETE")
    print(f"checked_observations={report['checked_observations']}")
    print(f"checked_profiles={report['checked_profiles']}")
    print(f"failed={report['failed']}")
    print(f"test_passed={str(report['test_passed']).lower()}")
    print("report=data/historical_outcome_verification_report.json")
    return 0 if report["test_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
