"""Continuous production process supervisor including Layer-6C Volume Profile."""

import time
from pathlib import Path
from typing import Any

import test_supervisor as supervisor_runtime
from test_supervisor import EngineSpec


VOLUME_PROFILE_EVENTS_WARNING_SECONDS = 300
VOLUME_PROFILE_OUTPUTS = (
    "volume_profile_dna.jsonl",
    "volume_profile_events.jsonl",
    "volume_memory_zones.json",
    "volume_profile_health.json",
)
VOLUME_PROFILE_REQUIRED_OUTPUTS = (
    "volume_profile_dna.jsonl",
    "volume_profile_health.json",
    "volume_memory_zones.json",
)
VOLUME_PROFILE_HEALTH_FIELDS = (
    "status",
    "input_rows_processed",
    "snapshots_written",
    "events_written",
    "memory_zones",
    "last_window_ts",
    "missing_inputs",
    "warnings",
    "registry_validation_passed",
)
VOLUME_PROFILE_TIMEFRAMES = ("1S", "3S", "5S", "15S", "1M")

VOLUME_PROFILE_ENGINE = EngineSpec(
    "layer_6c",
    "volume_profile_engine.py",
    VOLUME_PROFILE_OUTPUTS,
    "window_start_ts",
    "volume_profile_health.json",
    "last_window_ts",
    VOLUME_PROFILE_HEALTH_FIELDS,
    ("volume_profile_events.jsonl",),
)


def _with_volume_profile(specs: tuple[EngineSpec, ...]) -> tuple[EngineSpec, ...]:
    filtered = tuple(spec for spec in specs if spec.script != "volume_profile_engine.py")
    smart_index = next(
        index for index, spec in enumerate(filtered) if spec.script == "smart_money_engine.py"
    )
    return filtered[: smart_index + 1] + (VOLUME_PROFILE_ENGINE,) + filtered[smart_index + 1 :]


ENGINE_SPECS = _with_volume_profile(supervisor_runtime.ENGINE_SPECS)
REQUIRED_OUTPUTS = tuple(dict.fromkeys(
    supervisor_runtime.REQUIRED_OUTPUTS + VOLUME_PROFILE_REQUIRED_OUTPUTS
))
NONCRITICAL_REQUIRED_OUTPUTS = set(supervisor_runtime.NONCRITICAL_REQUIRED_OUTPUTS)
SMART_MONEY_STRUCTURE_WARNING_SECONDS = supervisor_runtime.SMART_MONEY_STRUCTURE_WARNING_SECONDS

SMART_MONEY_ENGINE = next(spec for spec in ENGINE_SPECS if spec.script == "smart_money_engine.py")
OBSERVER_ENGINE = next((spec for spec in ENGINE_SPECS if spec.script == "observer_engine.py"), None)
HISTORICAL_OUTCOME_ENGINE = next((spec for spec in ENGINE_SPECS if spec.script == "historical_outcome_engine.py"), None)
SETUP_ENGINE = next((spec for spec in ENGINE_SPECS if spec.script == "setup_engine.py"), None)


def _volume_profile_snapshot(runtime: dict[str, Any]) -> dict[str, Any]:
    snapshot = supervisor_runtime.engine_snapshot(runtime)
    snapshot["warnings"] = [
        warning
        for warning in snapshot["warnings"]
        if warning != "smart_money_structure_events_missing"
    ]
    health = snapshot.get("health")
    if isinstance(health, dict):
        processed = health.get("input_rows_processed")
        if not isinstance(processed, dict):
            if "health_field_missing:input_rows_processed" not in snapshot["warnings"]:
                snapshot["warnings"].append("health_field_missing:input_rows_processed")
        else:
            for timeframe in VOLUME_PROFILE_TIMEFRAMES:
                if timeframe not in processed:
                    snapshot["warnings"].append(
                        f"health_field_missing:input_rows_processed.{timeframe}"
                    )
    elapsed = max(0.0, time.time() - runtime["started_at"])
    events_path = supervisor_runtime.DATA_DIR / "volume_profile_events.jsonl"
    if not events_path.exists() and elapsed >= VOLUME_PROFILE_EVENTS_WARNING_SECONDS:
        snapshot["warnings"].append("volume_profile_events_missing")
    if snapshot.get("last_window_ts") is None:
        snapshot["last_window_ts"] = 0
    return snapshot


def engine_snapshot(runtime: dict[str, Any]) -> dict[str, Any]:
    if runtime["spec"].script == "volume_profile_engine.py":
        return _volume_profile_snapshot(runtime)
    return supervisor_runtime.engine_snapshot(runtime)


def collect_supervisor_health(runtimes: list[dict[str, Any]]) -> dict[str, Any]:
    engines = {runtime["spec"].script: engine_snapshot(runtime) for runtime in runtimes}
    required_outputs = {
        filename: {
            "exists": (supervisor_runtime.DATA_DIR / filename).exists(),
            "size_bytes": (supervisor_runtime.DATA_DIR / filename).stat().st_size
            if (supervisor_runtime.DATA_DIR / filename).exists()
            else 0,
        }
        for filename in REQUIRED_OUTPUTS
    }
    warnings = []
    for script, snapshot in engines.items():
        warnings.extend(f"{script}:{warning}" for warning in snapshot["warnings"])
    for filename, state in required_outputs.items():
        if not state["exists"] and filename not in NONCRITICAL_REQUIRED_OUTPUTS:
            warnings.append(f"required_output_missing:data/{filename}")
        elif not state["exists"] and filename == "structure_events.jsonl":
            smart_runtime = next(
                (item for item in runtimes if item["spec"].script == "smart_money_engine.py"),
                None,
            )
            if smart_runtime and time.time() - smart_runtime["started_at"] >= SMART_MONEY_STRUCTURE_WARNING_SECONDS:
                warnings.append("smart_money_structure_events_missing")
    return {
        "status": "alive" if all(item["process_alive"] for item in engines.values()) else "degraded",
        "checked_at": time.time(),
        "engines": engines,
        "required_outputs": required_outputs,
        "warnings": warnings,
    }


def run_supervisor(duration: int, clean: bool = False) -> dict[str, Any]:
    supervisor_runtime.ENGINE_SPECS = ENGINE_SPECS
    supervisor_runtime.REQUIRED_OUTPUTS = REQUIRED_OUTPUTS
    supervisor_runtime.NONCRITICAL_REQUIRED_OUTPUTS = NONCRITICAL_REQUIRED_OUTPUTS
    supervisor_runtime.collect_supervisor_health = collect_supervisor_health
    return supervisor_runtime.run_supervisor(duration=duration, clean=clean)


if __name__ == "__main__":
    run_supervisor(duration=0, clean=False)
