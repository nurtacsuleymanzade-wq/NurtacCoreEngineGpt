"""Production-capable supervisor for the Layer-0 through Layer-7 pipeline."""

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from detector_contracts import validate_detector_contracts
from observer_contracts import validate_observer_contracts
from verify_evidence_contracts import verify_registry as verify_evidence_registry


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
LOGS_DIR = ROOT_DIR / "logs"
REPORT_FILE = DATA_DIR / "supervisor_test_report.json"
SUPERVISOR_HEALTH_FILE = DATA_DIR / "production_supervisor_health.json"

STATUS_INTERVAL_SECONDS = 10
RESTART_DELAY_SECONDS = 2


@dataclass(frozen=True)
class EngineSpec:
    name: str
    script: str
    output_files: tuple[str, ...]
    timestamp_field: str
    health_file: str | None = None
    health_timestamp_field: str | None = None
    required_health_fields: tuple[str, ...] = ()
    noncritical_outputs: tuple[str, ...] = ()


ENGINE_SPECS = (
    EngineSpec("layer_0", "main.py", ("one_second_combined_dna.jsonl",), "window_start_ts"),
    EngineSpec("layer_1", "rolling_window_engine.py", ("rolling_3s_dna.jsonl",), "window_start_ts"),
    EngineSpec("layer_2", "aligned_candle_engine.py", ("aligned_1m_candle_dna.jsonl",), "window_start_ts"),
    EngineSpec(
        "sync_integrity",
        "sync_integrity_engine.py",
        ("system_health.json", "data_quality.jsonl"),
        "window_start_ts",
        "system_health.json",
        "layer_0.last_window_ts",
    ),
    EngineSpec("layer_3", "context_engine.py", ("context_dna.jsonl",), "source_window_ts"),
    EngineSpec(
        "layer_4",
        "detector_engine.py",
        ("detector_measurements.jsonl", "detector_events.jsonl"),
        "window_start_ts",
        "detector_health.json",
        "last_event_ts",
    ),
    EngineSpec(
        "layer_5",
        "evidence_engine.py",
        ("evidence_packets.jsonl",),
        "window_start_ts",
        "evidence_health.json",
        "last_window_ts",
    ),
    EngineSpec(
        "layer_6a",
        "smart_money_engine.py",
        ("smart_money_dna.jsonl", "structure_events.jsonl", "smart_money_health.json"),
        "window_start_ts",
        "smart_money_health.json",
        "last_window_ts",
        (
            "status",
            "processed_rows",
            "snapshots_written",
            "structure_events_written",
            "last_window_ts",
            "missing_inputs",
            "warnings",
            "registry_validation_passed",
        ),
        ("structure_events.jsonl",),
    ),
    EngineSpec(
        "layer_7",
        "observer_engine.py",
        ("observer_states.jsonl", "observer_events.jsonl", "observer_health.json"),
        "window_start_ts",
        "observer_health.json",
        "last_window_ts",
        (
            "status",
            "input_rows_processed",
            "observer_states_written",
            "observer_events_written",
            "open_watch_states",
            "last_window_ts",
            "missing_inputs",
            "warnings",
            "registry_validation_passed",
        ),
    ),
)

REQUIRED_OUTPUTS = (
    "context_dna.jsonl",
    "detector_measurements.jsonl",
    "detector_events.jsonl",
    "evidence_packets.jsonl",
    "smart_money_dna.jsonl",
    "structure_events.jsonl",
    "smart_money_health.json",
    "observer_states.jsonl",
    "observer_events.jsonl",
    "observer_health.json",
)

NONCRITICAL_REQUIRED_OUTPUTS = {"structure_events.jsonl"}
SMART_MONEY_STRUCTURE_WARNING_SECONDS = 300


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Layer-0 through Layer-7 pipeline supervisor")
    parser.add_argument(
        "--duration",
        type=int,
        default=600,
        help="Run duration in seconds. Use 0 for continuous production mode.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete generated JSONL data before startup. Disabled by default.",
    )
    return parser.parse_args()


def prepare_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def clean_jsonl_data() -> None:
    for path in DATA_DIR.glob("*.jsonl"):
        path.unlink()


def validate_contract_registries() -> dict[str, Any]:
    detector_errors = validate_detector_contracts()
    evidence_report = verify_evidence_registry()
    evidence_errors = list(evidence_report["errors"])
    observer_errors = validate_observer_contracts()
    errors = [f"detector: {error}" for error in detector_errors]
    errors.extend(f"evidence: {error}" for error in evidence_errors)
    errors.extend(f"observer: {error}" for error in observer_errors)
    return {
        "detector_registry_valid": not detector_errors,
        "evidence_registry_valid": bool(evidence_report["test_passed"]),
        "observer_registry_valid": not observer_errors,
        "errors": errors,
        "passed": not errors,
    }


def start_engine(spec: EngineSpec) -> dict[str, Any]:
    log_path = LOGS_DIR / f"{Path(spec.script).stem}.log"
    log_handle = log_path.open("a", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, spec.script],
        cwd=ROOT_DIR,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return {
        "spec": spec,
        "process": process,
        "log_handle": log_handle,
        "log_path": log_path,
        "started_at": time.time(),
        "restart_count": 0,
        "last_exit_code": None,
    }


def restart_engine(runtime: dict[str, Any]) -> None:
    runtime["log_handle"].close()
    time.sleep(RESTART_DELAY_SECONDS)
    spec = runtime["spec"]
    replacement = start_engine(spec)
    replacement["restart_count"] = runtime["restart_count"] + 1
    replacement["last_exit_code"] = runtime["process"].returncode
    runtime.clear()
    runtime.update(replacement)


def stop_process(runtime: dict[str, Any]) -> None:
    process: subprocess.Popen = runtime["process"]
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    runtime["last_exit_code"] = process.returncode
    runtime["log_handle"].close()


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def read_last_jsonl(path: Path) -> dict[str, Any] | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        with path.open("rb") as handle:
            position = handle.seek(0, 2)
            buffer = bytearray()
            while position > 0:
                position -= 1
                handle.seek(position)
                char = handle.read(1)
                if char == b"\n" and buffer:
                    break
                if char != b"\n":
                    buffer.extend(char)
            line = bytes(reversed(buffer)).decode("utf-8")
            payload = json.loads(line)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def nested_value(payload: dict[str, Any], dotted_field: str) -> Any:
    value: Any = payload
    for part in dotted_field.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def latest_file_mtime(paths: list[Path]) -> float | None:
    mtimes = [path.stat().st_mtime for path in paths if path.exists()]
    return max(mtimes) if mtimes else None


def engine_last_window(spec: EngineSpec) -> int | None:
    if spec.health_file and spec.health_timestamp_field:
        health = read_json(DATA_DIR / spec.health_file)
        if health is not None:
            timestamp = safe_int(nested_value(health, spec.health_timestamp_field))
            if timestamp is not None:
                return timestamp
    timestamps: list[int] = []
    for filename in spec.output_files:
        path = DATA_DIR / filename
        if path.suffix != ".jsonl":
            continue
        row = read_last_jsonl(path)
        if row is not None:
            timestamp = safe_int(row.get(spec.timestamp_field))
            if timestamp is not None:
                timestamps.append(timestamp)
    return max(timestamps) if timestamps else None


def engine_snapshot(runtime: dict[str, Any]) -> dict[str, Any]:
    spec: EngineSpec = runtime["spec"]
    process: subprocess.Popen = runtime["process"]
    process_alive = process.poll() is None
    output_paths = [DATA_DIR / filename for filename in spec.output_files]
    missing_outputs = [relative_path(path) for path in output_paths if not path.exists()]
    observed_paths = output_paths + [runtime["log_path"]]
    if spec.health_file:
        observed_paths.append(DATA_DIR / spec.health_file)
    heartbeat = latest_file_mtime(observed_paths)
    last_window_ts = engine_last_window(spec)
    lag_seconds = None
    if last_window_ts is not None and last_window_ts > 0:
        lag_seconds = max(0.0, (time.time() * 1000 - last_window_ts) / 1000)
    elapsed = max(0.0, time.time() - runtime["started_at"])
    warnings = []
    for path in missing_outputs:
        filename = Path(path).name
        if filename in spec.noncritical_outputs:
            if elapsed >= SMART_MONEY_STRUCTURE_WARNING_SECONDS:
                warnings.append("smart_money_structure_events_missing")
            continue
        warnings.append(f"missing_output:{path}")
    if not process_alive:
        warnings.append(f"process_exited:{process.returncode}")
    health_payload = read_json(DATA_DIR / spec.health_file) if spec.health_file else None
    if spec.health_file and health_payload is None:
        warnings.append(f"missing_or_invalid_health:data/{spec.health_file}")
    if health_payload is not None:
        for field_name in spec.required_health_fields:
            if field_name not in health_payload:
                warnings.append(f"health_field_missing:{field_name}")
    return {
        "script": spec.script,
        "pid": process.pid,
        "process_alive": process_alive,
        "heartbeat": heartbeat,
        "health": health_payload if health_payload is not None else {"status": "running" if process_alive else "stopped"},
        "last_window_ts": last_window_ts,
        "lag_seconds": lag_seconds,
        "missing_outputs": missing_outputs,
        "warnings": warnings,
        "restart_count": runtime["restart_count"],
        "last_exit_code": runtime["last_exit_code"],
        "log_file": relative_path(runtime["log_path"]),
    }


def collect_supervisor_health(runtimes: list[dict[str, Any]]) -> dict[str, Any]:
    engines = {runtime["spec"].script: engine_snapshot(runtime) for runtime in runtimes}
    required_outputs = {
        filename: {
            "exists": (DATA_DIR / filename).exists(),
            "size_bytes": (DATA_DIR / filename).stat().st_size if (DATA_DIR / filename).exists() else 0,
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
                (runtime for runtime in runtimes if runtime["spec"].script == "smart_money_engine.py"),
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


def write_supervisor_health(payload: dict[str, Any]) -> None:
    SUPERVISOR_HEALTH_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def relative_path(path: Path) -> str:
    return str(path.relative_to(ROOT_DIR)).replace("\\", "/")


def print_status(elapsed: int, health: dict[str, Any]) -> None:
    alive = sum(snapshot["process_alive"] for snapshot in health["engines"].values())
    print(
        f"[SUPERVISOR] elapsed={elapsed}s engines_alive={alive}/{len(ENGINE_SPECS)} "
        f"warnings={len(health['warnings'])}",
        flush=True,
    )
    for script, snapshot in health["engines"].items():
        print(
            f"[ENGINE] {script} heartbeat={snapshot['heartbeat']} "
            f"last_window_ts={snapshot['last_window_ts']} lag_seconds={snapshot['lag_seconds']} "
            f"warnings={len(snapshot['warnings'])}",
            flush=True,
        )


def build_report(
    duration: int,
    contract_validation: dict[str, Any],
    health: dict[str, Any],
) -> dict[str, Any]:
    all_processes_alive = all(item["process_alive"] for item in health["engines"].values())
    all_outputs_exist = all(
        state["exists"] or filename in NONCRITICAL_REQUIRED_OUTPUTS
        for filename, state in health["required_outputs"].items()
    )
    validation = {
        "contract_registries_valid": contract_validation["passed"],
        "all_engines_alive": all_processes_alive,
        "required_outputs_exist": all_outputs_exist,
        "test_passed": contract_validation["passed"] and all_processes_alive and all_outputs_exist,
    }
    return {
        "duration_seconds": duration,
        "contract_validation": contract_validation,
        "supervisor_health": health,
        "validation": validation,
    }


def run_supervisor(duration: int, clean: bool = False) -> dict[str, Any]:
    prepare_directories()
    if clean:
        clean_jsonl_data()
    contract_validation = validate_contract_registries()
    if not contract_validation["passed"]:
        raise RuntimeError("Contract registry validation failed: " + "; ".join(contract_validation["errors"]))

    runtimes: list[dict[str, Any]] = []
    start_time = time.monotonic()
    next_status_at = 0
    interrupted = False
    last_health: dict[str, Any] | None = None
    try:
        for spec in ENGINE_SPECS:
            runtimes.append(start_engine(spec))
            time.sleep(1)

        start_time = time.monotonic()
        while duration == 0 or time.monotonic() - start_time < duration:
            for runtime in runtimes:
                if runtime["process"].poll() is not None:
                    restart_engine(runtime)
            elapsed = int(time.monotonic() - start_time)
            health = collect_supervisor_health(runtimes)
            last_health = health
            write_supervisor_health(health)
            if elapsed >= next_status_at:
                print_status(elapsed, health)
                next_status_at = elapsed + STATUS_INTERVAL_SECONDS
            time.sleep(1)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        final_health = last_health if last_health is not None else collect_supervisor_health(runtimes)
        write_supervisor_health(final_health)
        for runtime in reversed(runtimes):
            stop_process(runtime)

    report = build_report(int(time.monotonic() - start_time), contract_validation, final_health)
    report["interrupted"] = interrupted
    REPORT_FILE.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def main() -> None:
    args = parse_args()
    report = run_supervisor(args.duration, args.clean)
    print("SUPERVISOR TEST COMPLETE", flush=True)
    print(f"test_passed={str(report['validation']['test_passed']).lower()}", flush=True)
    print("report=data/supervisor_test_report.json", flush=True)
    print("health=data/production_supervisor_health.json", flush=True)


if __name__ == "__main__":
    main()
