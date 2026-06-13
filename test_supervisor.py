import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
LOGS_DIR = ROOT_DIR / "logs"
REPORT_FILE = DATA_DIR / "supervisor_test_report.json"

ENGINES = [
    ("main.py", LOGS_DIR / "main.log"),
    ("rolling_window_engine.py", LOGS_DIR / "rolling_window_engine.log"),
    ("aligned_candle_engine.py", LOGS_DIR / "aligned_candle_engine.log"),
    ("sync_integrity_engine.py", LOGS_DIR / "sync_integrity_engine.log"),
]

OUTPUT_FILES = {
    "one_second_combined_dna_rows": DATA_DIR / "one_second_combined_dna.jsonl",
    "rolling_3s_rows": DATA_DIR / "rolling_3s_dna.jsonl",
    "rolling_5s_rows": DATA_DIR / "rolling_5s_dna.jsonl",
    "rolling_15s_rows": DATA_DIR / "rolling_15s_dna.jsonl",
    "aligned_1m_rows": DATA_DIR / "aligned_1m_candle_dna.jsonl",
    "gap_events_rows": DATA_DIR / "gap_events.jsonl",
    "data_quality_rows": DATA_DIR / "data_quality.jsonl",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NurtacCoreEngineGpt integration test supervisor")
    parser.add_argument(
        "--duration",
        type=int,
        default=600,
        help="Seconds to run after all engines have started. Default: 600",
    )
    return parser.parse_args()


def prepare_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def clean_jsonl_data() -> None:
    for path in DATA_DIR.glob("*.jsonl"):
        path.unlink()


def count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def collect_counts() -> dict[str, int | bool]:
    return {
        "one_second_combined_dna_rows": count_rows(OUTPUT_FILES["one_second_combined_dna_rows"]),
        "rolling_3s_rows": count_rows(OUTPUT_FILES["rolling_3s_rows"]),
        "rolling_5s_rows": count_rows(OUTPUT_FILES["rolling_5s_rows"]),
        "rolling_15s_rows": count_rows(OUTPUT_FILES["rolling_15s_rows"]),
        "aligned_1m_rows": count_rows(OUTPUT_FILES["aligned_1m_rows"]),
        "system_health_exists": (DATA_DIR / "system_health.json").exists(),
        "gap_events_rows": count_rows(OUTPUT_FILES["gap_events_rows"]),
        "data_quality_rows": count_rows(OUTPUT_FILES["data_quality_rows"]),
    }


def print_status(elapsed: int) -> None:
    counts = collect_counts()
    print(
        "[SUPERVISOR] "
        f"elapsed={elapsed}s "
        f"layer0_rows={counts['one_second_combined_dna_rows']} "
        f"rolling3={counts['rolling_3s_rows']} "
        f"rolling5={counts['rolling_5s_rows']} "
        f"rolling15={counts['rolling_15s_rows']} "
        f"aligned1m={counts['aligned_1m_rows']} "
        f"gaps={counts['gap_events_rows']}",
        flush=True,
    )


def start_engine(engine_file: str, log_file: Path) -> tuple[subprocess.Popen, Any]:
    log_handle = log_file.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, engine_file],
        cwd=ROOT_DIR,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, log_handle


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def build_report(
    duration: int,
    process_info: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    counts = collect_counts()
    validation = {
        "layer0_rows_gt_0": counts["one_second_combined_dna_rows"] > 0,
        "rolling_rows_gt_0": (
            counts["rolling_3s_rows"] > 0
            and counts["rolling_5s_rows"] > 0
            and counts["rolling_15s_rows"] > 0
        ),
        "aligned_1m_rows_gt_0": counts["aligned_1m_rows"] > 0,
        "sync_health_exists": bool(counts["system_health_exists"]),
    }
    validation["test_passed"] = all(validation.values())

    return {
        "duration_seconds": duration,
        "processes": process_info,
        "output_files": counts,
        "validation": validation,
    }


def write_report(report: dict[str, Any]) -> None:
    REPORT_FILE.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    prepare_directories()
    clean_jsonl_data()

    processes: list[tuple[str, subprocess.Popen, Any]] = []
    process_info: dict[str, dict[str, Any]] = {}

    try:
        for index, (engine_file, log_file) in enumerate(ENGINES):
            process, log_handle = start_engine(engine_file, log_file)
            processes.append((engine_file, process, log_handle))
            process_info[engine_file] = {
                "started": True,
                "exit_code": None,
                "log_file": str(log_file.relative_to(ROOT_DIR)).replace("\\", "/"),
            }
            if index < len(ENGINES) - 1:
                time.sleep(5)

        start_time = time.monotonic()
        next_status_at = 30
        while True:
            elapsed = int(time.monotonic() - start_time)
            if elapsed >= args.duration:
                break
            if elapsed >= next_status_at:
                print_status(elapsed)
                next_status_at += 30
            time.sleep(1)

    finally:
        for engine_file, process, _log_handle in reversed(processes):
            stop_process(process)
            process_info[engine_file]["exit_code"] = process.returncode

        for _engine_file, _process, log_handle in processes:
            log_handle.close()

    report = build_report(args.duration, process_info)
    write_report(report)

    print("SUPERVISOR TEST COMPLETE", flush=True)
    print(f"test_passed={str(report['validation']['test_passed']).lower()}", flush=True)
    print(r"report=data\supervisor_test_report.json", flush=True)


if __name__ == "__main__":
    main()
