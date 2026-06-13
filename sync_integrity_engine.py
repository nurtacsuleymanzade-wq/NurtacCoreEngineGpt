import asyncio
import json
import time
from pathlib import Path
from typing import Any


DATA_DIR = Path("data")

LAYER_0_FILE = DATA_DIR / "one_second_combined_dna.jsonl"
LAYER_1_FILES = {
    "3S": {
        "path": DATA_DIR / "rolling_3s_dna.jsonl",
        "expected_source_count": 3,
    },
    "5S": {
        "path": DATA_DIR / "rolling_5s_dna.jsonl",
        "expected_source_count": 5,
    },
    "15S": {
        "path": DATA_DIR / "rolling_15s_dna.jsonl",
        "expected_source_count": 15,
    },
}
LAYER_2_FILE = DATA_DIR / "aligned_1m_candle_dna.jsonl"

SYSTEM_HEALTH_FILE = DATA_DIR / "system_health.json"
GAP_EVENTS_FILE = DATA_DIR / "gap_events.jsonl"
DATA_QUALITY_FILE = DATA_DIR / "data_quality.jsonl"

POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10
ONE_SECOND_MS = 1000
ONE_MINUTE_MS = 60_000


class JsonlAppendWriter:
    def __init__(self, path: Path) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.handle = path.open("a", encoding="utf-8")

    def write(self, payload: dict[str, Any]) -> None:
        self.handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.handle.flush()

    def close(self) -> None:
        self.handle.close()


class SyncIntegrityEngine:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.gap_writer = JsonlAppendWriter(GAP_EVENTS_FILE)
        self.quality_writer = JsonlAppendWriter(DATA_QUALITY_FILE)
        self.layer_rows = {
            "layer_0": 0,
            "layer_1": 0,
            "layer_2": 0,
        }
        self.last_window_ts = {
            "layer_0": None,
            "layer_1": None,
            "layer_2": None,
        }
        self.layer_0_previous_ts: int | None = None
        self.gap_count = 0
        self.seen_quality_buckets: set[int] = set()

    def close(self) -> None:
        self.gap_writer.close()
        self.quality_writer.close()

    def process_layer_0(self, payload: dict[str, Any]) -> None:
        window_start_ts = int(payload["window_start_ts"])
        self.layer_rows["layer_0"] += 1
        self.last_window_ts["layer_0"] = window_start_ts

        if self.layer_0_previous_ts is not None:
            expected_next = self.layer_0_previous_ts + ONE_SECOND_MS
            if window_start_ts != expected_next:
                missing_count = max(0, (window_start_ts - expected_next) // ONE_SECOND_MS)
                gap_event = {
                    "layer": "Layer-0",
                    "gap_start": expected_next,
                    "gap_end": window_start_ts - ONE_SECOND_MS,
                    "missing_count": missing_count,
                }
                self.gap_writer.write(gap_event)
                self.gap_count += 1

        self.layer_0_previous_ts = window_start_ts
        self.write_system_health()

    def process_layer_1(self, timeframe: str, payload: dict[str, Any]) -> None:
        expected = int(LAYER_1_FILES[timeframe]["expected_source_count"])
        actual = int(payload.get("source_1s_count", -1))
        window_start_ts = int(payload["window_start_ts"])
        self.layer_rows["layer_1"] += 1
        self.last_window_ts["layer_1"] = window_start_ts

        if actual != expected:
            self.quality_writer.write(
                {
                    "layer": "Layer-1",
                    "bucket_ts": window_start_ts,
                    "timeframe": timeframe,
                    "expected_source_count": expected,
                    "actual_source_count": actual,
                    "quality_state": "invalid",
                }
            )

        self.write_system_health()

    def process_layer_2(self, payload: dict[str, Any]) -> None:
        bucket_ts = int(payload["window_start_ts"])
        self.layer_rows["layer_2"] += 1
        self.last_window_ts["layer_2"] = bucket_ts

        if bucket_ts in self.seen_quality_buckets:
            self.write_system_health()
            return
        self.seen_quality_buckets.add(bucket_ts)

        source_refs = payload.get("source_refs", {}).get("source_window_start_ts", [])
        actual_source_count = len(source_refs)
        expected_source_count = 60
        missing_source_count = max(0, expected_source_count - actual_source_count)
        coverage_ratio = actual_source_count / expected_source_count
        if coverage_ratio == 1.0:
            quality_state = "complete"
        elif coverage_ratio > 0:
            quality_state = "partial"
        else:
            quality_state = "empty"

        self.quality_writer.write(
            {
                "bucket_ts": bucket_ts,
                "timeframe": "1M",
                "expected_source_count": expected_source_count,
                "actual_source_count": actual_source_count,
                "missing_source_count": missing_source_count,
                "coverage_ratio": coverage_ratio,
                "quality_state": quality_state,
            }
        )
        self.write_system_health()

    def write_system_health(self) -> None:
        payload = {
            "layer_0": {
                "status": self._status("layer_0"),
                "last_window_ts": self.last_window_ts["layer_0"],
                "lag_seconds": self._lag_seconds("layer_0"),
            },
            "layer_1": {
                "status": self._status("layer_1"),
                "last_window_ts": self.last_window_ts["layer_1"],
            },
            "layer_2": {
                "status": self._status("layer_2"),
                "last_window_ts": self.last_window_ts["layer_2"],
            },
            "gap_count": self.gap_count,
        }
        SYSTEM_HEALTH_FILE.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def heartbeat(self) -> None:
        self.write_system_health()
        print("Layer-S alive", flush=True)
        print(f"Layer0 rows={self.layer_rows['layer_0']}", flush=True)
        print(f"Layer1 rows={self.layer_rows['layer_1']}", flush=True)
        print(f"Layer2 rows={self.layer_rows['layer_2']}", flush=True)
        print(f"gap_count={self.gap_count}", flush=True)

    def _status(self, layer: str) -> str:
        return "alive" if self.last_window_ts[layer] is not None else "missing"

    def _lag_seconds(self, layer: str) -> float | None:
        last_ts = self.last_window_ts[layer]
        if last_ts is None:
            return None
        return max(0.0, (time.time() * 1000 - last_ts) / 1000)


async def follow_jsonl(
    path: Path,
    callback,
    missing_label: str,
) -> None:
    handle = None
    try:
        while True:
            if handle is None:
                if not path.exists():
                    print(f"{missing_label} missing: {path}", flush=True)
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue
                handle = path.open("r", encoding="utf-8")

            line = handle.readline()
            if not line:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            stripped = line.strip()
            if not stripped:
                continue

            try:
                callback(json.loads(stripped))
            except json.JSONDecodeError as exc:
                print(f"Invalid JSON ignored in {path}: {exc}", flush=True)
            except KeyError as exc:
                print(f"Missing field reported in {path}: {exc}", flush=True)
            except Exception as exc:
                print(f"Integrity processing error in {path}: {exc}", flush=True)
    finally:
        if handle is not None:
            handle.close()


async def heartbeat_loop(engine: SyncIntegrityEngine) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        engine.heartbeat()


async def run() -> None:
    engine = SyncIntegrityEngine()
    tasks = [
        asyncio.create_task(
            follow_jsonl(
                LAYER_0_FILE,
                engine.process_layer_0,
                "Layer-0 input",
            )
        ),
        asyncio.create_task(
            follow_jsonl(
                LAYER_2_FILE,
                engine.process_layer_2,
                "Layer-2 input",
            )
        ),
        asyncio.create_task(heartbeat_loop(engine)),
    ]
    for timeframe, config in LAYER_1_FILES.items():
        tasks.append(
            asyncio.create_task(
                follow_jsonl(
                    config["path"],
                    lambda payload, tf=timeframe: engine.process_layer_1(tf, payload),
                    f"Layer-1 {timeframe} input",
                )
            )
        )

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        engine.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
