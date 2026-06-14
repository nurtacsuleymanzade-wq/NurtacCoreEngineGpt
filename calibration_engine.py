"""Layer-6 outcome observation engine.

This engine measures historical outcomes for detector events. It does not
produce scores, thresholds, confidence, signals, setups, or trade decisions.
"""

import bisect
import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"

EVIDENCE_INPUT_FILE = DATA_DIR / "evidence_inbox.jsonl"
DETECTOR_INPUT_FILE = DATA_DIR / "detector_events.jsonl"
PRICE_INPUT_FILE = DATA_DIR / "one_second_combined_dna.jsonl"
CONTEXT_INPUT_FILE = DATA_DIR / "context_dna.jsonl"

OBSERVATIONS_FILE = DATA_DIR / "calibration_observations.jsonl"
PROFILES_FILE = DATA_DIR / "calibration_profiles.json"
HEALTH_FILE = DATA_DIR / "calibration_health.json"

HORIZONS = [30000, 60000, 180000, 300000]
HORIZON_LABELS = {
    30000: "30s",
    60000: "60s",
    180000: "180s",
    300000: "300s",
}
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0
PROFILE_INTERVAL_SECONDS = 30.0

NULL_SCORES = {
    "confidence": None,
    "strength_score": None,
    "edge_score": None,
    "threshold": None,
}


class CalibrationObservationEngine:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.processed_event_ids: set[str] = set()
        self.seen_event_ids: set[str] = set()
        self.open_observations: dict[str, dict[str, Any]] = {}
        self.price_index: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        self.price_timestamps: dict[str, list[int]] = defaultdict(list)
        self.detector_event_index: dict[str, dict[str, Any]] = {}
        self.profile_groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self.input_events_processed = 0
        self.prices_indexed = 0
        self.completed_observations = 0
        self.profiles_written = 0
        self.last_event_ts = 0
        self.last_price_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_heartbeat = time.monotonic()
        self.last_profile_write = time.monotonic()
        self.observation_handle = OBSERVATIONS_FILE.open("a", encoding="utf-8")
        self.load_existing_observations()
        self.refresh_missing_inputs()
        self.write_health()

    def close(self) -> None:
        self.write_profiles()
        self.write_health()
        self.observation_handle.close()

    def load_existing_observations(self) -> None:
        for row in read_jsonl(OBSERVATIONS_FILE):
            event_id = row.get("event_id")
            if event_id:
                event_id = str(event_id)
                self.processed_event_ids.add(event_id)
                self.seen_event_ids.add(event_id)
            if row.get("record_type") == "calibration_observation":
                self.update_profile(row)

    def refresh_missing_inputs(self) -> None:
        for path in (
            EVIDENCE_INPUT_FILE,
            DETECTOR_INPUT_FILE,
            PRICE_INPUT_FILE,
            CONTEXT_INPUT_FILE,
        ):
            label = relative_label(path)
            if path.exists():
                self.missing_inputs.discard(label)
            else:
                self.missing_inputs.add(label)

    def index_price(self, row: dict[str, Any]) -> None:
        symbol = str(row.get("symbol", ""))
        window_start_ts = safe_int(row.get("window_start_ts"))
        window_end_ts = safe_int(row.get("window_end_ts"))
        close_price = extract_close_price(row)
        if not symbol or window_start_ts is None or close_price is None:
            self.warnings.add("invalid_price_row")
            return
        symbol_prices = self.price_index[symbol]
        if window_start_ts not in symbol_prices:
            bisect.insort(self.price_timestamps[symbol], window_start_ts)
            self.prices_indexed += 1
        symbol_prices[window_start_ts] = {
            "symbol": symbol,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "close_price": close_price,
        }
        self.last_price_ts = max(self.last_price_ts, window_start_ts)
        self.measure_open_observations(symbol)

    def register_detector_event(self, row: dict[str, Any]) -> None:
        event_id = source_event_id(row)
        if event_id:
            self.detector_event_index[event_id] = row

    def process_detector_event(self, row: dict[str, Any]) -> None:
        self.register_detector_event(row)
        self.process_event(row)

    def process_event(self, row: dict[str, Any]) -> None:
        self.input_events_processed += 1
        event = normalize_event(row, self.detector_event_index)
        if event is None:
            self.warnings.add("invalid_event_row")
            return
        event_id = event["event_id"]
        if event_id in self.seen_event_ids or event_id in self.processed_event_ids:
            return
        self.seen_event_ids.add(event_id)
        self.last_event_ts = max(self.last_event_ts, event["window_start_ts"])

        reference = self.find_reference_price(event["symbol"], event["window_start_ts"])
        if reference is None:
            self.warnings.add("missing_reference_price")
            return
        observation = {
            "event_id": event_id,
            "symbol": event["symbol"],
            "timeframe": event["timeframe"],
            "event_type": event["event_type"],
            "side": event["side"],
            "event_window_start_ts": event["window_start_ts"],
            "event_window_end_ts": event["window_end_ts"],
            "reference_price": reference["close_price"],
            "reference_price_ts": reference["window_start_ts"],
            "horizons_pending": list(HORIZONS),
            "future_prices": {},
            "source_event": event["source_event"],
            "data_quality": event["data_quality"],
        }
        self.open_observations[event_id] = observation
        self.measure_observation(observation)

    def find_reference_price(self, symbol: str, event_ts: int) -> dict[str, Any] | None:
        timestamps = self.price_timestamps.get(symbol, [])
        index = bisect.bisect_right(timestamps, event_ts) - 1
        if index < 0:
            return None
        reference_ts = timestamps[index]
        return self.price_index[symbol][reference_ts]

    def find_future_price(self, symbol: str, target_ts: int) -> dict[str, Any] | None:
        timestamps = self.price_timestamps.get(symbol, [])
        index = bisect.bisect_left(timestamps, target_ts)
        if index >= len(timestamps):
            return None
        future_ts = timestamps[index]
        return self.price_index[symbol][future_ts]

    def measure_open_observations(self, symbol: str) -> None:
        for observation in list(self.open_observations.values()):
            if observation["symbol"] == symbol:
                self.measure_observation(observation)

    def measure_observation(self, observation: dict[str, Any]) -> None:
        for horizon_ms in list(observation["horizons_pending"]):
            target_ts = observation["event_window_start_ts"] + horizon_ms
            future = self.find_future_price(observation["symbol"], target_ts)
            if future is None or future["window_start_ts"] < target_ts:
                continue
            raw_return = (future["close_price"] - observation["reference_price"]) / observation["reference_price"]
            adjusted = side_adjusted_return(raw_return, observation["side"])
            observation["future_prices"][horizon_ms] = {
                "future_price": future["close_price"],
                "future_price_ts": future["window_start_ts"],
                "future_price_delay_ms": future["window_start_ts"] - target_ts,
                "raw_return": raw_return,
                "side_adjusted_return": adjusted,
                "directional_result": directional_result(adjusted),
            }
            observation["horizons_pending"].remove(horizon_ms)
        if not observation["horizons_pending"]:
            self.complete_observation(observation)

    def complete_observation(self, observation: dict[str, Any]) -> None:
        event_id = observation["event_id"]
        if event_id in self.processed_event_ids:
            self.open_observations.pop(event_id, None)
            return
        outcomes = {
            HORIZON_LABELS[horizon]: observation["future_prices"][horizon]
            for horizon in HORIZONS
        }
        payload = {
            "layer": "Layer-6",
            "engine": "CalibrationObservationEngine",
            "record_type": "calibration_observation",
            "calibration_status": "observed_not_scored",
            "event_id": event_id,
            "symbol": observation["symbol"],
            "timeframe": observation["timeframe"],
            "event_type": observation["event_type"],
            "side": observation["side"],
            "event_window_start_ts": observation["event_window_start_ts"],
            "event_window_end_ts": observation["event_window_end_ts"],
            "reference": {
                "price": observation["reference_price"],
                "price_ts": observation["reference_price_ts"],
            },
            "outcomes": outcomes,
            "source_event": observation["source_event"],
            "data_quality": observation["data_quality"],
            "scores": dict(NULL_SCORES),
            "validation": {
                "reference_price_valid": True,
                "all_horizons_measured": True,
                "errors": [],
            },
        }
        self.observation_handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.observation_handle.flush()
        self.processed_event_ids.add(event_id)
        self.open_observations.pop(event_id, None)
        self.completed_observations += 1
        self.update_profile(payload)

    def update_profile(self, observation: dict[str, Any]) -> None:
        key = (
            str(observation.get("symbol", "unknown")),
            str(observation.get("timeframe", "unknown")),
            str(observation.get("event_type", "unknown")),
            str(observation.get("side", "unknown")),
        )
        group = self.profile_groups.get(key)
        if group is None:
            group = {
                "symbol": key[0],
                "timeframe": key[1],
                "event_type": key[2],
                "side": key[3],
                "sample_count": 0,
                "horizons": {
                    label: {
                        "favorable_count": 0,
                        "unfavorable_count": 0,
                        "flat_count": 0,
                        "unknown_count": 0,
                        "raw_returns": [],
                        "adjusted_returns": [],
                    }
                    for label in HORIZON_LABELS.values()
                },
            }
            self.profile_groups[key] = group
        group["sample_count"] += 1
        for label in HORIZON_LABELS.values():
            outcome = observation.get("outcomes", {}).get(label, {})
            horizon = group["horizons"][label]
            result = outcome.get("directional_result", "unknown")
            count_field = f"{result}_count" if result in ("favorable", "unfavorable", "flat") else "unknown_count"
            horizon[count_field] += 1
            raw_return = outcome.get("raw_return")
            adjusted_return = outcome.get("side_adjusted_return")
            if isinstance(raw_return, (int, float)):
                horizon["raw_returns"].append(float(raw_return))
            if isinstance(adjusted_return, (int, float)):
                horizon["adjusted_returns"].append(float(adjusted_return))

    def write_profiles(self) -> None:
        groups = []
        for key in sorted(self.profile_groups):
            source = self.profile_groups[key]
            horizons = {}
            for label, values in source["horizons"].items():
                horizons[label] = {
                    "favorable_count": values["favorable_count"],
                    "unfavorable_count": values["unfavorable_count"],
                    "flat_count": values["flat_count"],
                    "unknown_count": values["unknown_count"],
                    "avg_raw_return": average(values["raw_returns"]),
                    "avg_side_adjusted_return": average(values["adjusted_returns"]),
                }
            groups.append(
                {
                    "symbol": source["symbol"],
                    "timeframe": source["timeframe"],
                    "event_type": source["event_type"],
                    "side": source["side"],
                    "sample_count": source["sample_count"],
                    "horizons": horizons,
                    "scores": dict(NULL_SCORES),
                }
            )
        payload = {
            "layer": "Layer-6",
            "engine": "CalibrationObservationEngine",
            "record_type": "calibration_profile_summary",
            "calibration_status": "observed_not_scored",
            "generated_at": time.time(),
            "groups": groups,
            "scores": dict(NULL_SCORES),
        }
        PROFILES_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        self.profiles_written += 1
        self.last_profile_write = time.monotonic()

    def tick(self) -> None:
        now = time.monotonic()
        if now - self.last_profile_write >= PROFILE_INTERVAL_SECONDS:
            self.write_profiles()
        if now - self.last_heartbeat >= HEARTBEAT_SECONDS:
            self.heartbeat()
            self.last_heartbeat = now

    def write_health(self) -> None:
        self.refresh_missing_inputs()
        payload = {
            "status": "alive",
            "input_events_processed": self.input_events_processed,
            "prices_indexed": self.prices_indexed,
            "open_observations": len(self.open_observations),
            "completed_observations": self.completed_observations,
            "profiles_written": self.profiles_written,
            "last_event_ts": self.last_event_ts,
            "last_price_ts": self.last_price_ts,
            "missing_inputs": sorted(self.missing_inputs),
            "warnings": sorted(self.warnings),
        }
        HEALTH_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def heartbeat(self) -> None:
        self.write_health()
        print("Calibration Engine alive", flush=True)
        print(f"input_events_processed={self.input_events_processed}", flush=True)
        print(f"prices_indexed={self.prices_indexed}", flush=True)
        print(f"open_observations={len(self.open_observations)}", flush=True)
        print(f"completed_observations={self.completed_observations}", flush=True)
        print(f"profiles_written={self.profiles_written}", flush=True)
        print(f"last_event_ts={self.last_event_ts}", flush=True)
        print(f"last_price_ts={self.last_price_ts}", flush=True)


def extract_close_price(row: dict[str, Any]) -> float | None:
    candidates = [
        nested_value(row, ("close", "price")),
        nested_value(row, ("ohlc", "close", "price")),
        nested_value(row, ("candle_dna", "close", "price")),
        row.get("close"),
        row.get("price"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            candidate = candidate.get("price")
        try:
            price = float(candidate)
        except (TypeError, ValueError, OverflowError):
            continue
        if price > 0:
            return price
    return None


def normalize_event(
    row: dict[str, Any], detector_index: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    detector_event_id = source_event_id(row)
    detector_row = detector_index.get(detector_event_id or "", {})
    merged = dict(detector_row)
    merged.update(row)
    symbol = str(merged.get("symbol", ""))
    timeframe = str(merged.get("timeframe", ""))
    window_start_ts = safe_int(merged.get("window_start_ts"))
    event_type = str(merged.get("event_type", ""))
    side = str(merged.get("side", detector_row.get("side", "unknown")))
    if side not in ("buy", "sell", "neutral", "unknown"):
        side = "unknown"
    detector_name = str(
        merged.get("detector_name")
        or merged.get("contract_name")
        or detector_row.get("contract_name")
        or event_type
    )
    if not symbol or not timeframe or window_start_ts is None or not event_type:
        return None
    event_id = detector_event_id or deterministic_event_id(
        symbol, timeframe, window_start_ts, event_type, side, detector_name
    )
    return {
        "event_id": event_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "window_start_ts": window_start_ts,
        "window_end_ts": safe_int(merged.get("window_end_ts")),
        "event_type": event_type,
        "side": side,
        "detector_name": detector_name,
        "calibration_status": merged.get("calibration_status", "unknown"),
        "source_refs": merged.get("source_refs", {}),
        "data_quality": merged.get("data_quality", {"quality_state": "unknown", "warning": "source_data_quality_missing"}),
        "source_event": {
            "event_id": event_id,
            "detector_name": detector_name,
            "event_type": event_type,
            "calibration_status": merged.get("calibration_status", "unknown"),
            "source_refs": merged.get("source_refs", {}),
        },
    }


def source_event_id(row: dict[str, Any]) -> str | None:
    value = row.get("event_id") or row.get("detector_event_id")
    return str(value) if value else None


def deterministic_event_id(
    symbol: str,
    timeframe: str,
    window_start_ts: int,
    event_type: str,
    side: str,
    detector_name: str,
) -> str:
    raw = f"{symbol}{timeframe}{window_start_ts}{event_type}{side}{detector_name}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def side_adjusted_return(raw_return: float, side: str) -> float | None:
    if side == "buy":
        return raw_return
    if side == "sell":
        return -raw_return
    return None


def directional_result(adjusted_return: float | None) -> str:
    if adjusted_return is None:
        return "unknown"
    if adjusted_return > 0:
        return "favorable"
    if adjusted_return < 0:
        return "unfavorable"
    return "flat"


def average(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def nested_value(row: dict[str, Any], path: tuple[str, ...]) -> Any:
    value: Any = row
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def relative_label(path: Path) -> str:
    return str(path.relative_to(ROOT_DIR)).replace("\\", "/")


def read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def open_tail(path: Path):
    return path.open("r", encoding="utf-8", errors="replace") if path.exists() else None


def consume_available(handle, callback) -> int:
    if handle is None:
        return 0
    consumed = 0
    while True:
        line = handle.readline()
        if not line:
            break
        stripped = line.strip()
        if not stripped:
            continue
        try:
            row = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            callback(row)
            consumed += 1
    return consumed


def run() -> None:
    engine = CalibrationObservationEngine()
    price_handle = None
    detector_handle = None
    evidence_handle = None
    try:
        price_handle = open_tail(PRICE_INPUT_FILE)
        consume_available(price_handle, engine.index_price)

        detector_handle = open_tail(DETECTOR_INPUT_FILE)
        consume_available(detector_handle, engine.register_detector_event)

        evidence_handle = open_tail(EVIDENCE_INPUT_FILE)
        consume_available(evidence_handle, engine.process_event)

        if detector_handle is not None:
            detector_handle.seek(0)
            consume_available(detector_handle, engine.process_detector_event)

        while True:
            if price_handle is None and PRICE_INPUT_FILE.exists():
                price_handle = open_tail(PRICE_INPUT_FILE)
            if detector_handle is None and DETECTOR_INPUT_FILE.exists():
                detector_handle = open_tail(DETECTOR_INPUT_FILE)
            if evidence_handle is None and EVIDENCE_INPUT_FILE.exists():
                evidence_handle = open_tail(EVIDENCE_INPUT_FILE)

            activity = 0
            activity += consume_available(price_handle, engine.index_price)
            activity += consume_available(detector_handle, engine.process_detector_event)
            activity += consume_available(evidence_handle, engine.process_event)
            engine.tick()
            if activity == 0:
                time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in (price_handle, detector_handle, evidence_handle):
            if handle is not None:
                handle.close()
        engine.close()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
