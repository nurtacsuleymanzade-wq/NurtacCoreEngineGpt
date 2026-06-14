"""Layer-6B historical forward-outcome measurement engine.

The engine records observations and descriptive profiles only. It never emits
scores, probabilities, thresholds, setups, signals, or trade decisions.
"""

import bisect
import hashlib
import json
import sqlite3
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from calibration_contracts import FORWARD_HORIZONS, validate_calibration_contracts


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
PRICE_FILE = DATA_DIR / "one_second_combined_dna.jsonl"
EVENT_FILES = {
    "detector_events": DATA_DIR / "detector_events.jsonl",
    "evidence_packets": DATA_DIR / "evidence_packets.jsonl",
    "structure_events": DATA_DIR / "structure_events.jsonl",
    "observer_events": DATA_DIR / "observer_events.jsonl",
}
CONTEXT_FILES = {
    "context_dna": DATA_DIR / "context_dna.jsonl",
    "smart_money_dna": DATA_DIR / "smart_money_dna.jsonl",
    "data_quality": DATA_DIR / "data_quality.jsonl",
}
OBSERVATIONS_FILE = DATA_DIR / "historical_outcome_observations.jsonl"
OPEN_FILE = DATA_DIR / "historical_outcome_open_positions.json"
PROFILES_FILE = DATA_DIR / "calibration_profiles.json"
HEALTH_FILE = DATA_DIR / "historical_outcome_health.json"
ERRORS_FILE = DATA_DIR / "historical_outcome_errors.jsonl"
PRICE_DB_FILE = ROOT_DIR / "runtime" / "tmp" / "historical_outcome_prices.sqlite3"
HORIZONS = list(FORWARD_HORIZONS.values())
HORIZON_LABELS = {value: key for key, value in FORWARD_HORIZONS.items()}
PRICE_RETENTION_MS = 4 * 60 * 60 * 1000
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0
STATE_INTERVAL_SECONDS = 10.0
PROFILE_INTERVAL_SECONDS = 60.0
NULL_SCORES = {
    "confidence": None,
    "strength_score": None,
    "edge_score": None,
    "probability_score": None,
    "threshold": None,
    "decision_score": None,
}

WindowKey = tuple[str, str, int]


class HistoricalOutcomeEngine:
    def __init__(self) -> None:
        errors = validate_calibration_contracts()
        if errors:
            raise RuntimeError("Calibration registry validation failed: " + "; ".join(errors))
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        PRICE_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.price_db = sqlite3.connect(PRICE_DB_FILE)
        self.price_db.execute("PRAGMA journal_mode=WAL")
        self.price_db.execute("CREATE TABLE IF NOT EXISTS prices (symbol TEXT NOT NULL, ts INTEGER NOT NULL, end_ts INTEGER, price REAL NOT NULL, PRIMARY KEY(symbol, ts))")
        self.price_db.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol_ts ON prices(symbol, ts)")
        self.price_index: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        self.price_timestamps: dict[str, list[int]] = defaultdict(list)
        self.processed_event_ids = load_completed_ids()
        self.open_observations = load_open_observations()
        self.seen_event_ids = set(self.processed_event_ids) | set(self.open_observations)
        self.window_components: dict[WindowKey, set[str]] = defaultdict(set)
        self.context_index: dict[WindowKey, dict[str, Any]] = {}
        self.structure_bias_index: dict[WindowKey, str] = {}
        self.quality_index: dict[WindowKey, dict[str, Any]] = {}
        self.path_excursion_cache: dict[tuple[str, int, int, str], tuple[float | None, float | None]] = {}
        self.profile_groups: dict[tuple[str, ...], dict[str, Any]] = {}
        self.prices_indexed = 0
        self.input_events_processed = {name: 0 for name in (*EVENT_FILES, "context_dna", "smart_money_dna")}
        self.completed_observations = 0
        self.profiles_written = 0
        self.last_price_ts = 0
        self.last_event_ts = max(
            (safe_int(item.get("event_window_start_ts")) or 0 for item in self.open_observations.values()),
            default=0,
        )
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.errors: list[str] = []
        self.last_heartbeat = time.monotonic()
        self.last_state_write = time.monotonic()
        self.last_profile_write = time.monotonic()
        self.loading_backlog = True
        self.observation_handle = OBSERVATIONS_FILE.open("a", encoding="utf-8")
        self.error_handle = ERRORS_FILE.open("a", encoding="utf-8")
        self.load_profiles_from_observations()
        self.refresh_missing_inputs()
        self.write_open_state()
        self.write_profiles()
        self.write_health()

    def close(self) -> None:
        self.write_open_state()
        self.write_profiles()
        self.write_health()
        self.observation_handle.close()
        self.error_handle.close()
        self.price_db.commit()
        self.price_db.close()

    def refresh_missing_inputs(self) -> None:
        for path in (PRICE_FILE, *EVENT_FILES.values(), *CONTEXT_FILES.values()):
            label = relative_label(path)
            if path.exists(): self.missing_inputs.discard(label)
            else: self.missing_inputs.add(label)

    def record_error(self, category: str, source: str, detail: str) -> None:
        message = f"{category}:{source}:{detail}"
        self.errors.append(message)
        self.errors = self.errors[-100:]
        payload = {"timestamp": time.time(), "category": category, "source": source, "detail": detail}
        self.error_handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.error_handle.flush()

    def index_price(self, row: dict[str, Any]) -> None:
        symbol = str(row.get("symbol") or "")
        timestamp = safe_int(row.get("window_start_ts"))
        price = extract_close_price(row)
        if not symbol or timestamp is None or price is None:
            self.warnings.add("invalid_price_row")
            return
        values = self.price_index[symbol]
        if timestamp not in values:
            bisect.insort(self.price_timestamps[symbol], timestamp)
            self.prices_indexed += 1
        values[timestamp] = {"symbol": symbol, "window_start_ts": timestamp, "window_end_ts": safe_int(row.get("window_end_ts")), "close_price": price}
        self.price_db.execute(
            "INSERT OR REPLACE INTO prices(symbol, ts, end_ts, price) VALUES (?, ?, ?, ?)",
            (symbol, timestamp, safe_int(row.get("window_end_ts")), price),
        )
        self.last_price_ts = max(self.last_price_ts, timestamp)
        if self.prices_indexed % 100 == 0:
            self.price_db.commit()
        self.prune_prices(symbol)
        if not self.loading_backlog:
            self.measure_open_for_symbol(symbol)

    def prune_prices(self, symbol: str) -> None:
        timestamps = self.price_timestamps[symbol]
        cutoff = self.last_price_ts - PRICE_RETENTION_MS
        split = bisect.bisect_left(timestamps, cutoff)
        if split <= 0: return
        for timestamp in timestamps[:split]:
            self.price_index[symbol].pop(timestamp, None)
        del timestamps[:split]

    def index_context(self, source: str, row: dict[str, Any]) -> None:
        if source in self.input_events_processed:
            self.input_events_processed[source] += 1
        timestamp_field = "source_window_ts" if source == "context_dna" else "window_start_ts"
        key = window_key(row, timestamp_field)
        if key is None: return
        if source == "context_dna": self.context_index[key] = row
        elif source == "smart_money_dna":
            swing = row.get("swing_state") if isinstance(row.get("swing_state"), dict) else {}
            self.structure_bias_index[key] = str(swing.get("structure_bias", "unknown"))
        else: self.quality_index[key] = row

    def preindex_event(self, source_name: str, row: dict[str, Any]) -> None:
        event = normalize_event(source_name, row)
        if event is None: return
        key = (event["symbol"], event["timeframe"], event["window_start_ts"])
        self.window_components[key].update(event["component_types"])

    def process_event(self, source_name: str, row: dict[str, Any]) -> None:
        self.input_events_processed[source_name] += 1
        event = normalize_event(source_name, row)
        if event is None:
            self.warnings.add(f"invalid_event:{source_name}")
            self.record_error("parse", source_name, "event normalization failed")
            return
        key = (event["symbol"], event["timeframe"], event["window_start_ts"])
        self.window_components[key].update(event["component_types"])
        event["pattern_components"] = self.pattern_components(event, key)
        self.open_event_observation(event)
        if source_name == "evidence_packets" and len(event["pattern_components"]) > 1:
            self.open_event_observation(make_composite_event(event))
        total_processed = sum(self.input_events_processed[name] for name in EVENT_FILES)
        if total_processed and total_processed % 1000 == 0:
            self.write_health()
            print(f"Historical Outcome backlog processed={total_processed}", flush=True)

    def pattern_components(self, event: dict[str, Any], key: WindowKey) -> list[str]:
        components = set(self.window_components.get(key, set()))
        components.add(event["event_type"])
        context = self.context_index.get(key, {})
        context_body = context.get("context") if isinstance(context.get("context"), dict) else {}
        for label, value in context_body.items():
            if isinstance(value, dict) and value.get("status"):
                components.add(f"context:{label}:{value['status']}")
        bias = self.structure_bias_index.get(key)
        if bias: components.add(f"structure_bias:{bias}")
        quality = event.get("data_quality") if isinstance(event.get("data_quality"), dict) else {}
        quality_state = quality.get("quality_state")
        if quality_state: components.add(f"data_quality:{quality_state}")
        return sorted(str(value) for value in components if value)

    def open_event_observation(self, event: dict[str, Any]) -> None:
        event_id = event["event_id"]
        if event_id in self.seen_event_ids: return
        self.seen_event_ids.add(event_id)
        self.last_event_ts = max(self.last_event_ts, event["window_start_ts"])
        reference = self.find_reference_price(event["symbol"], event["window_start_ts"])
        if reference is None:
            self.warnings.add("missing_reference_price")
            return
        components = event["pattern_components"]
        signature = pattern_signature(event["symbol"], event["timeframe"], components)
        pattern_key = f"{event['symbol']}|{event['timeframe']}|{'+'.join(components)}"
        observation_id = "outcome_" + hashlib.sha256(f"{event_id}|{signature}".encode()).hexdigest()[:24]
        observation = {
            "observation_id": observation_id, "event_id": event_id,
            "pattern_signature": signature, "pattern_key": pattern_key,
            "source": event["source"], "symbol": event["symbol"], "timeframe": event["timeframe"],
            "event_type": event["event_type"], "side": event["side"], "direction": event["direction"],
            "event_window_start_ts": event["window_start_ts"], "event_window_end_ts": event["window_end_ts"],
            "reference_price": reference["close_price"], "reference_price_ts": reference["window_start_ts"],
            "horizons_pending": list(HORIZONS), "outcomes": {}, "source_event": event["source_event"],
            "pattern_components": components, "data_quality": event["data_quality"],
        }
        self.open_observations[event_id] = observation
        self.measure_observation(observation)

    def find_reference_price(self, symbol: str, event_ts: int) -> dict[str, Any] | None:
        timestamps = self.price_timestamps.get(symbol, [])
        index = bisect.bisect_right(timestamps, event_ts) - 1
        if index >= 0: return self.price_index[symbol][timestamps[index]]
        row = self.price_db.execute(
            "SELECT ts, end_ts, price FROM prices WHERE symbol = ? AND ts <= ? ORDER BY ts DESC LIMIT 1",
            (symbol, event_ts),
        ).fetchone()
        return db_price(symbol, row)

    def find_future_price(self, symbol: str, target_ts: int) -> dict[str, Any] | None:
        if target_ts > self.last_price_ts:
            return None
        timestamps = self.price_timestamps.get(symbol, [])
        index = bisect.bisect_left(timestamps, target_ts)
        if index < len(timestamps): return self.price_index[symbol][timestamps[index]]
        row = self.price_db.execute(
            "SELECT ts, end_ts, price FROM prices WHERE symbol = ? AND ts >= ? ORDER BY ts ASC LIMIT 1",
            (symbol, target_ts),
        ).fetchone()
        return db_price(symbol, row)

    def path_prices(self, symbol: str, reference_ts: int, future_ts: int) -> list[float]:
        timestamps = self.price_timestamps.get(symbol, [])
        start = bisect.bisect_left(timestamps, reference_ts)
        end = bisect.bisect_right(timestamps, future_ts)
        values = [self.price_index[symbol][ts]["close_price"] for ts in timestamps[start:end]]
        if values and timestamps[start] <= reference_ts and timestamps[end - 1] >= future_ts:
            return values
        rows = self.price_db.execute(
            "SELECT price FROM prices WHERE symbol = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC",
            (symbol, reference_ts, future_ts),
        ).fetchall()
        return [float(row[0]) for row in rows]

    def path_excursions_for(
        self, symbol: str, reference_ts: int, future_ts: int, reference_price: float, side: str
    ) -> tuple[float | None, float | None]:
        if side not in ("buy", "sell"):
            return None, None
        row = self.price_db.execute(
            "SELECT MIN(price), MAX(price) FROM prices WHERE symbol = ? AND ts >= ? AND ts <= ?",
            (symbol, reference_ts, future_ts),
        ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            return None, None
        minimum, maximum = float(row[0]), float(row[1])
        if side == "buy":
            return (maximum - reference_price) / reference_price, (minimum - reference_price) / reference_price
        return (reference_price - minimum) / reference_price, (reference_price - maximum) / reference_price

    def measure_open_for_symbol(self, symbol: str) -> None:
        for observation in list(self.open_observations.values()):
            if observation["symbol"] == symbol: self.measure_observation(observation)

    def measure_observation(self, observation: dict[str, Any]) -> None:
        for horizon in list(observation["horizons_pending"]):
            target_ts = observation["event_window_start_ts"] + horizon
            future = self.find_future_price(observation["symbol"], target_ts)
            if future is None or future["window_start_ts"] < target_ts: continue
            raw_return = (future["close_price"] - observation["reference_price"]) / observation["reference_price"]
            adjusted = side_adjusted_return(raw_return, observation["side"])
            cache_key = (observation["symbol"], observation["reference_price_ts"], future["window_start_ts"], observation["side"])
            excursions = self.path_excursion_cache.get(cache_key)
            if excursions is None:
                excursions = self.path_excursions_for(
                    observation["symbol"], observation["reference_price_ts"],
                    future["window_start_ts"], observation["reference_price"], observation["side"],
                )
                self.path_excursion_cache[cache_key] = excursions
            favorable, adverse = excursions
            observation["outcomes"][str(horizon)] = {
                "future_price": future["close_price"], "future_price_ts": future["window_start_ts"],
                "future_price_delay_ms": future["window_start_ts"] - target_ts, "raw_return": raw_return,
                "side_adjusted_return": adjusted, "directional_result": directional_result(adjusted),
                "max_favorable_return_until_horizon": favorable, "max_adverse_return_until_horizon": adverse,
            }
            observation["horizons_pending"].remove(horizon)
        if not observation["horizons_pending"]: self.complete_observation(observation)

    def complete_observation(self, observation: dict[str, Any]) -> None:
        event_id = observation["event_id"]
        if event_id in self.processed_event_ids:
            self.open_observations.pop(event_id, None); return
        outcomes = {HORIZON_LABELS[h]: observation["outcomes"][str(h)] for h in HORIZONS}
        payload = {
            "layer": "Layer-6B", "engine": "HistoricalOutcomeEngine", "record_type": "historical_outcome_observation",
            "calibration_status": "observed_not_scored", "observation_id": observation["observation_id"],
            "event_id": event_id, "pattern_signature": observation["pattern_signature"], "pattern_key": observation["pattern_key"],
            "pattern_components": observation["pattern_components"], "source": observation["source"],
            "symbol": observation["symbol"], "timeframe": observation["timeframe"], "event_type": observation["event_type"],
            "side": observation["side"], "direction": observation["direction"],
            "event_window_start_ts": observation["event_window_start_ts"], "event_window_end_ts": observation["event_window_end_ts"],
            "reference": {"price": observation["reference_price"], "price_ts": observation["reference_price_ts"]},
            "outcomes": outcomes, "source_event": observation["source_event"], "data_quality": observation["data_quality"],
            "scores": dict(NULL_SCORES),
            "validation": {"reference_price_valid": True, "all_horizons_measured": True, "future_leakage_detected": False, "errors": []},
        }
        self.observation_handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.observation_handle.flush()
        self.processed_event_ids.add(event_id)
        self.open_observations.pop(event_id, None)
        self.completed_observations += 1
        self.update_profile(payload)

    def load_profiles_from_observations(self) -> None:
        for row in read_jsonl(OBSERVATIONS_FILE):
            self.last_event_ts = max(self.last_event_ts, safe_int(row.get("event_window_start_ts")) or 0)
            self.update_profile(row)

    def update_profile(self, row: dict[str, Any]) -> None:
        key = tuple(str(row.get(field, "unknown")) for field in ("symbol", "timeframe", "source", "event_type", "side", "direction", "pattern_signature"))
        group = self.profile_groups.setdefault(key, {
            "symbol": key[0], "timeframe": key[1], "source": key[2], "event_type": key[3], "side": key[4], "direction": key[5],
            "pattern_signature": key[6], "pattern_key": row.get("pattern_key", ""), "sample_count": 0,
            "horizons": {label: {"results": [], "raw": [], "adjusted": [], "favorable": [], "adverse": []} for label in FORWARD_HORIZONS},
        })
        group["sample_count"] += 1
        for label in FORWARD_HORIZONS:
            outcome = row.get("outcomes", {}).get(label, {})
            values = group["horizons"][label]
            values["results"].append(outcome.get("directional_result", "unknown"))
            append_number(values["raw"], outcome.get("raw_return")); append_number(values["adjusted"], outcome.get("side_adjusted_return"))
            append_number(values["favorable"], outcome.get("max_favorable_return_until_horizon")); append_number(values["adverse"], outcome.get("max_adverse_return_until_horizon"))

    def write_profiles(self) -> None:
        groups = []
        for key in sorted(self.profile_groups):
            source = self.profile_groups[key]; horizons = {}
            for label, values in source["horizons"].items():
                results = values["results"]
                horizons[label] = {
                    "favorable_count": results.count("favorable"), "unfavorable_count": results.count("unfavorable"),
                    "flat_count": results.count("flat"), "unknown_count": results.count("unknown"),
                    "avg_raw_return": average(values["raw"]), "median_raw_return": median(values["raw"]),
                    "avg_side_adjusted_return": average(values["adjusted"]), "median_side_adjusted_return": median(values["adjusted"]),
                    "avg_max_favorable_return": average(values["favorable"]), "avg_max_adverse_return": average(values["adverse"]),
                }
            groups.append({field: source[field] for field in ("symbol", "timeframe", "source", "event_type", "side", "direction", "pattern_signature", "pattern_key", "sample_count")} | {
                "sample_status": "insufficient_data" if source["sample_count"] < 30 else "observed_sample",
                "horizons": horizons, "scores": dict(NULL_SCORES),
            })
        payload = {"layer": "Layer-6B", "engine": "HistoricalOutcomeEngine", "record_type": "calibration_profile_summary", "calibration_status": "observed_not_scored", "generated_at": time.time(), "groups": groups, "scores": dict(NULL_SCORES)}
        PROFILES_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        self.profiles_written += 1; self.last_profile_write = time.monotonic()

    def write_open_state(self) -> None:
        payload = {"layer": "Layer-6B", "engine": "HistoricalOutcomeEngine", "record_type": "open_historical_outcomes", "generated_at": time.time(), "observations": list(self.open_observations.values())}
        OPEN_FILE.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
        self.last_state_write = time.monotonic()

    def write_health(self) -> None:
        self.refresh_missing_inputs()
        payload = {"status": "alive", "prices_indexed": self.prices_indexed, "input_events_processed": dict(self.input_events_processed), "open_observations": len(self.open_observations), "completed_observations": self.completed_observations, "profiles_written": self.profiles_written, "last_price_ts": self.last_price_ts, "last_event_ts": self.last_event_ts, "missing_inputs": sorted(self.missing_inputs), "warnings": sorted(self.warnings), "errors": list(self.errors)}
        HEALTH_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def tick(self) -> None:
        now = time.monotonic()
        if now - self.last_state_write >= STATE_INTERVAL_SECONDS: self.write_open_state()
        if now - self.last_profile_write >= PROFILE_INTERVAL_SECONDS: self.write_profiles()
        if now - self.last_heartbeat >= HEARTBEAT_SECONDS:
            self.write_health(); self.heartbeat(); self.last_heartbeat = now

    def heartbeat(self) -> None:
        print("Historical Outcome Engine alive", flush=True)
        print(f"prices_indexed={self.prices_indexed}", flush=True)
        print(f"open_observations={len(self.open_observations)}", flush=True)
        print(f"completed_observations={self.completed_observations}", flush=True)
        print(f"profiles_written={self.profiles_written}", flush=True)
        print(f"last_price_ts={self.last_price_ts}", flush=True)
        print(f"last_event_ts={self.last_event_ts}", flush=True)


def normalize_event(source_name: str, row: dict[str, Any]) -> dict[str, Any] | None:
    source = {"detector_events": "detector", "evidence_packets": "evidence", "structure_events": "smart_money", "observer_events": "observer"}[source_name]
    symbol = str(row.get("symbol") or ""); timeframe = str(row.get("timeframe") or "")
    timestamp = safe_int(row.get("window_start_ts"))
    if not symbol or not timeframe or timestamp is None: return None
    if source == "evidence":
        summary = row.get("evidence_summary") if isinstance(row.get("evidence_summary"), dict) else {}
        buy = list(summary.get("buy_side_events", [])); sell = list(summary.get("sell_side_events", []))
        neutral = list(summary.get("neutral_events", [])); unknown = list(summary.get("unknown_side_events", []))
        side = "mixed" if buy and sell else "buy" if buy else "sell" if sell else "neutral" if neutral else "unknown"
        components = [str(value.get("event_type") if isinstance(value, dict) else value) for value in buy + sell + neutral + unknown]
        event_type = "evidence_packet"; direction = "unknown"
    else:
        side = str(row.get("side") or "unknown")
        if source == "observer": side = {"long": "buy", "short": "sell"}.get(str(row.get("watch_side")), side)
        event_type = str(row.get("event_type") or "unknown"); direction = str(row.get("direction") or "unknown")
        components = [event_type]
    event_id = row.get("event_id") or row.get("detector_event_id")
    if not event_id: event_id = deterministic_event_id(source, symbol, timeframe, timestamp, event_type, side, direction)
    quality = row.get("data_quality") if isinstance(row.get("data_quality"), dict) else {"quality_state": "unknown"}
    return {"source": source, "event_id": str(event_id), "symbol": symbol, "timeframe": timeframe, "window_start_ts": timestamp, "window_end_ts": safe_int(row.get("window_end_ts")), "event_type": event_type, "side": side, "direction": direction, "calibration_status": row.get("calibration_status"), "source_refs": row.get("source_refs", {}), "data_quality": quality, "component_types": components, "pattern_components": [], "source_event": {"source": source, "event_id": str(event_id), "event_type": event_type, "calibration_status": row.get("calibration_status"), "source_refs": row.get("source_refs", {})}}


def make_composite_event(event: dict[str, Any]) -> dict[str, Any]:
    result = dict(event); components = event["pattern_components"]
    result["source"] = "composite"; result["event_type"] = "composite_pattern"
    result["event_id"] = "composite_" + hashlib.sha256(f"{event['symbol']}|{event['timeframe']}|{event['window_start_ts']}|{'+'.join(components)}".encode()).hexdigest()[:24]
    result["source_event"] = {"source": "composite", "component_event_id": event["event_id"]}
    return result


def pattern_signature(symbol: str, timeframe: str, components: list[str]) -> str:
    return hashlib.sha256(f"{symbol}|{timeframe}|{'+'.join(sorted(components))}".encode()).hexdigest()


def deterministic_event_id(source: str, symbol: str, timeframe: str, timestamp: int, event_type: str, side: str, direction: str) -> str:
    return hashlib.sha256(f"{source}{symbol}{timeframe}{timestamp}{event_type}{side}{direction}".encode()).hexdigest()


def extract_close_price(row: dict[str, Any]) -> float | None:
    candidates = [
        nested(row, "close", "price"),
        nested(row, "ohlc", "close", "price"),
        nested(row, "candle_dna", "close", "price"),
        nested(row, "candle_dna", "close"),
        nested(row, "candle_dna", "last_trade_price"),
        nested(row, "candle_dna", "carry_forward_price"),
        row.get("close"),
        row.get("price"),
    ]
    for value in candidates:
        if isinstance(value, dict): value = value.get("price")
        try: price = float(value)
        except (TypeError, ValueError, OverflowError): continue
        if price > 0: return price
    return None


def db_price(symbol: str, row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "symbol": symbol,
        "window_start_ts": int(row[0]),
        "window_end_ts": safe_int(row[1]),
        "close_price": float(row[2]),
    }


def scan_reference_price(symbol: str, event_ts: int) -> dict[str, Any] | None:
    best = None
    for row in read_jsonl(PRICE_FILE):
        ts = safe_int(row.get("window_start_ts")); price = extract_close_price(row)
        if str(row.get("symbol")) == symbol and ts is not None and ts <= event_ts and price is not None:
            if best is None or ts > best["window_start_ts"]: best = {"symbol": symbol, "window_start_ts": ts, "window_end_ts": safe_int(row.get("window_end_ts")), "close_price": price}
    return best


def scan_future_price(symbol: str, target_ts: int) -> dict[str, Any] | None:
    best = None
    for row in read_jsonl(PRICE_FILE):
        ts = safe_int(row.get("window_start_ts")); price = extract_close_price(row)
        if str(row.get("symbol")) == symbol and ts is not None and ts >= target_ts and price is not None:
            if best is None or ts < best["window_start_ts"]: best = {"symbol": symbol, "window_start_ts": ts, "window_end_ts": safe_int(row.get("window_end_ts")), "close_price": price}
    return best


def scan_path_prices(symbol: str, start_ts: int, end_ts: int) -> list[float]:
    values = []
    for row in read_jsonl(PRICE_FILE):
        ts = safe_int(row.get("window_start_ts")); price = extract_close_price(row)
        if str(row.get("symbol")) == symbol and ts is not None and start_ts <= ts <= end_ts and price is not None: values.append(price)
    return values


def path_excursions(prices: list[float], reference: float, side: str) -> tuple[float | None, float | None]:
    if side not in ("buy", "sell") or not prices: return None, None
    returns = [(price - reference) / reference for price in prices]
    if side == "sell": returns = [-value for value in returns]
    return max(returns), min(returns)


def side_adjusted_return(raw: float, side: str) -> float | None:
    if side == "buy": return raw
    if side == "sell": return -raw
    return None


def directional_result(value: float | None) -> str:
    if value is None: return "unknown"
    if value > 0: return "favorable"
    if value < 0: return "unfavorable"
    return "flat"


def load_completed_ids() -> set[str]:
    return {str(row["event_id"]) for row in read_jsonl(OBSERVATIONS_FILE) if row.get("event_id")}


def load_open_observations() -> dict[str, dict[str, Any]]:
    if not OPEN_FILE.exists(): return {}
    try: payload = json.loads(OPEN_FILE.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError): return {}
    values = payload.get("observations", []) if isinstance(payload, dict) else []
    return {str(item["event_id"]): item for item in values if isinstance(item, dict) and item.get("event_id")}


def window_key(row: dict[str, Any], timestamp_field: str) -> WindowKey | None:
    symbol = row.get("symbol"); timeframe = row.get("timeframe"); timestamp = safe_int(row.get(timestamp_field))
    return (str(symbol), str(timeframe), timestamp) if symbol and timeframe and timestamp is not None else None


def nested(value: Any, *path: str) -> Any:
    for key in path:
        if not isinstance(value, dict): return None
        value = value.get(key)
    return value


def safe_int(value: Any) -> int | None:
    try: return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError): return None


def append_number(values: list[float], value: Any) -> None:
    if isinstance(value, (int, float)): values.append(float(value))


def average(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def relative_label(path: Path) -> str:
    return str(path.relative_to(ROOT_DIR)).replace("\\", "/")


def read_jsonl(path: Path):
    if not path.exists(): return
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try: row = json.loads(line)
            except json.JSONDecodeError: continue
            if isinstance(row, dict): yield row


def run() -> None:
    engine = HistoricalOutcomeEngine()
    all_paths = {"prices": PRICE_FILE, **CONTEXT_FILES, **EVENT_FILES}
    handles: dict[str, Any] = {name: None for name in all_paths}
    try:
        # Build deterministic backlog indexes before opening observations.
        for source in ("prices", "context_dna", "smart_money_dna", "data_quality"):
            path = all_paths[source]
            if not path.exists(): continue
            handles[source] = path.open("r", encoding="utf-8", errors="replace")
            for line in handles[source]:
                try: row = json.loads(line)
                except json.JSONDecodeError: continue
                if source == "prices": engine.index_price(row)
                else: engine.index_context(source, row)
        engine.loading_backlog = False
        for observation in list(engine.open_observations.values()):
            engine.measure_observation(observation)
        backlog: dict[str, list[dict[str, Any]]] = {name: [] for name in EVENT_FILES}
        for source, path in EVENT_FILES.items():
            if not path.exists(): continue
            handles[source] = path.open("r", encoding="utf-8", errors="replace")
            for line in handles[source]:
                try: row = json.loads(line)
                except json.JSONDecodeError: continue
                if isinstance(row, dict): backlog[source].append(row); engine.preindex_event(source, row)
        for source in ("structure_events", "detector_events", "observer_events", "evidence_packets"):
            for row in backlog[source]: engine.process_event(source, row)
        engine.write_open_state(); engine.write_health()
        while True:
            activity = 0
            for source, path in all_paths.items():
                if handles[source] is None:
                    if not path.exists(): continue
                    handles[source] = path.open("r", encoding="utf-8", errors="replace")
                while True:
                    line = handles[source].readline()
                    if not line: break
                    try: row = json.loads(line)
                    except json.JSONDecodeError:
                        engine.record_error("parse", source, "invalid JSON"); continue
                    if source == "prices": engine.index_price(row)
                    elif source in CONTEXT_FILES: engine.index_context(source, row)
                    else: engine.preindex_event(source, row); engine.process_event(source, row)
                    activity += 1
            engine.tick()
            if activity == 0: time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in handles.values():
            if handle is not None: handle.close()
        engine.close()


if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: print("Stopped.", flush=True)
