"""Uncalibrated market-structure and Smart Money candidate engine."""

import hashlib
import json
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from smart_money_contracts import get_smart_money_contract, validate_smart_money_contracts


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
ACTIVE_SOURCES = {
    "1S": DATA_DIR / "one_second_combined_dna.jsonl",
    "3S": DATA_DIR / "rolling_3s_dna.jsonl",
    "5S": DATA_DIR / "rolling_5s_dna.jsonl",
    "15S": DATA_DIR / "rolling_15s_dna.jsonl",
    "1M": DATA_DIR / "aligned_1m_candle_dna.jsonl",
}
OPTIONAL_SOURCES = {
    "5M": DATA_DIR / "aligned_5m_candle_dna.jsonl",
    "15M": DATA_DIR / "aligned_15m_candle_dna.jsonl",
    "context": DATA_DIR / "context_dna.jsonl",
    "detectors": DATA_DIR / "detector_events.jsonl",
    "evidence": DATA_DIR / "evidence_packets.jsonl",
}
DNA_FILE = DATA_DIR / "smart_money_dna.jsonl"
EVENTS_FILE = DATA_DIR / "structure_events.jsonl"
HEALTH_FILE = DATA_DIR / "smart_money_health.json"
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0
NULL_SCORES = {"confidence": None, "strength_score": None, "structure_score": None, "threshold": None}


@dataclass
class TimeframeState:
    last_candles: deque = field(default_factory=lambda: deque(maxlen=5))
    last_fractal_high: dict[str, Any] | None = None
    last_fractal_low: dict[str, Any] | None = None
    last_swing_high: dict[str, Any] | None = None
    last_swing_low: dict[str, Any] | None = None
    last_high_class: str | None = None
    last_low_class: str | None = None
    structure_bias: str = "unknown"
    order_blocks: list[dict[str, Any]] = field(default_factory=list)
    breaker_blocks: list[dict[str, Any]] = field(default_factory=list)
    imbalances: list[dict[str, Any]] = field(default_factory=list)
    mitigations: list[dict[str, Any]] = field(default_factory=list)
    equal_highs: list[dict[str, Any]] = field(default_factory=list)
    equal_lows: list[dict[str, Any]] = field(default_factory=list)
    written_event_keys: set[tuple[str, str, int, str, str]] = field(default_factory=set)


class SmartMoneyEngine:
    def __init__(self) -> None:
        registry_errors = validate_smart_money_contracts()
        self.registry_validation_passed = not registry_errors
        if registry_errors:
            raise RuntimeError("Smart Money registry validation failed: " + "; ".join(registry_errors))
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.state_by_timeframe = {timeframe: TimeframeState() for timeframe in ACTIVE_SOURCES}
        self.snapshot_keys = load_snapshot_keys()
        existing_events = load_event_keys()
        for state in self.state_by_timeframe.values():
            state.written_event_keys.update(existing_events)
        self.processed_rows = {timeframe: 0 for timeframe in ACTIVE_SOURCES}
        self.snapshots_written = 0
        self.structure_events_written = 0
        self.last_window_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_heartbeat = time.monotonic()
        self.dna_handle = DNA_FILE.open("a", encoding="utf-8")
        self.event_handle = EVENTS_FILE.open("a", encoding="utf-8")
        self.refresh_missing_inputs()
        self.write_health()

    def close(self) -> None:
        self.write_health()
        self.dna_handle.close()
        self.event_handle.close()

    def refresh_missing_inputs(self) -> None:
        for path in ACTIVE_SOURCES.values():
            label = relative_label(path)
            if path.exists(): self.missing_inputs.discard(label)
            else: self.missing_inputs.add(label)
        for path in OPTIONAL_SOURCES.values():
            if not path.exists():
                self.warnings.add(f"optional_input_missing:{relative_label(path)}")

    def process_row(self, row: dict[str, Any], timeframe: str, source_file: Path) -> None:
        candle = normalize_candle(row, timeframe, source_file)
        self.processed_rows[timeframe] += 1
        ts = candle.get("window_start_ts")
        if ts is None:
            self.warnings.add(f"invalid_window_start_ts:{timeframe}")
            return
        self.last_window_ts = max(self.last_window_ts, ts)
        state = self.state_by_timeframe[timeframe]
        valid = all(candle.get(field) is not None for field in ("open", "high", "low", "close"))
        errors = [] if valid else ["missing_ohlc"]
        if valid:
            self.detect_existing_zone_events(state, candle)
            state.last_candles.append(candle)
            self.detect_direction_change(state, candle)
            self.detect_imbalance(state)
            self.detect_fractals(state)
            self.detect_structure_breaks(state, candle)
        self.write_snapshot(state, candle, valid, errors)

    def detect_existing_zone_events(self, state: TimeframeState, candle: dict[str, Any]) -> None:
        for zone in state.order_blocks:
            if zone.get("broken"):
                continue
            if zone["side"] == "bullish" and candle["close"] < zone["low"]:
                zone["broken"] = True
                breaker = make_zone("bearish", zone["low"], zone["high"], candle, "order_block")
                state.breaker_blocks.append(breaker)
                self.emit(state, candle, "breaker_block_candidate", "sell", "down", breaker, {"broken_order_block_id": zone["zone_id"]})
            elif zone["side"] == "bearish" and candle["close"] > zone["high"]:
                zone["broken"] = True
                breaker = make_zone("bullish", zone["low"], zone["high"], candle, "order_block")
                state.breaker_blocks.append(breaker)
                self.emit(state, candle, "breaker_block_candidate", "buy", "up", breaker, {"broken_order_block_id": zone["zone_id"]})
        for zone in state.order_blocks + state.imbalances:
            if zone.get("created_ts") == candle["window_start_ts"] or zone.get("mitigated"):
                continue
            if candle["low"] <= zone["high"] and candle["high"] >= zone["low"]:
                zone["mitigated"] = True
                mitigation = make_zone("neutral", zone["low"], zone["high"], candle, zone["zone_type"])
                state.mitigations.append(mitigation)
                self.emit(state, candle, "mitigation_candidate", "neutral", "unknown", mitigation, {"source_zone_id": zone["zone_id"], "reaction_required": False})

    def detect_direction_change(self, state: TimeframeState, current: dict[str, Any]) -> None:
        if len(state.last_candles) < 2:
            return
        previous = state.last_candles[-2]
        side = None
        if previous["close"] < previous["open"] and current["close"] > current["open"]:
            side = "bullish"
        elif previous["close"] > previous["open"] and current["close"] < current["open"]:
            side = "bearish"
        if side is None:
            return
        zone = make_zone(side, previous["low"], previous["high"], previous, "order_block")
        zone.update({"open": previous["open"], "close": previous["close"], "broken": False, "mitigated": False})
        state.order_blocks.append(zone)
        event_side, direction = ("buy", "up") if side == "bullish" else ("sell", "down")
        self.emit(state, current, "order_block_candidate", event_side, direction, zone, {"direction_change_only": True, "source_candle_ts": previous["window_start_ts"]})

    def detect_imbalance(self, state: TimeframeState) -> None:
        if len(state.last_candles) < 3:
            return
        first, _, third = list(state.last_candles)[-3:]
        if first["high"] < third["low"]:
            zone = make_zone("bullish", first["high"], third["low"], third, "imbalance")
            zone["mitigated"] = False
            state.imbalances.append(zone)
            self.emit(state, third, "imbalance_candidate", "buy", "up", zone, {"candle_1_ts": first["window_start_ts"], "exact_gap": True})
        elif first["low"] > third["high"]:
            zone = make_zone("bearish", third["high"], first["low"], third, "imbalance")
            zone["mitigated"] = False
            state.imbalances.append(zone)
            self.emit(state, third, "imbalance_candidate", "sell", "down", zone, {"candle_1_ts": first["window_start_ts"], "exact_gap": True})

    def detect_fractals(self, state: TimeframeState) -> None:
        if len(state.last_candles) < 3:
            return
        previous, current, next_candle = list(state.last_candles)[-3:]
        if current["high"] > previous["high"] and current["high"] > next_candle["high"]:
            fractal = swing_point(current, "high")
            self.emit(state, current, "fractal_high_candidate", "sell", "unknown", fractal, {"next_candle_ts": next_candle["window_start_ts"]})
            old = state.last_swing_high
            prior_fractal = state.last_fractal_high
            state.last_fractal_high = fractal
            if prior_fractal and fractal["high"] == prior_fractal["high"]:
                state.equal_highs.append(fractal)
                self.emit(state, current, "equal_high_candidate", "sell", "unknown", fractal, {"previous_fractal_ts": prior_fractal["window_start_ts"], "exact_equality": True})
            if old:
                classification = "HH_candidate" if fractal["high"] > old["high"] else "LH_candidate" if fractal["high"] < old["high"] else None
                if classification:
                    side, direction = ("buy", "up") if classification == "HH_candidate" else ("sell", "down")
                    self.emit(state, current, classification, side, direction, {"current": fractal, "previous": old}, {})
                    state.last_high_class = classification
            state.last_swing_high = fractal
        if current["low"] < previous["low"] and current["low"] < next_candle["low"]:
            fractal = swing_point(current, "low")
            self.emit(state, current, "fractal_low_candidate", "buy", "unknown", fractal, {"next_candle_ts": next_candle["window_start_ts"]})
            old = state.last_swing_low
            prior_fractal = state.last_fractal_low
            state.last_fractal_low = fractal
            if prior_fractal and fractal["low"] == prior_fractal["low"]:
                state.equal_lows.append(fractal)
                self.emit(state, current, "equal_low_candidate", "buy", "unknown", fractal, {"previous_fractal_ts": prior_fractal["window_start_ts"], "exact_equality": True})
            if old:
                classification = "HL_candidate" if fractal["low"] > old["low"] else "LL_candidate" if fractal["low"] < old["low"] else None
                if classification:
                    side, direction = ("buy", "up") if classification == "HL_candidate" else ("sell", "down")
                    self.emit(state, current, classification, side, direction, {"current": fractal, "previous": old}, {})
                    state.last_low_class = classification
            state.last_swing_low = fractal
        if state.last_high_class == "HH_candidate" and state.last_low_class == "HL_candidate":
            state.structure_bias = "up"
        elif state.last_high_class == "LH_candidate" and state.last_low_class == "LL_candidate":
            state.structure_bias = "down"
        elif state.last_high_class and state.last_low_class:
            state.structure_bias = "range"

    def detect_structure_breaks(self, state: TimeframeState, candle: dict[str, Any]) -> None:
        event = None
        if state.structure_bias == "up" and state.last_swing_high and candle["close"] > state.last_swing_high["high"]:
            event = ("BOS_candidate", "buy", "up", state.last_swing_high)
        elif state.structure_bias == "down" and state.last_swing_low and candle["close"] < state.last_swing_low["low"]:
            event = ("BOS_candidate", "sell", "down", state.last_swing_low)
        elif state.structure_bias == "up" and state.last_swing_low and candle["close"] < state.last_swing_low["low"]:
            event = ("CHoCH_candidate", "sell", "down", state.last_swing_low)
        elif state.structure_bias == "down" and state.last_swing_high and candle["close"] > state.last_swing_high["high"]:
            event = ("CHoCH_candidate", "buy", "up", state.last_swing_high)
        if event:
            event_type, side, direction, swing = event
            source_id = self.emit(state, candle, event_type, side, direction, {"close": candle["close"], "swing": swing}, {"structure_bias": state.structure_bias})
            if source_id:
                self.emit(state, candle, "MSB_candidate", side, direction, {}, {"source_structure_event_id": source_id})

    def emit(self, state: TimeframeState, candle: dict[str, Any], event_type: str, side: str, direction: str, measurements: dict[str, Any], reason: dict[str, Any]) -> str | None:
        contract = get_smart_money_contract(event_type)
        key = (event_type, candle["timeframe"], candle["window_start_ts"], direction, side)
        if contract is None or key in state.written_event_keys:
            if contract is None: self.warnings.add(f"contract_missing:{event_type}")
            return None
        event_id = make_event_id(*key)
        payload = {
            "layer": "Layer-6A", "engine": "SmartMoneyEngine", "record_type": "structure_event",
            "event_id": event_id, "symbol": candle["symbol"], "timeframe": candle["timeframe"],
            "window_start_ts": candle["window_start_ts"], "window_end_ts": candle["window_end_ts"],
            "event_type": event_type, "side": side, "direction": direction,
            "calibration_status": contract["calibration_status"], "confidence": None,
            "strength_score": None, "thresholds": None, "measurements": measurements,
            "reason": reason, "source_refs": {"source_file": candle["source_file"], "source_window_ts": candle["window_start_ts"]},
            "data_quality": candle["data_quality"],
            "validation": {"input_valid": True, "contract_found": True, "invariants_passed": True, "errors": []},
        }
        self.event_handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.event_handle.flush()
        state.written_event_keys.add(key)
        self.structure_events_written += 1
        return event_id

    def write_snapshot(self, state: TimeframeState, candle: dict[str, Any], valid: bool, errors: list[str]) -> None:
        key = (candle["timeframe"], candle["window_start_ts"])
        if key in self.snapshot_keys:
            return
        payload = {
            "layer": "Layer-6A", "engine": "SmartMoneyEngine", "record_type": "smart_money_snapshot",
            "symbol": candle["symbol"], "timeframe": candle["timeframe"], "window_start_ts": candle["window_start_ts"],
            "window_end_ts": candle["window_end_ts"], "source_file": candle["source_file"],
            "swing_state": {"last_fractal_high": state.last_fractal_high, "last_fractal_low": state.last_fractal_low, "last_swing_high": state.last_swing_high, "last_swing_low": state.last_swing_low, "structure_bias": state.structure_bias},
            "zones": {"order_blocks": state.order_blocks, "breaker_blocks": state.breaker_blocks, "imbalances": state.imbalances, "mitigations": state.mitigations, "equal_highs": state.equal_highs, "equal_lows": state.equal_lows},
            "calibration_status": "uncalibrated", "scores": dict(NULL_SCORES),
            "source_refs": {"source_file": candle["source_file"], "source_window_ts": candle["window_start_ts"]},
            "data_quality": candle["data_quality"], "validation": {"input_valid": valid, "errors": errors},
        }
        self.dna_handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.dna_handle.flush()
        self.snapshot_keys.add(key)
        self.snapshots_written += 1

    def tick(self) -> None:
        if time.monotonic() - self.last_heartbeat >= HEARTBEAT_SECONDS:
            self.heartbeat()
            self.last_heartbeat = time.monotonic()

    def write_health(self) -> None:
        self.refresh_missing_inputs()
        payload = {"status": "alive", "processed_rows": self.processed_rows, "snapshots_written": self.snapshots_written, "structure_events_written": self.structure_events_written, "last_window_ts": self.last_window_ts, "missing_inputs": sorted(self.missing_inputs), "warnings": sorted(self.warnings), "registry_validation_passed": self.registry_validation_passed}
        HEALTH_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def heartbeat(self) -> None:
        self.write_health()
        print("Smart Money Engine alive", flush=True)
        for timeframe in ACTIVE_SOURCES: print(f"{timeframe} processed={self.processed_rows[timeframe]}", flush=True)
        print(f"snapshots_written={self.snapshots_written}", flush=True)
        print(f"structure_events_written={self.structure_events_written}", flush=True)
        print(f"last_window_ts={self.last_window_ts}", flush=True)


def normalize_candle(row: dict[str, Any], timeframe: str, source_file: Path) -> dict[str, Any]:
    if timeframe == "1S":
        ohlc = row.get("candle_dna", {})
        volume = ohlc
        flow = ohlc
        footprint = row.get("footprint_dna", {})
    else:
        ohlc = row.get("ohlc", {})
        volume = row.get("volume", {})
        flow = row.get("trade_flow", {})
        footprint = row.get("footprint", {})
    return {
        "symbol": str(row.get("symbol", "BTCUSDT")), "timeframe": timeframe,
        "source_file": relative_label(source_file), "window_start_ts": safe_int(row.get("window_start_ts")),
        "window_end_ts": safe_int(row.get("window_end_ts")), "open": point_price(ohlc.get("open")),
        "high": point_price(ohlc.get("high")), "low": point_price(ohlc.get("low")), "close": point_price(ohlc.get("close")),
        "buy_volume": optional_float(volume.get("buy_volume")), "sell_volume": optional_float(volume.get("sell_volume")),
        "total_volume": optional_float(volume.get("total_volume")), "delta": optional_float(volume.get("delta")),
        "trade_count": safe_int(flow.get("trade_count")), "footprint_levels": list(footprint.get("price_levels", [])) if isinstance(footprint, dict) else [],
        "data_quality": row.get("data_quality", {"quality_state": "unknown", "warning": "source_data_quality_missing"}),
    }


def make_zone(side: str, low: float, high: float, candle: dict[str, Any], zone_type: str) -> dict[str, Any]:
    raw = f"{zone_type}|{side}|{candle['timeframe']}|{candle['window_start_ts']}|{low}|{high}"
    return {"zone_id": hashlib.sha256(raw.encode()).hexdigest()[:20], "zone_type": zone_type, "side": side, "low": low, "high": high, "created_ts": candle["window_start_ts"]}


def swing_point(candle: dict[str, Any], kind: str) -> dict[str, Any]:
    return {"window_start_ts": candle["window_start_ts"], kind: candle[kind], "source_file": candle["source_file"]}


def make_event_id(event_type: str, timeframe: str, ts: int, direction: str, side: str) -> str:
    return "smart_" + hashlib.sha256(f"{event_type}|{timeframe}|{ts}|{direction}|{side}".encode()).hexdigest()[:20]


def point_price(value: Any) -> float | None:
    if isinstance(value, dict): value = value.get("price")
    return optional_float(value)


def optional_float(value: Any) -> float | None:
    try: return float(value) if value is not None else None
    except (TypeError, ValueError, OverflowError): return None


def safe_int(value: Any) -> int | None:
    try: return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError): return None


def relative_label(path: Path) -> str:
    resolved = path if path.is_absolute() else ROOT_DIR / path
    try:
        return str(resolved.relative_to(ROOT_DIR)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def read_jsonl(path: Path):
    if not path.exists(): return
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try: row = json.loads(line)
            except json.JSONDecodeError: continue
            if isinstance(row, dict): yield row


def load_snapshot_keys() -> set[tuple[str, int]]:
    keys = set()
    for row in read_jsonl(DNA_FILE):
        ts = safe_int(row.get("window_start_ts"))
        if row.get("timeframe") and ts is not None: keys.add((str(row["timeframe"]), ts))
    return keys


def load_event_keys() -> set[tuple[str, str, int, str, str]]:
    keys = set()
    for row in read_jsonl(EVENTS_FILE):
        ts = safe_int(row.get("window_start_ts"))
        if ts is not None: keys.add((str(row.get("event_type")), str(row.get("timeframe")), ts, str(row.get("direction")), str(row.get("side"))))
    return keys


def run() -> None:
    engine = SmartMoneyEngine()
    handles: dict[str, Any] = {timeframe: None for timeframe in ACTIVE_SOURCES}
    try:
        while True:
            activity = 0
            for timeframe, path in ACTIVE_SOURCES.items():
                if handles[timeframe] is None:
                    if not path.exists(): continue
                    handles[timeframe] = path.open("r", encoding="utf-8", errors="replace")
                while True:
                    line = handles[timeframe].readline()
                    if not line: break
                    try: row = json.loads(line)
                    except json.JSONDecodeError:
                        engine.warnings.add(f"invalid_json:{timeframe}")
                        continue
                    if isinstance(row, dict):
                        engine.process_row(row, timeframe, path)
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
