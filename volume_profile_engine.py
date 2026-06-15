"""Layer-6C uncalibrated volume and market-profile context engine."""

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from volume_profile_contracts import get_volume_profile_contract, validate_volume_profile_contracts


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
ACTIVE_SOURCES = {
    "1S": DATA_DIR / "one_second_combined_dna.jsonl",
    "3S": DATA_DIR / "rolling_3s_dna.jsonl",
    "5S": DATA_DIR / "rolling_5s_dna.jsonl",
    "15S": DATA_DIR / "rolling_15s_dna.jsonl",
    "1M": DATA_DIR / "aligned_1m_candle_dna.jsonl",
}
FORWARD_SOURCES = {
    "5M": DATA_DIR / "aligned_5m_candle_dna.jsonl",
    "15M": DATA_DIR / "aligned_15m_candle_dna.jsonl",
    "1H": DATA_DIR / "aligned_1h_candle_dna.jsonl",
}
AUXILIARY_SOURCES = [
    DATA_DIR / "context_dna.jsonl", DATA_DIR / "detector_events.jsonl",
    DATA_DIR / "structure_events.jsonl", DATA_DIR / "evidence_packets.jsonl",
]
OPTIONAL_SOURCES = [DATA_DIR / "smart_money_dna.jsonl", DATA_DIR / "data_quality.jsonl"]
DNA_FILE = DATA_DIR / "volume_profile_dna.jsonl"
EVENTS_FILE = DATA_DIR / "volume_profile_events.jsonl"
MEMORY_FILE = DATA_DIR / "volume_memory_zones.json"
HEALTH_FILE = DATA_DIR / "volume_profile_health.json"
ERRORS_FILE = DATA_DIR / "volume_profile_errors.jsonl"
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0


@dataclass
class ProfileState:
    volume_by_price: dict[str, float] = field(default_factory=dict)
    time_by_price: dict[str, int] = field(default_factory=dict)
    buy_volume_by_price: dict[str, float] = field(default_factory=dict)
    sell_volume_by_price: dict[str, float] = field(default_factory=dict)
    last_touch_by_price: dict[str, int] = field(default_factory=dict)
    last_poc: float | None = None
    last_value_area: dict[str, Any] | None = None
    last_close: float | None = None
    last_shape: str = "unknown"
    memory_zones: list[dict[str, Any]] = field(default_factory=list)
    processed_window_keys: set[tuple[str, str, int]] = field(default_factory=set)
    written_event_keys: set[str] = field(default_factory=set)


class VolumeProfileEngine:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        registry_errors = validate_volume_profile_contracts()
        self.registry_validation_passed = not registry_errors
        if registry_errors:
            raise RuntimeError("Volume profile registry validation failed: " + "; ".join(registry_errors))
        self.state_by_key: dict[tuple[str, str], ProfileState] = {}
        self.memory_by_key: dict[str, dict[str, Any]] = load_memory_zones()
        self.load_restart_state()
        self.input_rows_processed = {timeframe: 0 for timeframe in ACTIVE_SOURCES}
        self.auxiliary_rows_processed = {relative_label(path): 0 for path in AUXILIARY_SOURCES + OPTIONAL_SOURCES}
        self.snapshots_written = 0
        self.events_written = 0
        self.last_window_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.memory_dirty = False
        self.last_heartbeat = time.monotonic()
        self.dna_handle = DNA_FILE.open("a", encoding="utf-8")
        self.events_handle = EVENTS_FILE.open("a", encoding="utf-8")
        self.errors_handle = ERRORS_FILE.open("a", encoding="utf-8")
        self.refresh_missing_inputs()
        self.write_memory()
        self.write_health()

    def state_for(self, symbol: str, timeframe: str) -> ProfileState:
        key = (symbol, timeframe)
        if key not in self.state_by_key:
            self.state_by_key[key] = ProfileState()
        return self.state_by_key[key]

    def load_restart_state(self) -> None:
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for row in read_jsonl(DNA_FILE):
            symbol, timeframe, ts = str(row.get("symbol", "")), str(row.get("timeframe", "")), safe_int(row.get("window_start_ts"))
            if not symbol or not timeframe or ts is None: continue
            state = self.state_for(symbol, timeframe)
            state.processed_window_keys.add((symbol, timeframe, ts))
            current = latest.get((symbol, timeframe))
            if current is None or ts > int(current["window_start_ts"]): latest[(symbol, timeframe)] = row
        for key, row in latest.items():
            state = self.state_for(*key)
            profile = row.get("profile", {})
            state.volume_by_price = numeric_map(profile.get("volume_by_price"))
            state.time_by_price = integer_map(profile.get("time_by_price"))
            state.last_poc = optional_float(profile.get("poc"))
            state.last_value_area = profile.get("value_area") if isinstance(profile.get("value_area"), dict) else None
            state.last_close = optional_float(row.get("location", {}).get("close_price"))
            state.last_shape = str(profile.get("profile_shape", "unknown"))
        for row in read_jsonl(EVENTS_FILE):
            symbol, timeframe = str(row.get("symbol", "")), str(row.get("timeframe", ""))
            if symbol and timeframe: self.state_for(symbol, timeframe).written_event_keys.add(event_key_from_row(row))
        for zone in self.memory_by_key.values():
            symbol, timeframe = str(zone.get("symbol", "")), str(zone.get("timeframe", ""))
            if symbol and timeframe: self.state_for(symbol, timeframe).memory_zones.append(zone)

    def refresh_missing_inputs(self) -> None:
        for path in list(ACTIVE_SOURCES.values()) + AUXILIARY_SOURCES:
            label = relative_label(path)
            if path.exists(): self.missing_inputs.discard(label)
            else: self.missing_inputs.add(label)
        for path in list(FORWARD_SOURCES.values()) + OPTIONAL_SOURCES:
            if not path.exists(): self.warnings.add(f"optional_input_missing:{relative_label(path)}")

    def process_line(self, line: str, timeframe: str, source_file: Path) -> None:
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            self.write_error(source_file, None, "json_parse_error", str(exc)); return
        if not isinstance(row, dict):
            self.write_error(source_file, None, "row_not_object", "JSON row must be an object"); return
        try:
            self.process_row(row, timeframe, source_file)
        except Exception as exc:
            self.write_error(source_file, safe_int(row.get("window_start_ts")), "processing_error", str(exc))

    def process_auxiliary_line(self, line: str, source_file: Path) -> None:
        label = relative_label(source_file)
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            self.write_error(source_file, None, "json_parse_error", str(exc)); return
        if not isinstance(row, dict):
            self.write_error(source_file, None, "row_not_object", "JSON row must be an object"); return
        self.auxiliary_rows_processed[label] += 1

    def process_row(self, row: dict[str, Any], timeframe: str, source_file: Path) -> None:
        candle = normalize_candle(row, timeframe, source_file)
        self.input_rows_processed[timeframe] += 1
        symbol, ts = candle["symbol"], candle["window_start_ts"]
        state = self.state_for(symbol, timeframe)
        if ts is None:
            self.write_error(source_file, None, "missing_window_start_ts", timeframe); return
        self.last_window_ts = max(self.last_window_ts, ts)
        window_key = (symbol, timeframe, ts)
        if window_key in state.processed_window_keys: return
        errors = validate_candle(candle)
        if errors:
            self.write_snapshot(state, candle, False, errors, empty_profile(), empty_location(candle), empty_auction())
            state.processed_window_keys.add(window_key)
            return

        previous_poc = state.last_poc
        previous_value = dict(state.last_value_area) if state.last_value_area else None
        previous_close = state.last_close
        new_bins = self.accumulate(state, candle)
        profile = build_profile(state, previous_poc, previous_value, candle, new_bins)
        location = build_location(candle, profile, state.memory_zones)
        auction = build_auction(previous_close, previous_value, candle, profile)
        memory_refs = self.update_memory(state, candle, profile, auction)
        self.emit_profile_events(state, candle, profile, location, auction, previous_poc, previous_value)
        self.write_snapshot(state, candle, True, [], profile, location, auction, memory_refs)
        state.last_poc = profile["poc"]
        state.last_value_area = profile["value_area"]
        state.last_close = candle["close"]
        state.last_shape = profile["profile_shape"]
        state.processed_window_keys.add(window_key)

    def accumulate(self, state: ProfileState, candle: dict[str, Any]) -> set[str]:
        new_bins: set[str] = set()
        levels = candle["footprint_levels"]
        if levels:
            for level in levels:
                price = optional_float(level.get("price"))
                if price is None: continue
                key = price_key(price)
                if key not in state.volume_by_price: new_bins.add(key)
                buy = optional_float(level.get("buy_volume")) or 0.0
                sell = optional_float(level.get("sell_volume")) or 0.0
                total = optional_float(level.get("total_volume"))
                if total is None: total = optional_float(level.get("volume"))
                if total is None: total = buy + sell
                state.volume_by_price[key] = state.volume_by_price.get(key, 0.0) + total
                state.buy_volume_by_price[key] = state.buy_volume_by_price.get(key, 0.0) + buy
                state.sell_volume_by_price[key] = state.sell_volume_by_price.get(key, 0.0) + sell
                state.last_touch_by_price[key] = candle["window_start_ts"]
        elif candle["close"] is not None:
            key = price_key(candle["close"])
            if key not in state.volume_by_price: new_bins.add(key)
            total = candle["total_volume"] or 0.0
            state.volume_by_price[key] = state.volume_by_price.get(key, 0.0) + total
            state.buy_volume_by_price[key] = state.buy_volume_by_price.get(key, 0.0) + (candle["buy_volume"] or 0.0)
            state.sell_volume_by_price[key] = state.sell_volume_by_price.get(key, 0.0) + (candle["sell_volume"] or 0.0)
            state.last_touch_by_price[key] = candle["window_start_ts"]
        if candle["close"] is not None:
            close_key = price_key(candle["close"])
            state.time_by_price[close_key] = state.time_by_price.get(close_key, 0) + 1
            state.last_touch_by_price[close_key] = candle["window_start_ts"]
        return new_bins

    def emit_profile_events(self, state: ProfileState, candle: dict[str, Any], profile: dict[str, Any],
                            location: dict[str, Any], auction: dict[str, Any], previous_poc: float | None,
                            previous_value: dict[str, Any] | None) -> None:
        poc = profile["poc"]
        if previous_poc is not None and poc is not None and poc != previous_poc:
            direction = "up" if poc > previous_poc else "down"
            self.emit(state, candle, "poc_shift_candidate", direction, poc, None,
                      {"previous_poc": previous_poc, "current_poc": poc})
        if range_touches(candle, profile["value_area"].get("val"), profile["value_area"].get("vah")):
            self.emit(state, candle, "value_area_touch_candidate", "flat", None, profile["value_area"], {})
        for level in profile["hvn_levels"]:
            if range_touches(candle, level, level): self.emit(state, candle, "hvn_touch_candidate", "flat", level, None, {})
        for level in profile["lvn_levels"]:
            if range_touches(candle, level, level): self.emit(state, candle, "lvn_touch_candidate", "flat", level, None, {})
        shape_event = {"p_shape": "p_shape_profile_candidate", "b_shape": "b_shape_profile_candidate",
                       "d_shape": "d_shape_profile_candidate", "b_distribution": "b_distribution_profile_candidate",
                       "trend_profile": "trend_profile_candidate"}.get(profile["profile_shape"])
        if shape_event and profile["profile_shape"] != state.last_shape:
            self.emit(state, candle, shape_event, "flat", poc, profile.get("balance_zone"), {"shape": profile["profile_shape"]})
        if auction["acceptance_candidate"]:
            self.emit(state, candle, "acceptance_zone_candidate", "flat", None, auction["acceptance_candidate"], {})
        if auction["rejection_candidate"]:
            self.emit(state, candle, "rejection_zone_candidate", "flat", None, auction["rejection_candidate"], {})
        if auction["failed_auction_candidate"]:
            self.emit(state, candle, "failed_auction_candidate", "flat", candle["close"], auction["failed_auction_candidate"], {})
        if auction["failed_action_return_to_value_candidate"]:
            self.emit(state, candle, "failed_action_return_to_value_candidate", "flat", candle["close"],
                      auction["failed_action_return_to_value_candidate"], {"previous_value": previous_value})

    def emit(self, state: ProfileState, candle: dict[str, Any], event_type: str, direction: str,
             level: float | None, zone: dict[str, Any] | None, reason: dict[str, Any]) -> None:
        contract = get_volume_profile_contract(event_type)
        key = make_event_key(event_type, candle["symbol"], candle["timeframe"], candle["window_start_ts"], level, zone)
        if key in state.written_event_keys: return
        errors = [] if contract else ["volume_profile_contract_not_found"]
        payload = {
            "layer": "Layer-6C", "engine": "VolumeProfileEngine", "record_type": "volume_profile_event",
            "event_id": "vp_" + hashlib.sha256(key.encode()).hexdigest()[:20],
            "symbol": candle["symbol"], "timeframe": candle["timeframe"],
            "window_start_ts": candle["window_start_ts"], "window_end_ts": candle["window_end_ts"],
            "event_type": event_type, "side": "neutral", "direction": direction,
            "level": level, "zone": zone, "reason": reason,
            "source_refs": [{"source_file": candle["source_file"], "window_start_ts": candle["window_start_ts"]}],
            "calibration_status": "uncalibrated", "confidence": None, "strength_score": None,
            "thresholds": None, "validation": {"contract_found": contract is not None,
            "invariants_passed": not errors, "errors": errors},
        }
        self.events_handle.write(json.dumps(payload, separators=(",", ":")) + "\n"); self.events_handle.flush()
        state.written_event_keys.add(key); self.events_written += 1

    def update_memory(self, state: ProfileState, candle: dict[str, Any], profile: dict[str, Any],
                      auction: dict[str, Any]) -> list[str]:
        specs: list[tuple[str, float | None, float | None, float | None]] = []
        if profile["poc"] is not None: specs.append(("poc_memory", profile["poc"], None, None))
        specs.extend(("hvn_memory", level, None, None) for level in profile["hvn_levels"])
        specs.extend(("lvn_memory", level, None, None) for level in profile["lvn_levels"])
        balance = profile.get("balance_zone")
        if balance: specs.append(("balance_zone_memory", None, balance["zone_low"], balance["zone_high"]))
        value = profile["value_area"]
        if value.get("val") is not None: specs.append(("value_area_memory", None, value["val"], value["vah"]))
        failed = auction.get("failed_auction_candidate")
        if failed: specs.append(("failed_auction_memory", candle["close"], failed.get("zone_low"), failed.get("zone_high")))
        refs = []
        for zone_type, price, low, high in specs:
            key = memory_key(candle["symbol"], candle["timeframe"], zone_type, price, low, high)
            zone = self.memory_by_key.get(key)
            ref = {"source_file": candle["source_file"], "window_start_ts": candle["window_start_ts"]}
            if zone is None:
                zone = {"zone_id": "vpm_" + hashlib.sha256(key.encode()).hexdigest()[:20],
                        "symbol": candle["symbol"], "timeframe": candle["timeframe"], "zone_type": zone_type,
                        "price": price, "zone_low": low, "zone_high": high,
                        "first_seen_ts": candle["window_start_ts"], "last_seen_ts": candle["window_start_ts"],
                        "touch_count": 1, "source_refs": [ref], "calibration_status": "uncalibrated",
                        "scores": {"confidence": None, "strength_score": None, "memory_score": None, "threshold": None}}
                self.memory_by_key[key] = zone; state.memory_zones.append(zone)
            else:
                zone["last_seen_ts"] = candle["window_start_ts"]
                zone["touch_count"] = int(zone.get("touch_count", 0)) + 1
                first_ref = zone.get("source_refs", [ref])[0]
                zone["source_refs"] = [first_ref] if first_ref == ref else [first_ref, ref]
            refs.append(zone["zone_id"]); self.memory_dirty = True
        return refs

    def write_snapshot(self, state: ProfileState, candle: dict[str, Any], valid: bool, errors: list[str],
                       profile: dict[str, Any], location: dict[str, Any], auction: dict[str, Any],
                       memory_refs: list[str] | None = None) -> None:
        payload = {"layer": "Layer-6C", "engine": "VolumeProfileEngine", "record_type": "volume_profile_snapshot",
                   "symbol": candle["symbol"], "timeframe": candle["timeframe"],
                   "window_start_ts": candle["window_start_ts"], "window_end_ts": candle["window_end_ts"],
                   "profile": profile, "location": location, "auction": auction, "memory_refs": memory_refs or [],
                   "calibration_status": "uncalibrated",
                   "scores": {"confidence": None, "strength_score": None, "profile_score": None, "threshold": None},
                   "data_quality": candle["data_quality"], "validation": {"input_valid": valid, "errors": errors}}
        self.dna_handle.write(json.dumps(payload, separators=(",", ":")) + "\n"); self.dna_handle.flush()
        self.snapshots_written += 1

    def write_error(self, source_file: Path, ts: int | None, error_type: str, message: str) -> None:
        payload = {"engine": "VolumeProfileEngine", "source_file": relative_label(source_file),
                   "window_start_ts": ts, "error_type": error_type, "message": message}
        self.errors_handle.write(json.dumps(payload, separators=(",", ":")) + "\n"); self.errors_handle.flush()
        self.warnings.add(f"{error_type}:{relative_label(source_file)}")

    def write_memory(self) -> None:
        MEMORY_FILE.write_text(json.dumps(list(self.memory_by_key.values()), indent=2) + "\n", encoding="utf-8")
        self.memory_dirty = False

    def write_health(self) -> None:
        payload = {"status": "alive", "input_rows_processed": self.input_rows_processed,
                   "snapshots_written": self.snapshots_written, "events_written": self.events_written,
                   "memory_zones": len(self.memory_by_key), "last_window_ts": self.last_window_ts,
                   "missing_inputs": sorted(self.missing_inputs), "warnings": sorted(self.warnings),
                   "registry_validation_passed": self.registry_validation_passed}
        HEALTH_FILE.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def tick(self) -> None:
        now = time.monotonic()
        if now - self.last_heartbeat < HEARTBEAT_SECONDS: return
        self.refresh_missing_inputs()
        if self.memory_dirty: self.write_memory()
        self.write_health()
        print("Volume Profile Engine alive", flush=True)
        for timeframe in ACTIVE_SOURCES: print(f"{timeframe} processed={self.input_rows_processed[timeframe]}", flush=True)
        print(f"snapshots_written={self.snapshots_written}", flush=True)
        print(f"events_written={self.events_written}", flush=True)
        print(f"memory_zones={len(self.memory_by_key)}", flush=True)
        print(f"last_window_ts={self.last_window_ts}", flush=True)
        self.last_heartbeat = now

    def close(self) -> None:
        if self.memory_dirty: self.write_memory()
        self.write_health()
        self.dna_handle.close(); self.events_handle.close(); self.errors_handle.close()


def normalize_candle(row: dict[str, Any], timeframe: str, source_file: Path) -> dict[str, Any]:
    candle = row.get("candle_dna") if isinstance(row.get("candle_dna"), dict) else row
    ohlc = row.get("ohlc") if isinstance(row.get("ohlc"), dict) else candle
    volume = row.get("volume") if isinstance(row.get("volume"), dict) else candle
    flow = row.get("trade_flow") if isinstance(row.get("trade_flow"), dict) else candle
    footprint = row.get("footprint") if isinstance(row.get("footprint"), dict) else row.get("footprint_dna", {})
    levels = footprint.get("price_levels", []) if isinstance(footprint, dict) else []
    buy = first_float(volume.get("buy_volume"), row.get("buy_volume"))
    sell = first_float(volume.get("sell_volume"), row.get("sell_volume"))
    total = first_float(volume.get("total_volume"), row.get("total_volume"), row.get("volume"))
    if total is None and buy is not None and sell is not None: total = buy + sell
    quality = dict(row.get("data_quality")) if isinstance(row.get("data_quality"), dict) else {}
    quality["volume_allocation"] = "footprint_levels" if levels else "close_price_fallback"
    return {"symbol": str(row.get("symbol") or candle.get("symbol") or "BTCUSDT"), "timeframe": timeframe,
            "source_file": relative_label(source_file), "window_start_ts": safe_int(row.get("window_start_ts", candle.get("window_start_ts"))),
            "window_end_ts": safe_int(row.get("window_end_ts", candle.get("window_end_ts"))),
            "open": point_price(ohlc.get("open")), "high": point_price(ohlc.get("high")),
            "low": point_price(ohlc.get("low")), "close": first_float(point_price(ohlc.get("close")), row.get("price")),
            "buy_volume": buy, "sell_volume": sell, "total_volume": total,
            "delta": first_float(volume.get("delta"), row.get("delta")), "trade_count": safe_int(flow.get("trade_count")),
            "footprint_levels": levels if isinstance(levels, list) else [], "data_quality": quality}


def validate_candle(candle: dict[str, Any]) -> list[str]:
    errors = []
    if candle["window_start_ts"] is None: errors.append("missing_window_start_ts")
    if candle["close"] is None: errors.append("missing_close")
    if candle["total_volume"] is None: errors.append("missing_total_volume")
    return errors


def build_profile(state: ProfileState, previous_poc: float | None, previous_value: dict[str, Any] | None,
                  candle: dict[str, Any], new_bins: set[str]) -> dict[str, Any]:
    if not state.volume_by_price: return empty_profile()
    ordered = sorted(state.volume_by_price, key=float)
    max_volume = max(state.volume_by_price.values())
    candidates = [key for key in ordered if state.volume_by_price[key] == max_volume]
    poc_key = max(candidates, key=lambda key: state.last_touch_by_price.get(key, 0))
    poc_index = ordered.index(poc_key)
    selected = {poc_index}
    index = poc_index - 1
    while index >= 0 and state.volume_by_price[ordered[index]] == max_volume: selected.add(index); index -= 1
    index = poc_index + 1
    while index < len(ordered) and state.volume_by_price[ordered[index]] == max_volume: selected.add(index); index += 1
    value_prices = [float(ordered[index]) for index in sorted(selected)]
    hvn, lvn = [], []
    for index in range(1, len(ordered) - 1):
        before, current, after = (state.volume_by_price[ordered[index - 1]], state.volume_by_price[ordered[index]],
                                  state.volume_by_price[ordered[index + 1]])
        if current > before and current > after: hvn.append(float(ordered[index]))
        if current < before and current < after: lvn.append(float(ordered[index]))
    balance_keys = ordered[max(0, poc_index - 1):poc_index + 2]
    balance = {"zone_low": float(balance_keys[0]), "zone_high": float(balance_keys[-1]),
               "visited_price_bins": balance_keys, "method": "poc_adjacent_observed_bins"}
    lower_count, upper_count = poc_index, len(ordered) - poc_index - 1
    shape = "unknown"
    separated_hvns = len(hvn) >= 2
    prior_retested = previous_value is not None and range_touches(candle, previous_value.get("val"), previous_value.get("vah"))
    shifted_to_new = previous_poc is not None and float(poc_key) != previous_poc and poc_key in new_bins
    if shifted_to_new and not prior_retested: shape = "trend_profile"
    elif separated_hvns: shape = "b_distribution"
    elif lower_count == upper_count: shape = "d_shape"
    elif upper_count > lower_count: shape = "p_shape"
    elif lower_count > upper_count: shape = "b_shape"
    market_state = "balanced_candidate" if shape in {"d_shape", "b_distribution"} else "imbalanced_candidate" if shape in {"p_shape", "b_shape", "trend_profile"} else "unknown"
    return {"poc": float(poc_key), "value_area": {"vah": max(value_prices), "val": min(value_prices),
            "method": "candidate_no_threshold"}, "hvn_levels": hvn, "lvn_levels": lvn,
            "balance_zone": balance, "market_state": market_state, "profile_shape": shape,
            "volume_by_price": state.volume_by_price, "time_by_price": state.time_by_price}


def build_location(candle: dict[str, Any], profile: dict[str, Any], zones: list[dict[str, Any]]) -> dict[str, Any]:
    close, poc, value = candle["close"], profile.get("poc"), profile.get("value_area", {})
    vs_poc = "unknown" if close is None or poc is None else "above" if close > poc else "below" if close < poc else "at"
    val, vah = value.get("val"), value.get("vah")
    vs_value = "unknown" if close is None or val is None or vah is None else "inside_value" if val <= close <= vah else "above_value" if close > vah else "below_value"
    nearest = None
    candidates = [(zone_distance(close, zone), zone) for zone in zones if close is not None and zone_anchor(zone) is not None]
    if candidates: nearest = min(candidates, key=lambda item: (item[0], item[1]["zone_id"]))[1]
    return {"close_price": close, "location_vs_poc": vs_poc, "location_vs_value": vs_value, "nearest_memory_zone": nearest}


def build_auction(previous_close: float | None, previous_value: dict[str, Any] | None,
                  candle: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    value = profile.get("value_area", {})
    current_inside = inside(candle["close"], value)
    previous_inside_current = inside(previous_close, value)
    previous_inside_old = inside(previous_close, previous_value)
    returned = previous_close is not None and previous_value is not None and not previous_inside_old and current_inside
    acceptance = dict(value) if current_inside and previous_inside_current else None
    rejection = dict(value) if returned else None
    failed = {"zone_low": value.get("val"), "zone_high": value.get("vah"), "return_price": candle["close"]} if returned else None
    failed_action = dict(failed) if returned else None
    return {"acceptance_candidate": acceptance, "rejection_candidate": rejection,
            "failed_auction_candidate": failed,
            "failed_action_return_to_value_candidate": failed_action}


def empty_profile() -> dict[str, Any]:
    return {"poc": None, "value_area": {"vah": None, "val": None, "method": "candidate_no_threshold"},
            "hvn_levels": [], "lvn_levels": [], "balance_zone": None, "market_state": "unknown", "profile_shape": "unknown",
            "volume_by_price": {}, "time_by_price": {}}


def empty_location(candle: dict[str, Any]) -> dict[str, Any]:
    return {"close_price": candle.get("close"), "location_vs_poc": "unknown", "location_vs_value": "unknown", "nearest_memory_zone": None}


def empty_auction() -> dict[str, Any]:
    return {"acceptance_candidate": None, "rejection_candidate": None,
            "failed_auction_candidate": None, "failed_action_return_to_value_candidate": None}


def run() -> None:
    engine = VolumeProfileEngine()
    sources = dict(ACTIVE_SOURCES)
    handles: dict[str, Any] = {timeframe: None for timeframe in sources}
    auxiliary_handles: dict[Path, Any] = {path: None for path in AUXILIARY_SOURCES + OPTIONAL_SOURCES}
    try:
        while True:
            activity = 0
            for timeframe, path in sources.items():
                if handles[timeframe] is None:
                    if not path.exists(): continue
                    handles[timeframe] = path.open("r", encoding="utf-8", errors="replace")
                while True:
                    line = handles[timeframe].readline()
                    if not line: break
                    engine.process_line(line, timeframe, path); activity += 1
                    engine.tick()
            for path in auxiliary_handles:
                if auxiliary_handles[path] is None:
                    if not path.exists(): continue
                    auxiliary_handles[path] = path.open("r", encoding="utf-8", errors="replace")
                while True:
                    line = auxiliary_handles[path].readline()
                    if not line: break
                    engine.process_auxiliary_line(line, path); activity += 1
                    engine.tick()
            engine.tick()
            if activity == 0: time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in handles.values():
            if handle is not None: handle.close()
        for handle in auxiliary_handles.values():
            if handle is not None: handle.close()
        engine.close()


def read_jsonl(path: Path):
    if not path.exists(): return
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try: row = json.loads(line)
            except json.JSONDecodeError: continue
            if isinstance(row, dict): yield row


def load_memory_zones() -> dict[str, dict[str, Any]]:
    if not MEMORY_FILE.exists(): return {}
    try: rows = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError): return {}
    result = {}
    for zone in rows if isinstance(rows, list) else []:
        if isinstance(zone, dict): result[memory_key(str(zone.get("symbol")), str(zone.get("timeframe")), str(zone.get("zone_type")), zone.get("price"), zone.get("zone_low"), zone.get("zone_high"))] = zone
    return result


def memory_key(symbol: str, timeframe: str, zone_type: str, price: Any, low: Any, high: Any) -> str:
    return "|".join((symbol, timeframe, zone_type, scalar_key(price), scalar_key(low), scalar_key(high)))


def make_event_key(event_type: str, symbol: str, timeframe: str, ts: int, level: Any, zone: Any) -> str:
    return "|".join((event_type, symbol, timeframe, str(ts), scalar_key(level), json.dumps(zone, sort_keys=True, separators=(",", ":"))))


def event_key_from_row(row: dict[str, Any]) -> str:
    return make_event_key(str(row.get("event_type")), str(row.get("symbol")), str(row.get("timeframe")), int(row.get("window_start_ts", 0)), row.get("level"), row.get("zone"))


def inside(price: float | None, value: dict[str, Any] | None) -> bool:
    if price is None or not value: return False
    low, high = optional_float(value.get("val", value.get("zone_low"))), optional_float(value.get("vah", value.get("zone_high")))
    return low is not None and high is not None and low <= price <= high


def range_touches(candle: dict[str, Any], low: Any, high: Any) -> bool:
    low, high = optional_float(low), optional_float(high)
    candle_low, candle_high = candle.get("low"), candle.get("high")
    return low is not None and high is not None and candle_low is not None and candle_high is not None and candle_low <= high and candle_high >= low


def zone_anchor(zone: dict[str, Any]) -> float | None:
    price = optional_float(zone.get("price"))
    if price is not None: return price
    low, high = optional_float(zone.get("zone_low")), optional_float(zone.get("zone_high"))
    return None if low is None or high is None else (low + high) / 2


def zone_distance(price: float, zone: dict[str, Any]) -> float:
    anchor = zone_anchor(zone)
    return abs(price - anchor) if anchor is not None else float("inf")


def point_price(value: Any) -> float | None:
    return optional_float(value.get("price")) if isinstance(value, dict) else optional_float(value)


def first_float(*values: Any) -> float | None:
    for value in values:
        parsed = optional_float(value)
        if parsed is not None: return parsed
    return None


def optional_float(value: Any) -> float | None:
    try: return float(value) if value is not None and not isinstance(value, dict) else None
    except (TypeError, ValueError, OverflowError): return None


def safe_int(value: Any) -> int | None:
    try: return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError): return None


def price_key(value: float) -> str:
    return format(value, ".15g")


def scalar_key(value: Any) -> str:
    parsed = optional_float(value)
    return "null" if parsed is None else price_key(parsed)


def numeric_map(value: Any) -> dict[str, float]:
    if not isinstance(value, dict): return {}
    return {str(key): parsed for key, item in value.items() if (parsed := optional_float(item)) is not None}


def integer_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict): return {}
    return {str(key): parsed for key, item in value.items() if (parsed := safe_int(item)) is not None}


def relative_label(path: Path) -> str:
    try: return str(path.relative_to(ROOT_DIR)).replace("\\", "/")
    except ValueError: return str(path).replace("\\", "/")


if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: print("Stopped.", flush=True)
