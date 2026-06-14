import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from detector_contracts import (
    CONTRACT_VERSION,
    DETECTOR_CONTRACTS,
    get_detector_contract,
    validate_detector_contracts,
)


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"

SOURCE_FILES = {
    "1S": DATA_DIR / "one_second_combined_dna.jsonl",
    "3S": DATA_DIR / "rolling_3s_dna.jsonl",
    "5S": DATA_DIR / "rolling_5s_dna.jsonl",
    "15S": DATA_DIR / "rolling_15s_dna.jsonl",
    "1M": DATA_DIR / "aligned_1m_candle_dna.jsonl",
}
CONTEXT_FILE = DATA_DIR / "context_dna.jsonl"

MEASUREMENTS_FILE = DATA_DIR / "detector_measurements.jsonl"
DETECTOR_EVENTS_FILE = DATA_DIR / "detector_events.jsonl"
EVIDENCE_INBOX_FILE = DATA_DIR / "evidence_inbox.jsonl"
HEALTH_FILE = DATA_DIR / "detector_health.json"

POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10
SYMBOL = "BTCUSDT"


@dataclass
class NormalizedRow:
    symbol: str
    timeframe: str
    source_file: str
    window_start_ts: int | None
    window_end_ts: int | None
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    buy_volume: float
    sell_volume: float
    total_volume: float
    delta: float
    trade_count: int
    footprint_levels: list[dict[str, Any]]
    bid_update_count: int | None
    ask_update_count: int | None
    dominant_side: str
    data_quality: dict[str, Any]
    valid: bool
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RuntimeEvent:
    contract_name: str
    event_type: str
    side: str
    direction: str
    condition: str


class DetectorEngine:
    def __init__(self) -> None:
        registry_errors = validate_detector_contracts()
        if registry_errors:
            raise RuntimeError("Detector contract registry validation failed: " + "; ".join(registry_errors))
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.contracts = {contract["detector_name"]: contract for contract in DETECTOR_CONTRACTS}
        self.measurement_handle = MEASUREMENTS_FILE.open("a", encoding="utf-8")
        self.detector_handle = DETECTOR_EVENTS_FILE.open("a", encoding="utf-8")
        self.evidence_handle = EVIDENCE_INBOX_FILE.open("a", encoding="utf-8")
        self.measurement_keys = load_measurement_keys()
        self.event_keys = load_event_keys()
        self.context_index = load_context_index()
        self.processed_rows = {timeframe: 0 for timeframe in SOURCE_FILES}
        self.measurements_written = 0
        self.detector_events_written = 0
        self.evidence_events_written = 0
        self.runtime_validation_errors = 0
        self.detectors_run: set[str] = set()
        self.last_event_ts = 0
        self.missing_inputs: set[str] = set()
        self.write_health()

    def close(self) -> None:
        self.write_health()
        self.measurement_handle.close()
        self.detector_handle.close()
        self.evidence_handle.close()

    def process_row(self, row: dict[str, Any], timeframe: str, source_file: Path) -> None:
        normalized = normalize_row(row, timeframe, source_file)
        self.processed_rows[timeframe] += 1
        if not normalized.valid or normalized.window_start_ts is None or normalized.window_end_ts is None:
            return

        metrics = calculate_metrics(normalized)
        context_refs = build_context_refs(self.context_index, timeframe, normalized.window_start_ts)
        measurement_id = make_record_id("measurement", timeframe, normalized.window_start_ts)
        measurement_key = (timeframe, normalized.window_start_ts)

        if measurement_key not in self.measurement_keys:
            measurement = {
                "layer": "Layer-4",
                "engine": "MeasurementDetectorEngine",
                "record_type": "measurement",
                "measurement_id": measurement_id,
                "symbol": normalized.symbol,
                "timeframe": timeframe,
                "window_start_ts": normalized.window_start_ts,
                "window_end_ts": normalized.window_end_ts,
                "source_file": normalized.source_file,
                "metrics": metrics,
                "context_refs": context_refs,
                "data_quality": normalized.data_quality,
            }
            write_jsonl(self.measurement_handle, measurement)
            self.measurement_keys.add(measurement_key)
            self.measurements_written += 1

        for event in observed_events(normalized, metrics):
            validation_errors = validate_runtime_event(event, normalized)
            if validation_errors:
                self.runtime_validation_errors += 1
                continue
            contract = self.contracts[event.contract_name]
            event_key = (event.event_type, timeframe, normalized.window_start_ts)
            if event_key in self.event_keys:
                continue

            event_id = make_record_id(event.event_type, timeframe, normalized.window_start_ts)
            source_refs = {
                "source_file": normalized.source_file,
                "source_window_ts": normalized.window_start_ts,
                "source_window_end_ts": normalized.window_end_ts,
            }
            detector_event = {
                "layer": "Layer-4",
                "engine": "MeasurementDetectorEngine",
                "record_type": "detector_event",
                "detector_event_id": event_id,
                "symbol": normalized.symbol,
                "timeframe": timeframe,
                "window_start_ts": normalized.window_start_ts,
                "window_end_ts": normalized.window_end_ts,
                "event_type": event.event_type,
                "contract_name": event.contract_name,
                "contract_version": contract["contract_version"],
                "status": "candidate",
                "calibration_status": contract["calibration_status"],
                "validation_passed": True,
                "side": event.side,
                "direction": event.direction,
                "confidence": None,
                "strength_score": None,
                "thresholds": None,
                "reason": {
                    "structural_condition": event.condition,
                    "numeric_threshold_used": False,
                },
                "measurement_ref": measurement_id,
                "raw_metrics": metrics,
                "context_refs": context_refs,
                "source_refs": source_refs,
                "data_quality": normalized.data_quality,
            }
            evidence = {
                "source_layer": "Layer-4",
                "source_engine": "MeasurementDetectorEngine",
                "evidence_type": "detector_candidate",
                "event_type": event.event_type,
                "contract_name": event.contract_name,
                "contract_version": contract["contract_version"],
                "symbol": normalized.symbol,
                "timeframe": timeframe,
                "window_start_ts": normalized.window_start_ts,
                "window_end_ts": normalized.window_end_ts,
                "calibration_status": contract["calibration_status"],
                "validation_passed": True,
                "confidence": None,
                "strength_score": None,
                "detector_event_id": event_id,
            }
            write_jsonl(self.detector_handle, detector_event)
            write_jsonl(self.evidence_handle, evidence)
            self.event_keys.add(event_key)
            self.detector_events_written += 1
            self.evidence_events_written += 1
            self.detectors_run.add(event.contract_name)
            self.last_event_ts = normalized.window_start_ts

    def write_health(self) -> None:
        payload = {
            "status": "alive",
            "processed_rows": self.processed_rows,
            "measurements_written": self.measurements_written,
            "detector_events_written": self.detector_events_written,
            "evidence_events_written": self.evidence_events_written,
            "detectors_run": sorted(self.detectors_run),
            "runtime_validation_errors": self.runtime_validation_errors,
            "registry_contract_version": CONTRACT_VERSION,
            "registry_validation_passed": True,
            "last_event_ts": self.last_event_ts,
            "missing_inputs": sorted(self.missing_inputs),
        }
        HEALTH_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def heartbeat(self) -> None:
        self.write_health()
        print("Detector Engine alive", flush=True)
        for timeframe in ("1S", "3S", "5S", "15S", "1M"):
            print(f"{timeframe} processed={self.processed_rows[timeframe]}", flush=True)
        print(f"measurements_written={self.measurements_written}", flush=True)
        print(f"detector_events_written={self.detector_events_written}", flush=True)
        print(f"evidence_events_written={self.evidence_events_written}", flush=True)
        print(f"detectors_run={len(self.detectors_run)}", flush=True)
        print(f"runtime_validation_errors={self.runtime_validation_errors}", flush=True)


def load_measurement_keys() -> set[tuple[str, int]]:
    keys: set[tuple[str, int]] = set()
    for row in read_jsonl(MEASUREMENTS_FILE):
        timeframe = row.get("timeframe")
        window_start_ts = safe_int(row.get("window_start_ts"))
        if timeframe in SOURCE_FILES and window_start_ts is not None:
            keys.add((timeframe, window_start_ts))
    return keys


def load_event_keys() -> set[tuple[str, str, int]]:
    keys: set[tuple[str, str, int]] = set()
    for row in read_jsonl(DETECTOR_EVENTS_FILE):
        event_type = row.get("event_type")
        timeframe = row.get("timeframe")
        window_start_ts = safe_int(row.get("window_start_ts"))
        if event_type and timeframe in SOURCE_FILES and window_start_ts is not None:
            keys.add((str(event_type), timeframe, window_start_ts))
    return keys


def load_context_index() -> dict[tuple[str, int], dict[str, Any]]:
    index: dict[tuple[str, int], dict[str, Any]] = {}
    for row in read_jsonl(CONTEXT_FILE):
        timeframe = row.get("timeframe")
        source_window_ts = safe_int(row.get("source_window_ts"))
        if timeframe in SOURCE_FILES and source_window_ts is not None:
            index[(timeframe, source_window_ts)] = row
    return index


def read_jsonl(path: Path):
    if not path.exists():
        return
    snapshot_size = path.stat().st_size
    with path.open("rb") as handle:
        while handle.tell() < snapshot_size:
            line = handle.readline(snapshot_size - handle.tell())
            if not line.endswith(b"\n"):
                break
            try:
                yield json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
                continue


def normalize_row(row: dict[str, Any], timeframe: str, source_file: Path) -> NormalizedRow:
    source_file_label = str(source_file.relative_to(ROOT_DIR)).replace("\\", "/")
    try:
        if timeframe == "1S":
            candle = row["candle_dna"]
            footprint = row["footprint_dna"]
            depth = row.get("depth_mutation_dna", row.get("depth_dna", {}))
            ohlc = candle
            volume = candle
            trade_flow = candle
        else:
            ohlc = row["ohlc"]
            volume = row["volume"]
            trade_flow = row["trade_flow"]
            footprint = row["footprint"]
            depth = row.get("depth_flow", {})

        window_start_ts = int(row["window_start_ts"])
        window_end_ts = int(row["window_end_ts"])
        buy_volume = float(volume.get("buy_volume", 0.0))
        sell_volume = float(volume.get("sell_volume", 0.0))
        total_volume = float(volume.get("total_volume", buy_volume + sell_volume))
        delta = float(volume.get("delta", buy_volume - sell_volume))
        dominant_side = str(depth.get("dominant_side", "unknown"))
        if dominant_side not in ("bid", "ask", "neutral"):
            dominant_side = "unknown"

        return NormalizedRow(
            symbol=str(row.get("symbol", SYMBOL)),
            timeframe=timeframe,
            source_file=source_file_label,
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
            open=point_price(ohlc.get("open")),
            high=point_price(ohlc.get("high")),
            low=point_price(ohlc.get("low")),
            close=point_price(ohlc.get("close")),
            buy_volume=buy_volume,
            sell_volume=sell_volume,
            total_volume=total_volume,
            delta=delta,
            trade_count=int(trade_flow.get("trade_count", 0)),
            footprint_levels=list(footprint.get("price_levels", [])),
            bid_update_count=safe_int(depth.get("bid_update_count")),
            ask_update_count=safe_int(depth.get("ask_update_count")),
            dominant_side=dominant_side,
            data_quality=row.get(
                "data_quality",
                {"quality_state": "unknown", "warning": "source_data_quality_missing"},
            ),
            valid=True,
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        return NormalizedRow(
            symbol=str(row.get("symbol", SYMBOL)),
            timeframe=timeframe,
            source_file=source_file_label,
            window_start_ts=safe_int(row.get("window_start_ts")),
            window_end_ts=safe_int(row.get("window_end_ts")),
            open=None,
            high=None,
            low=None,
            close=None,
            buy_volume=0.0,
            sell_volume=0.0,
            total_volume=0.0,
            delta=0.0,
            trade_count=0,
            footprint_levels=[],
            bid_update_count=None,
            ask_update_count=None,
            dominant_side="unknown",
            data_quality={"quality_state": "invalid", "warning": "source_row_invalid"},
            valid=False,
            errors=[f"normalize_failed: {exc}"],
        )


def calculate_metrics(row: NormalizedRow) -> dict[str, Any]:
    price_range = difference(row.high, row.low)
    body = difference(row.close, row.open)
    close_position = None
    if price_range is not None and price_range != 0 and row.close is not None and row.low is not None:
        close_position = (row.close - row.low) / price_range

    delta_ratio = row.delta / row.total_volume if row.total_volume != 0 else None
    buy_ratio = row.buy_volume / row.total_volume if row.total_volume != 0 else None
    sell_ratio = row.sell_volume / row.total_volume if row.total_volume != 0 else None
    avg_trade_size = row.total_volume / row.trade_count if row.trade_count != 0 else None

    depth_balance = None
    depth_imbalance = None
    if row.bid_update_count is not None and row.ask_update_count is not None:
        depth_balance = float(row.bid_update_count - row.ask_update_count)
        total_updates = row.bid_update_count + row.ask_update_count
        if total_updates != 0:
            depth_imbalance = depth_balance / total_updates

    max_level = max(
        row.footprint_levels,
        key=lambda level: safe_float(level.get("total_volume"), 0.0),
        default=None,
    )
    max_level_volume = optional_float(max_level, "total_volume")
    concentration = None
    if max_level_volume is not None and row.total_volume != 0:
        concentration = max_level_volume / row.total_volume

    return {
        "price": {
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "range": price_range,
            "body": body,
            "abs_body": abs(body) if body is not None else None,
            "price_change": body,
            "close_position": close_position,
        },
        "volume": {
            "buy_volume": row.buy_volume,
            "sell_volume": row.sell_volume,
            "total_volume": row.total_volume,
            "delta": row.delta,
            "delta_abs": abs(row.delta),
            "delta_ratio": delta_ratio,
            "buy_ratio": buy_ratio,
            "sell_ratio": sell_ratio,
        },
        "flow": {
            "trade_count": row.trade_count,
            "avg_trade_size": avg_trade_size,
        },
        "depth": {
            "bid_update_count": row.bid_update_count,
            "ask_update_count": row.ask_update_count,
            "dominant_side": row.dominant_side,
            "depth_balance": depth_balance,
            "depth_imbalance": depth_imbalance,
        },
        "footprint": {
            "price_level_count": len(row.footprint_levels),
            "max_level_volume": max_level_volume,
            "max_level_price": optional_float(max_level, "price"),
            "max_level_delta": optional_float(max_level, "delta"),
            "max_level_trade_count": optional_int(max_level, "trade_count"),
            "footprint_volume_concentration": concentration,
        },
    }


def observed_events(
    row: NormalizedRow, metrics: dict[str, Any]
) -> list[RuntimeEvent]:
    events: list[RuntimeEvent] = []
    delta = row.delta
    open_price = row.open
    close_price = row.close

    events.append(RuntimeEvent("delta_imbalance_candidate", "delta_imbalance_candidate", delta_side(delta), "unknown", "delta sign and volume ratios are measurable"))
    if row.trade_count > 0 or row.total_volume > 0:
        events.append(RuntimeEvent("aggression_burst_candidate", "aggression_burst_candidate", delta_side(delta), "unknown", "trade_count or total_volume records measurable activity"))

    if open_price is not None and close_price is not None:
        direction = price_direction(open_price, close_price)
        events.append(RuntimeEvent("momentum_candidate", "momentum_candidate", direction_side(direction), direction, "open and close relation is measurable"))
        if delta > 0 and close_price > open_price:
            events.append(RuntimeEvent("initiative_flow_candidate", "initiative_buyer_candidate", "buy", "up", "delta > 0 and close > open"))
        if delta < 0 and close_price < open_price:
            events.append(RuntimeEvent("initiative_flow_candidate", "initiative_seller_candidate", "sell", "down", "delta < 0 and close < open"))
        if delta > 0 and close_price < open_price:
            events.append(RuntimeEvent("trapped_trader_candidate", "trapped_buyer_candidate", "sell", "down", "delta > 0 and close < open"))
        if delta < 0 and close_price > open_price:
            events.append(RuntimeEvent("trapped_trader_candidate", "trapped_seller_candidate", "buy", "up", "delta < 0 and close > open"))
        if delta > 0 and close_price <= open_price:
            events.append(RuntimeEvent("absorption_candidate", "absorption_candidate", "sell", direction, "delta > 0 and price response is not upward"))
        if delta < 0 and close_price >= open_price:
            events.append(RuntimeEvent("absorption_candidate", "absorption_candidate", "buy", direction, "delta < 0 and price response is not downward"))
        if delta < 0 and close_price >= open_price:
            events.append(RuntimeEvent("responsive_buyer_candidate", "responsive_buyer_candidate", "buy", direction, "delta < 0 and close >= open"))
        if delta > 0 and close_price <= open_price:
            events.append(RuntimeEvent("responsive_seller_candidate", "responsive_seller_candidate", "sell", direction, "delta > 0 and close <= open"))
        if (delta == 0 and row.total_volume > 0) or (delta > 0 and close_price <= open_price) or (delta < 0 and close_price >= open_price):
            events.append(RuntimeEvent("exhaustion_candidate", "exhaustion_candidate", exhaustion_side(delta), direction, "flow exists with neutral or contradictory directional result"))

    footprint = metrics["footprint"]
    if footprint["price_level_count"] > 1 and row.high is not None and row.low is not None and row.high != row.low:
        direction = price_direction(open_price, close_price)
        events.append(RuntimeEvent("sweep_candidate", "sweep_candidate", direction_side(direction), direction, "footprint price_level_count > 1 and high != low"))
    if (
        footprint["max_level_trade_count"] is not None
        and footprint["max_level_price"] is not None
        and footprint["footprint_volume_concentration"] is not None
    ):
        max_level_delta = footprint["max_level_delta"]
        iceberg_side = "neutral" if max_level_delta is None else opposite_delta_side(max_level_delta)
        events.append(RuntimeEvent("iceberg_candidate", "iceberg_candidate", iceberg_side, "unknown", "footprint concentration is measurable; public stream cannot confirm iceberg"))
    return events


def validate_runtime_event(event: RuntimeEvent, row: NormalizedRow) -> list[str]:
    errors: list[str] = []
    contract = get_detector_contract(event.contract_name)
    if contract is None:
        return [f"undefined detector contract: {event.contract_name}"]
    allowed = contract["output_schema"]["event_type"]
    allowed_events = set(allowed if isinstance(allowed, list) else [allowed])
    if event.event_type not in allowed_events:
        errors.append(f"undefined event_type for {event.contract_name}: {event.event_type}")
    for field_name in contract["required_fields"]:
        value = runtime_field_value(row, field_name)
        if value is None:
            errors.append(f"required field missing: {field_name}")
    if row.total_volume < 0 or row.trade_count < 0:
        errors.append("negative volume or trade_count")
    if row.high is not None and row.low is not None and row.high < row.low:
        errors.append("high is below low")
    if abs(row.delta - (row.buy_volume - row.sell_volume)) > 1e-9:
        errors.append("delta does not equal buy_volume - sell_volume")
    return errors


def runtime_field_value(row: NormalizedRow, field_name: str) -> Any:
    if field_name == "price_level_count":
        return len(row.footprint_levels)
    return getattr(row, field_name, None)


def build_context_refs(
    context_index: dict[tuple[str, int], dict[str, Any]], timeframe: str, window_start_ts: int
) -> dict[str, Any]:
    context = context_index.get((timeframe, window_start_ts))
    refs: dict[str, Any] = {
        "context_missing": context is None,
        "context_key": {"timeframe": timeframe, "source_window_ts": window_start_ts},
    }
    if context is not None:
        refs["context_window_end_ts"] = context.get("source_window_end_ts")
    return refs


def make_record_id(record_type: str, timeframe: str, window_start_ts: int) -> str:
    raw = f"{record_type}|{timeframe}|{window_start_ts}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    prefix = "measurement" if record_type == "measurement" else "detector"
    return f"{prefix}_{digest}"


def write_jsonl(handle, payload: dict[str, Any]) -> None:
    handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
    handle.flush()


def point_price(point: Any) -> float | None:
    if point is None:
        return None
    if isinstance(point, dict):
        point = point.get("price")
    return float(point) if point is not None else None


def difference(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return default


def optional_float(row: dict[str, Any] | None, key: str) -> float | None:
    if row is None or row.get(key) is None:
        return None
    try:
        return float(row[key])
    except (TypeError, ValueError, OverflowError):
        return None


def optional_int(row: dict[str, Any] | None, key: str) -> int | None:
    if row is None:
        return None
    return safe_int(row.get(key))


def delta_side(delta: float) -> str:
    if delta > 0:
        return "buy"
    if delta < 0:
        return "sell"
    return "neutral"


def opposite_delta_side(delta: float) -> str:
    if delta > 0:
        return "sell"
    if delta < 0:
        return "buy"
    return "neutral"


def exhaustion_side(delta: float) -> str:
    return opposite_delta_side(delta) if delta != 0 else "neutral"


def direction_side(direction: str) -> str:
    if direction == "up":
        return "buy"
    if direction == "down":
        return "sell"
    return "neutral"


def price_direction(open_price: float | None, close_price: float | None) -> str:
    if open_price is None or close_price is None:
        return "unknown"
    if close_price > open_price:
        return "up"
    if close_price < open_price:
        return "down"
    return "flat"


async def follow_source(engine: DetectorEngine, timeframe: str, path: Path) -> None:
    handle = None
    missing_reported = False
    try:
        while True:
            if handle is None:
                if not path.exists():
                    label = str(path.relative_to(ROOT_DIR)).replace("\\", "/")
                    engine.missing_inputs.add(label)
                    engine.write_health()
                    if not missing_reported:
                        print(f"Missing input: {label}", flush=True)
                        missing_reported = True
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue
                handle = path.open("r", encoding="utf-8")
                label = str(path.relative_to(ROOT_DIR)).replace("\\", "/")
                engine.missing_inputs.discard(label)

            line = handle.readline()
            if not line:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue
            stripped = line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                engine.processed_rows[timeframe] += 1
                continue
            if isinstance(parsed, dict):
                engine.process_row(parsed, timeframe, path)
            else:
                engine.processed_rows[timeframe] += 1
    finally:
        if handle is not None:
            handle.close()


async def heartbeat_loop(engine: DetectorEngine) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        engine.heartbeat()


async def run() -> None:
    engine = DetectorEngine()
    tasks = [
        asyncio.create_task(follow_source(engine, timeframe, path))
        for timeframe, path in SOURCE_FILES.items()
    ]
    tasks.append(asyncio.create_task(heartbeat_loop(engine)))
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
