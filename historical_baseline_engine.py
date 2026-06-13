import argparse
import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from typing import Any


SYMBOL = "BTCUSDT"
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "historical_baseline_dna.jsonl"
FULL_PRINT = False
POLL_INTERVAL_SECONDS = 0.5
DAY_MS = 86_400_000
ATR_PERIOD = 14
BASELINE_WINDOWS = {
    "short": 20,
    "medium": 100,
    "long": 500,
}
EPSILON = 1e-9

TIMEFRAME_FILES = {
    "1S": DATA_DIR / "one_second_combined_dna.jsonl",
    "3S": DATA_DIR / "rolling_3s_dna.jsonl",
    "5S": DATA_DIR / "rolling_5s_dna.jsonl",
    "15S": DATA_DIR / "rolling_15s_dna.jsonl",
    "1M": DATA_DIR / "aligned_1m_candle_dna.jsonl",
    "5M": DATA_DIR / "aligned_5m_candle_dna.jsonl",
    "15M": DATA_DIR / "aligned_15m_candle_dna.jsonl",
    "1H": DATA_DIR / "aligned_1h_candle_dna.jsonl",
    "4H": DATA_DIR / "aligned_4h_candle_dna.jsonl",
    "1D": DATA_DIR / "aligned_1d_candle_dna.jsonl",
}

BASIC_METRICS = [
    "range",
    "total_volume",
    "buy_volume",
    "sell_volume",
    "delta",
    "absolute_delta",
    "trade_count",
    "footprint_price_level_count",
    "bid_update_count",
    "ask_update_count",
    "depth_balance",
    "depth_imbalance",
    "close_price",
]


@dataclass
class NormalizedRecord:
    symbol: str
    timeframe: str
    window_start_ts: int
    window_end_ts: int
    open_price: float | None
    high_price: float | None
    low_price: float | None
    close_price: float | None
    buy_volume: float
    sell_volume: float
    total_volume: float
    delta: float
    trade_count: int
    footprint_price_level_count: int
    bid_update_count: int
    ask_update_count: int
    depth_balance: int
    depth_imbalance: float
    true_range: float | None = None


@dataclass
class TimeframeState:
    records: list[NormalizedRecord] = field(default_factory=list)
    previous_close: float | None = None
    session_start_ts: int | None = None
    session_vwap_numerator: float = 0.0
    session_vwap_denominator: float = 0.0
    session_cvd: float = 0.0
    previous_session_cvd: float = 0.0


class HistoricalBaselineWriter:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.handle = OUTPUT_FILE.open("a", encoding="utf-8")

    def write(self, payload: dict[str, Any]) -> None:
        self.handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.handle.flush()

    def close(self) -> None:
        self.handle.close()


class HistoricalBaselineEngine:
    def __init__(self, writer: HistoricalBaselineWriter) -> None:
        self.writer = writer
        self.states = {
            timeframe: TimeframeState()
            for timeframe in TIMEFRAME_FILES
        }
        self.seen = {
            timeframe: set()
            for timeframe in TIMEFRAME_FILES
        }

    def process_payload(self, timeframe: str, source_file: Path, payload: dict[str, Any]) -> None:
        record = normalize_record(timeframe, payload)
        key = int(record.window_start_ts)
        if key in self.seen[timeframe]:
            return
        self.seen[timeframe].add(key)

        state = self.states[timeframe]
        record.true_range = calculate_true_range(record, state.previous_close)
        if record.close_price is not None:
            state.previous_close = record.close_price
        self._update_session_state(state, record)
        state.records.append(record)

        baseline = build_baseline_output(timeframe, source_file, state, record)
        errors = validate_baseline_output(baseline)
        if errors:
            print(
                f"Validation failed for baseline {timeframe} {record.window_start_ts}: "
                f"{'; '.join(errors)}",
                flush=True,
            )
            return

        self.writer.write(baseline)
        print_baseline_output(baseline)

    def _update_session_state(self, state: TimeframeState, record: NormalizedRecord) -> None:
        session_start = day_start(record.window_start_ts)
        if state.session_start_ts != session_start:
            state.session_start_ts = session_start
            state.session_vwap_numerator = 0.0
            state.session_vwap_denominator = 0.0
            state.session_cvd = 0.0
            state.previous_session_cvd = 0.0

        if (
            record.high_price is not None
            and record.low_price is not None
            and record.close_price is not None
            and record.total_volume > 0
        ):
            typical_price = (record.high_price + record.low_price + record.close_price) / 3
            state.session_vwap_numerator += typical_price * record.total_volume
            state.session_vwap_denominator += record.total_volume

        previous_cvd = state.session_cvd
        state.session_cvd += record.delta
        state.previous_session_cvd = previous_cvd


def normalize_record(timeframe: str, payload: dict[str, Any]) -> NormalizedRecord:
    if timeframe == "1S":
        candle = payload["candle_dna"]
        footprint = payload["footprint_dna"]
        depth = payload["depth_mutation_dna"]
        ohlc = {
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
        }
        volume = candle
        trade_count = int(candle["trade_count"])
        depth_balance = int(depth["balance"])
        depth_imbalance = float(depth["imbalance"])
    else:
        ohlc = payload["ohlc"]
        volume = payload["volume"]
        footprint = payload["footprint"]
        depth = payload["depth_flow"]
        trade_count = int(payload["trade_flow"]["trade_count"])
        depth_balance = int(depth["balance"])
        depth_imbalance = float(depth["imbalance"])

    return NormalizedRecord(
        symbol=str(payload.get("symbol", SYMBOL)),
        timeframe=timeframe,
        window_start_ts=int(payload["window_start_ts"]),
        window_end_ts=int(payload["window_end_ts"]),
        open_price=point_price(ohlc["open"]),
        high_price=point_price(ohlc["high"]),
        low_price=point_price(ohlc["low"]),
        close_price=point_price(ohlc["close"]),
        buy_volume=float(volume["buy_volume"]),
        sell_volume=float(volume["sell_volume"]),
        total_volume=float(volume["total_volume"]),
        delta=float(volume["delta"]),
        trade_count=trade_count,
        footprint_price_level_count=len(footprint["price_levels"]),
        bid_update_count=int(depth["bid_update_count"]),
        ask_update_count=int(depth["ask_update_count"]),
        depth_balance=depth_balance,
        depth_imbalance=depth_imbalance,
    )


def build_baseline_output(
    timeframe: str,
    source_file: Path,
    state: TimeframeState,
    latest_record: NormalizedRecord,
) -> dict[str, Any]:
    records = state.records
    metrics = {
        metric: build_metric_windows(records, metric)
        for metric in BASIC_METRICS
    }
    atr = build_atr_output(records)
    vwap = build_vwap_output(state, latest_record)
    cvd = build_cvd_output(state)

    return {
        "symbol": SYMBOL,
        "layer": "Layer-3",
        "engine": "HistoricalBaselineEngine",
        "timeframe": timeframe,
        "source_file": str(source_file).replace("\\", "/"),
        "generated_at_ts": int(time.time() * 1000),
        "record_window": {
            "first_ts": records[0].window_start_ts,
            "last_ts": records[-1].window_start_ts,
            "record_count": len(records),
        },
        "baseline_windows": BASELINE_WINDOWS,
        "metrics": metrics,
        "atr": atr,
        "vwap": vwap,
        "cvd": cvd,
    }


def build_metric_windows(records: list[NormalizedRecord], metric: str) -> dict[str, Any]:
    return {
        window_name: calculate_stats(
            [metric_value(record, metric) for record in records[-window_size:]]
        )
        for window_name, window_size in BASELINE_WINDOWS.items()
    }


def calculate_stats(values: list[float]) -> dict[str, Any]:
    cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not cleaned:
        return empty_stats()

    latest = cleaned[-1]
    mean_value = sum(cleaned) / len(cleaned)
    variance = sum((value - mean_value) ** 2 for value in cleaned) / len(cleaned)
    std_value = math.sqrt(variance)
    return {
        "sample_count": len(cleaned),
        "mean": mean_value,
        "median": median(cleaned),
        "min": min(cleaned),
        "max": max(cleaned),
        "std": std_value,
        "p10": percentile(cleaned, 10),
        "p25": percentile(cleaned, 25),
        "p50": percentile(cleaned, 50),
        "p75": percentile(cleaned, 75),
        "p90": percentile(cleaned, 90),
        "latest": latest,
        "latest_percentile": percentile_rank(cleaned, latest),
        "z_score": (latest - mean_value) / std_value if std_value > 0 else 0.0,
    }


def build_atr_output(records: list[NormalizedRecord]) -> dict[str, Any]:
    valid_tr = [
        float(record.true_range)
        for record in records
        if record.true_range is not None
    ]
    current_tr = valid_tr[-1] if valid_tr else 0.0
    atr = sum(valid_tr[-ATR_PERIOD:]) / min(len(valid_tr), ATR_PERIOD) if valid_tr else 0.0
    short_values = valid_tr[-BASELINE_WINDOWS["short"]:]
    medium_values = valid_tr[-BASELINE_WINDOWS["medium"]:]
    long_values = valid_tr[-BASELINE_WINDOWS["long"]:]
    atr_percentile_medium = percentile_rank(medium_values, atr) if medium_values else 0.0
    return {
        "atr_period": ATR_PERIOD,
        "current_tr": current_tr,
        "atr": atr,
        "atr_percentile_short": percentile_rank(short_values, atr) if short_values else 0.0,
        "atr_percentile_medium": atr_percentile_medium,
        "atr_percentile_long": percentile_rank(long_values, atr) if long_values else 0.0,
        "atr_z_score_medium": z_score(medium_values, atr) if medium_values else 0.0,
        "atr_status": atr_status(atr_percentile_medium),
    }


def build_vwap_output(state: TimeframeState, record: NormalizedRecord) -> dict[str, Any]:
    session_start = state.session_start_ts if state.session_start_ts is not None else day_start(record.window_start_ts)
    if state.session_vwap_denominator <= 0:
        return {
            "session_start_ts": session_start,
            "session_vwap": None,
            "price_vs_vwap": "unknown",
            "distance_to_vwap": None,
            "distance_to_vwap_pct": None,
        }

    session_vwap = state.session_vwap_numerator / state.session_vwap_denominator
    if record.close_price is None:
        price_vs_vwap = "unknown"
        distance = None
        distance_pct = None
    else:
        distance = record.close_price - session_vwap
        distance_pct = distance / session_vwap * 100 if session_vwap != 0 else None
        if record.close_price > session_vwap:
            price_vs_vwap = "above"
        elif record.close_price < session_vwap:
            price_vs_vwap = "below"
        else:
            price_vs_vwap = "at"

    return {
        "session_start_ts": session_start,
        "session_vwap": session_vwap,
        "price_vs_vwap": price_vs_vwap,
        "distance_to_vwap": distance,
        "distance_to_vwap_pct": distance_pct,
    }


def build_cvd_output(state: TimeframeState) -> dict[str, Any]:
    cvd_change = state.session_cvd - state.previous_session_cvd
    if cvd_change > 0:
        direction = "rising"
    elif cvd_change < 0:
        direction = "falling"
    else:
        direction = "flat"
    return {
        "session_start_ts": state.session_start_ts,
        "cvd": state.session_cvd,
        "cvd_change": cvd_change,
        "cvd_direction": direction,
    }


def validate_baseline_output(payload: dict[str, Any]) -> list[str]:
    errors = []
    if not payload["timeframe"]:
        errors.append("timeframe empty")
    if payload["record_window"]["record_count"] <= 0:
        errors.append("record_count <= 0")
    for metric in BASIC_METRICS:
        if metric not in payload["metrics"]:
            errors.append(f"missing metric {metric}")

    atr = payload["atr"]
    for key in ("atr_period", "current_tr", "atr", "atr_status"):
        if key not in atr:
            errors.append(f"missing atr {key}")
    vwap = payload["vwap"]
    for key in ("session_vwap", "price_vs_vwap"):
        if key not in vwap:
            errors.append(f"missing vwap {key}")
    cvd = payload["cvd"]
    for key in ("cvd", "cvd_direction"):
        if key not in cvd:
            errors.append(f"missing cvd {key}")

    for metric_name, windows in payload["metrics"].items():
        for window_name, stats in windows.items():
            latest_percentile = stats["latest_percentile"]
            if latest_percentile < 0 or latest_percentile > 100:
                errors.append(f"{metric_name}.{window_name} latest_percentile out of range")
            if stats["std"] < 0:
                errors.append(f"{metric_name}.{window_name} std negative")
            if not isinstance(stats["z_score"], (int, float)) or not math.isfinite(stats["z_score"]):
                errors.append(f"{metric_name}.{window_name} z_score invalid")
    return errors


def run_batch(engine: HistoricalBaselineEngine) -> None:
    for timeframe, source_file in TIMEFRAME_FILES.items():
        if not source_file.exists():
            print(f"Missing source skipped: {source_file}", flush=True)
            continue
        with source_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                process_line(engine, timeframe, source_file, line)


def run_live(engine: HistoricalBaselineEngine) -> None:
    handles = {}
    try:
        while True:
            for timeframe, source_file in TIMEFRAME_FILES.items():
                if timeframe not in handles:
                    if not source_file.exists():
                        continue
                    handles[timeframe] = source_file.open("r", encoding="utf-8")

                handle = handles[timeframe]
                while True:
                    line = handle.readline()
                    if not line:
                        break
                    process_line(engine, timeframe, source_file, line)
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in handles.values():
            handle.close()


def process_line(
    engine: HistoricalBaselineEngine,
    timeframe: str,
    source_file: Path,
    line: str,
) -> None:
    stripped = line.strip()
    if not stripped:
        return
    try:
        engine.process_payload(timeframe, source_file, json.loads(stripped))
    except json.JSONDecodeError as exc:
        print(f"Invalid JSONL ignored in {source_file}: {exc}", flush=True)
    except KeyError as exc:
        print(f"Missing field ignored in {source_file}: {exc}", flush=True)
    except Exception as exc:
        print(f"Baseline processing error in {source_file}: {exc}", flush=True)


def print_baseline_output(payload: dict[str, Any]) -> None:
    if FULL_PRINT:
        print(json.dumps(payload, indent=2, ensure_ascii=False), flush=True)
        return

    volume_pctl = payload["metrics"]["total_volume"]["medium"]["latest_percentile"]
    delta_z = payload["metrics"]["delta"]["medium"]["z_score"]
    print(
        f"[BASELINE {payload['timeframe']}] "
        f"records={payload['record_window']['record_count']} "
        f"atr={payload['atr']['atr']} "
        f"atr_status={payload['atr']['atr_status']} "
        f"vwap={payload['vwap']['session_vwap']} "
        f"cvd={payload['cvd']['cvd']} "
        f"volume_pctl={volume_pctl} "
        f"delta_z={delta_z}",
        flush=True,
    )


def metric_value(record: NormalizedRecord, metric: str) -> float | None:
    if metric == "range":
        if record.high_price is None or record.low_price is None:
            return 0.0
        return record.high_price - record.low_price
    if metric == "absolute_delta":
        return abs(record.delta)
    return getattr(record, metric)


def calculate_true_range(record: NormalizedRecord, previous_close: float | None) -> float | None:
    if record.high_price is None or record.low_price is None:
        return None
    high_low = record.high_price - record.low_price
    if previous_close is None:
        return high_low
    return max(
        high_low,
        abs(record.high_price - previous_close),
        abs(record.low_price - previous_close),
    )


def percentile(values: list[float], pct: float) -> float:
    cleaned = sorted(float(value) for value in values)
    if not cleaned:
        return 0.0
    if len(cleaned) == 1:
        return cleaned[0]
    rank = (pct / 100) * (len(cleaned) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return cleaned[int(rank)]
    weight = rank - lower
    return cleaned[lower] * (1 - weight) + cleaned[upper] * weight


def percentile_rank(values: list[float], latest: float) -> float:
    cleaned = [float(value) for value in values if math.isfinite(float(value))]
    if not cleaned:
        return 0.0
    below = sum(1 for value in cleaned if value < latest)
    equal = sum(1 for value in cleaned if same_float(value, latest))
    return ((below + 0.5 * equal) / len(cleaned)) * 100


def z_score(values: list[float], latest: float) -> float:
    cleaned = [float(value) for value in values if math.isfinite(float(value))]
    if not cleaned:
        return 0.0
    mean_value = sum(cleaned) / len(cleaned)
    variance = sum((value - mean_value) ** 2 for value in cleaned) / len(cleaned)
    std_value = math.sqrt(variance)
    return (latest - mean_value) / std_value if std_value > 0 else 0.0


def empty_stats() -> dict[str, Any]:
    return {
        "sample_count": 0,
        "mean": 0.0,
        "median": 0.0,
        "min": 0.0,
        "max": 0.0,
        "std": 0.0,
        "p10": 0.0,
        "p25": 0.0,
        "p50": 0.0,
        "p75": 0.0,
        "p90": 0.0,
        "latest": 0.0,
        "latest_percentile": 0.0,
        "z_score": 0.0,
    }


def atr_status(atr_percentile_medium: float) -> str:
    if atr_percentile_medium >= 90:
        return "extreme_high"
    if atr_percentile_medium >= 75:
        return "high"
    if atr_percentile_medium <= 10:
        return "extreme_low"
    if atr_percentile_medium <= 25:
        return "low"
    return "normal"


def point_price(point: dict[str, Any] | None) -> float | None:
    if point is None:
        return None
    return float(point["price"])


def day_start(timestamp_ms: int) -> int:
    return (timestamp_ms // DAY_MS) * DAY_MS


def same_float(left: float, right: float) -> bool:
    return abs(float(left) - float(right)) <= EPSILON


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Layer-3 Historical Baseline Engine")
    parser.add_argument(
        "--mode",
        choices=("batch", "live"),
        required=True,
        help="batch reads current files once; live follows files for new JSONL rows",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    writer = HistoricalBaselineWriter()
    engine = HistoricalBaselineEngine(writer)
    try:
        if args.mode == "batch":
            run_batch(engine)
        else:
            run_live(engine)
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
    finally:
        writer.close()


if __name__ == "__main__":
    main()
