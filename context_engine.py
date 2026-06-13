import asyncio
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
OUTPUT_FILE = DATA_DIR / "context_dna.jsonl"

FULL_DEBUG = False
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10
SYMBOL = "BTCUSDT"

SOURCE_FILES = {
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

ATR_PERIODS = (14, 20, 50)
VOLUME_PERIODS = (20, 50, 100)


@dataclass
class NormalizedCandle:
    symbol: str
    timeframe: str
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
    has_trade: bool
    data_quality: dict[str, Any]
    valid: bool
    errors: list[str] = field(default_factory=list)


@dataclass
class TimeframeContextState:
    previous_close: float | None = None
    tr_values: list[float] = field(default_factory=list)
    atr_values: dict[int, float | None] = field(default_factory=lambda: {period: None for period in ATR_PERIODS})
    cumulative_pv: float = 0.0
    cumulative_volume: float = 0.0
    cvd: float = 0.0
    volume_history: list[float] = field(default_factory=list)


class ContextEngine:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.output_handle = OUTPUT_FILE.open("a", encoding="utf-8")
        self.written_keys = load_written_keys()
        self.states = {timeframe: TimeframeContextState() for timeframe in SOURCE_FILES}
        self.processed = {timeframe: 0 for timeframe in SOURCE_FILES}
        self.context_rows_written = 0
        self.missing_inputs: set[str] = set()

    def close(self) -> None:
        self.output_handle.close()

    def process_row(self, row: dict[str, Any], timeframe: str, source_file: Path) -> None:
        candle = normalize_candle(row, timeframe)
        self.processed[timeframe] += 1

        key = (timeframe, candle.window_start_ts)
        if candle.window_start_ts is not None and key in self.written_keys:
            return

        context = self.build_context(candle)
        output = {
            "layer": "Layer-3",
            "engine": "ContextEngine",
            "symbol": candle.symbol,
            "timeframe": timeframe,
            "source_file": str(source_file.relative_to(ROOT_DIR)).replace("\\", "/"),
            "source_window_ts": candle.window_start_ts,
            "source_window_end_ts": candle.window_end_ts,
            "context": context,
            "data_quality": candle.data_quality,
        }
        if candle.errors:
            output["normalization_errors"] = candle.errors

        self.output_handle.write(json.dumps(output, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.output_handle.flush()
        if candle.window_start_ts is not None:
            self.written_keys.add(key)
        self.context_rows_written += 1

        if FULL_DEBUG:
            print(json.dumps(output, ensure_ascii=False), flush=True)

    def build_context(self, candle: NormalizedCandle) -> dict[str, Any]:
        state = self.states[candle.timeframe]
        atr = build_atr_context(candle, state)
        vwap = build_vwap_context(candle, state)
        cvd = build_cvd_context(candle, state)
        volume_context = build_volume_context(candle, state)
        return {
            "atr": atr,
            "vwap": vwap,
            "cvd": cvd,
            "volume_context": volume_context,
        }

    def heartbeat(self) -> None:
        print("Context Engine alive", flush=True)
        for timeframe in ("1S", "3S", "5S", "15S", "1M"):
            print(f"{timeframe} processed={self.processed[timeframe]}", flush=True)
        print(f"context_rows_written={self.context_rows_written}", flush=True)
        if self.missing_inputs:
            print(f"missing_inputs={sorted(self.missing_inputs)}", flush=True)


def load_written_keys() -> set[tuple[str, int | None]]:
    keys: set[tuple[str, int | None]] = set()
    if not OUTPUT_FILE.exists():
        return keys
    with OUTPUT_FILE.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
                keys.add((row.get("timeframe"), row.get("source_window_ts")))
            except json.JSONDecodeError:
                continue
    return keys


def normalize_candle(row: dict[str, Any], timeframe: str) -> NormalizedCandle:
    errors = []
    try:
        if timeframe == "1S":
            candle = row["candle_dna"]
            volume = candle
            trade_flow = candle
            ohlc = {
                "open": candle.get("open"),
                "high": candle.get("high"),
                "low": candle.get("low"),
                "close": candle.get("close"),
            }
            has_trade = bool(candle.get("has_trade", False))
        else:
            ohlc = row["ohlc"]
            volume = row["volume"]
            trade_flow = row["trade_flow"]
            has_trade = ohlc.get("open") is not None

        data_quality = row.get(
            "data_quality",
            {
                "quality_state": "unknown",
                "warning": "source_data_quality_missing",
            },
        )

        return NormalizedCandle(
            symbol=str(row.get("symbol", SYMBOL)),
            timeframe=timeframe,
            window_start_ts=int(row["window_start_ts"]),
            window_end_ts=int(row["window_end_ts"]),
            open=point_price(ohlc.get("open")),
            high=point_price(ohlc.get("high")),
            low=point_price(ohlc.get("low")),
            close=point_price(ohlc.get("close")),
            buy_volume=float(volume.get("buy_volume", 0.0)),
            sell_volume=float(volume.get("sell_volume", 0.0)),
            total_volume=float(volume.get("total_volume", 0.0)),
            delta=float(volume.get("delta", float(volume.get("buy_volume", 0.0)) - float(volume.get("sell_volume", 0.0)))),
            trade_count=int(trade_flow.get("trade_count", 0)),
            has_trade=has_trade,
            data_quality=data_quality,
            valid=True,
            errors=[],
        )
    except Exception as exc:
        errors.append(f"normalize_failed: {exc}")
        return NormalizedCandle(
            symbol=str(row.get("symbol", SYMBOL)),
            timeframe=timeframe,
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
            has_trade=False,
            data_quality={
                "quality_state": "invalid",
                "warning": "source_row_invalid",
            },
            valid=False,
            errors=errors,
        )


def point_price(point: dict[str, Any] | None) -> float | None:
    if point is None:
        return None
    return float(point["price"])


def safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def build_atr_context(candle: NormalizedCandle, state: TimeframeContextState) -> dict[str, Any]:
    if candle.high is None or candle.low is None or candle.close is None:
        return {
            str(period): {
                "period": period,
                "tr": None,
                "atr": None,
                "status": "insufficient_data",
            }
            for period in ATR_PERIODS
        }

    high_low = candle.high - candle.low
    if state.previous_close is None:
        tr = high_low
    else:
        tr = max(high_low, abs(candle.high - state.previous_close), abs(candle.low - state.previous_close))
    state.tr_values.append(tr)
    state.previous_close = candle.close

    output = {}
    for period in ATR_PERIODS:
        previous_atr = state.atr_values[period]
        if len(state.tr_values) < period:
            atr = None
            status = "warming"
        elif len(state.tr_values) == period:
            atr = sum(state.tr_values[-period:]) / period
            status = "ready"
        else:
            atr = ((previous_atr * (period - 1)) + tr) / period if previous_atr is not None else sum(state.tr_values[-period:]) / period
            status = "ready"
        state.atr_values[period] = atr
        output[str(period)] = {
            "period": period,
            "tr": tr,
            "atr": atr,
            "status": status,
        }
    return output


def build_vwap_context(candle: NormalizedCandle, state: TimeframeContextState) -> dict[str, Any]:
    if candle.high is None or candle.low is None or candle.close is None or candle.total_volume <= 0:
        return {
            "typical_price": None,
            "vwap": state.cumulative_pv / state.cumulative_volume if state.cumulative_volume > 0 else None,
            "cumulative_volume": state.cumulative_volume,
            "status": "insufficient_data",
        }

    typical_price = (candle.high + candle.low + candle.close) / 3
    state.cumulative_pv += typical_price * candle.total_volume
    state.cumulative_volume += candle.total_volume
    return {
        "typical_price": typical_price,
        "vwap": state.cumulative_pv / state.cumulative_volume,
        "cumulative_volume": state.cumulative_volume,
        "status": "ready",
    }


def build_cvd_context(candle: NormalizedCandle, state: TimeframeContextState) -> dict[str, Any]:
    previous_cvd = state.cvd
    state.cvd += candle.delta
    return {
        "delta": candle.delta,
        "cvd": state.cvd,
        "cvd_change": state.cvd - previous_cvd,
        "status": "ready",
    }


def build_volume_context(candle: NormalizedCandle, state: TimeframeContextState) -> dict[str, Any]:
    state.volume_history.append(candle.total_volume)
    output = {}
    for period in VOLUME_PERIODS:
        values = state.volume_history[-period:]
        mean_value = sum(values) / len(values)
        variance = sum((value - mean_value) ** 2 for value in values) / len(values)
        std_value = math.sqrt(variance)
        z_score = (candle.total_volume - mean_value) / std_value if std_value > 0 else 0.0
        percentile = percentile_rank(values, candle.total_volume)
        output[str(period)] = {
            "period": period,
            "rolling_mean": mean_value,
            "rolling_std": std_value,
            "z_score": z_score,
            "percentile": percentile,
            "status": "ready" if len(values) >= period else "warming",
        }
    return output


def percentile_rank(values: list[float], current: float) -> float:
    if not values:
        return 0.0
    below = sum(1 for value in values if value < current)
    equal = sum(1 for value in values if math.isclose(value, current, abs_tol=1e-9))
    return ((below + 0.5 * equal) / len(values)) * 100


async def follow_source(engine: ContextEngine, timeframe: str, path: Path) -> None:
    handle = None
    missing_reported = False
    try:
        while True:
            if handle is None:
                if not path.exists():
                    engine.missing_inputs.add(str(path.relative_to(ROOT_DIR)).replace("\\", "/"))
                    if not missing_reported:
                        print(f"Missing input: {path}", flush=True)
                        missing_reported = True
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
                engine.process_row(json.loads(stripped), timeframe, path)
            except json.JSONDecodeError as exc:
                invalid_row = {
                    "window_start_ts": None,
                    "window_end_ts": None,
                    "data_quality": {
                        "quality_state": "invalid",
                        "warning": f"invalid_json: {exc}",
                    },
                }
                engine.process_row(invalid_row, timeframe, path)
    finally:
        if handle is not None:
            handle.close()


async def heartbeat_loop(engine: ContextEngine) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        engine.heartbeat()


async def run() -> None:
    engine = ContextEngine()
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
