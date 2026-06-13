import json
import time
from collections import deque
from pathlib import Path
from typing import Any


SYMBOL = "BTCUSDT"
INPUT_FILE = Path("data") / "one_second_combined_dna.jsonl"
DATA_DIR = Path("data")

WINDOW_SIZES = {
    3: DATA_DIR / "rolling_3s_dna.jsonl",
    5: DATA_DIR / "rolling_5s_dna.jsonl",
    15: DATA_DIR / "rolling_15s_dna.jsonl",
}

FULL_PRINT = False
POLL_INTERVAL_SECONDS = 0.25
EPSILON = 1e-9


class RollingWindowWriter:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.files = {
            size: path.open("a", encoding="utf-8")
            for size, path in WINDOW_SIZES.items()
        }

    def write(self, window_size: int, payload: dict[str, Any]) -> None:
        handle = self.files[window_size]
        handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        handle.flush()

    def close(self) -> None:
        for handle in self.files.values():
            handle.close()


class RollingWindowEngine:
    def __init__(self, writer: RollingWindowWriter) -> None:
        self.writer = writer
        self.windows = {
            size: deque(maxlen=size)
            for size in WINDOW_SIZES
        }

    def process_1s(self, combined_dna: dict[str, Any]) -> None:
        current_ts = int(combined_dna["window_start_ts"])
        self._reset_buffers_on_gap(current_ts)
        for size, source_window in self.windows.items():
            source_window.append(combined_dna)
            if len(source_window) == size:
                rolling_dna = build_rolling_dna(size, list(source_window))
                errors = validate_rolling_dna(rolling_dna)
                if errors:
                    if "source_windows non-contiguous" in errors:
                        print(
                            "Rolling window skipped due to non-contiguous source windows: "
                            f"{rolling_dna['source_refs']['first_1s_ts']}-"
                            f"{rolling_dna['source_refs']['last_1s_ts']}",
                            flush=True,
                        )
                        continue
                    print(
                        f"Validation failed for rolling {size}S "
                        f"{rolling_dna['window_start_ts']}-{rolling_dna['window_end_ts']}: "
                        f"{'; '.join(errors)}",
                        flush=True,
                    )
                    continue
                self.writer.write(size, rolling_dna)
                print_rolling_output(rolling_dna)

    def _reset_buffers_on_gap(self, current_ts: int) -> None:
        previous_values = [
            int(source_window[-1]["window_start_ts"])
            for source_window in self.windows.values()
            if source_window
        ]
        if not previous_values:
            return

        previous_ts = max(previous_values)
        if abs(current_ts - previous_ts) > 1000:
            for source_window in self.windows.values():
                source_window.clear()
            print(
                f"Rolling buffer reset due to time gap: previous={previous_ts} current={current_ts}",
                flush=True,
            )


def build_rolling_dna(window_size: int, source_items: list[dict[str, Any]]) -> dict[str, Any]:
    first = source_items[0]
    last = source_items[-1]
    candles = [item["candle_dna"] for item in source_items]
    footprints = [item["footprint_dna"] for item in source_items]
    depths = [item["depth_mutation_dna"] for item in source_items]

    buy_volume = sum(float(candle["buy_volume"]) for candle in candles)
    sell_volume = sum(float(candle["sell_volume"]) for candle in candles)
    total_volume = buy_volume + sell_volume
    delta = buy_volume - sell_volume

    trade_counts = [int(candle["trade_count"]) for candle in candles]
    active_seconds = sum(1 for candle in candles if candle["has_trade"])
    empty_seconds = window_size - active_seconds

    bid_update_count = sum(int(depth["bid_update_count"]) for depth in depths)
    ask_update_count = sum(int(depth["ask_update_count"]) for depth in depths)
    balance = bid_update_count - ask_update_count
    depth_total = bid_update_count + ask_update_count

    source_windows = [int(item["window_start_ts"]) for item in source_items]

    return {
        "symbol": SYMBOL,
        "window_type": f"{window_size}S",
        "window_size_seconds": window_size,
        "window_start_ts": int(first["window_start_ts"]),
        "window_end_ts": int(last["window_end_ts"]),
        "source_1s_count": len(source_items),
        "ohlc": build_ohlc(candles),
        "volume": {
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "total_volume": total_volume,
            "delta": delta,
            "delta_state": delta_state(delta),
        },
        "trade_flow": {
            "trade_count": sum(trade_counts),
            "active_seconds": active_seconds,
            "empty_seconds": empty_seconds,
            "avg_trade_count_per_second": sum(trade_counts) / len(source_items),
            "max_trade_count_1s": max(trade_counts),
            "min_trade_count_1s": min(trade_counts),
        },
        "footprint": {
            "price_levels": build_footprint_price_levels(footprints),
        },
        "depth_flow": {
            "bid_update_count": bid_update_count,
            "ask_update_count": ask_update_count,
            "dominant_side": dominant_depth_side(bid_update_count, ask_update_count),
            "balance": balance,
            "imbalance": balance / depth_total if depth_total > 0 else 0.0,
            "ratio": bid_update_count / ask_update_count if ask_update_count > 0 else None,
        },
        "micro_behavior": {
            "delta_sequence": [float(candle["delta"]) for candle in candles],
            "price_sequence": [extract_1s_price(candle) for candle in candles],
            "trade_count_sequence": trade_counts,
            "dominant_side_sequence": [depth["dominant_side"] for depth in depths],
        },
        "source_refs": {
            "first_1s_ts": source_windows[0],
            "last_1s_ts": source_windows[-1],
            "source_windows": source_windows,
        },
    }


def build_ohlc(candles: list[dict[str, Any]]) -> dict[str, Any]:
    trade_candles = [candle for candle in candles if candle["has_trade"]]
    if not trade_candles:
        return {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
        }

    high = trade_candles[0]["high"]
    low = trade_candles[0]["low"]
    for candle in trade_candles[1:]:
        if candle["high"]["price"] > high["price"]:
            high = candle["high"]
        if candle["low"]["price"] < low["price"]:
            low = candle["low"]

    return {
        "open": trade_candles[0]["open"],
        "high": high,
        "low": low,
        "close": trade_candles[-1]["close"],
    }


def build_footprint_price_levels(footprints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    levels: dict[float, dict[str, float | int]] = {}
    for footprint in footprints:
        for level in footprint["price_levels"]:
            price = float(level["price"])
            aggregate = levels.setdefault(
                price,
                {"buy_volume": 0.0, "sell_volume": 0.0, "trade_count": 0},
            )
            aggregate["buy_volume"] = float(aggregate["buy_volume"]) + float(level["buy_volume"])
            aggregate["sell_volume"] = float(aggregate["sell_volume"]) + float(level["sell_volume"])
            aggregate["trade_count"] = int(aggregate["trade_count"]) + int(level["trade_count"])

    price_levels = []
    for price in sorted(levels.keys(), reverse=True):
        aggregate = levels[price]
        buy_volume = float(aggregate["buy_volume"])
        sell_volume = float(aggregate["sell_volume"])
        total_volume = buy_volume + sell_volume
        delta = buy_volume - sell_volume
        price_levels.append(
            {
                "price": price,
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
                "total_volume": total_volume,
                "delta": delta,
                "delta_state": delta_state(delta),
                "trade_count": int(aggregate["trade_count"]),
            }
        )
    return price_levels


def validate_rolling_dna(rolling_dna: dict[str, Any]) -> list[str]:
    errors = []
    source_1s_count = int(rolling_dna["source_1s_count"])
    window_size_seconds = int(rolling_dna["window_size_seconds"])
    source_windows = rolling_dna["source_refs"]["source_windows"]
    volume = rolling_dna["volume"]
    footprint_levels = rolling_dna["footprint"]["price_levels"]
    trade_flow = rolling_dna["trade_flow"]
    micro_behavior = rolling_dna["micro_behavior"]

    if source_1s_count != window_size_seconds:
        errors.append("source_1s_count does not equal window_size_seconds")
    if len(source_windows) != source_1s_count:
        errors.append("source_windows length mismatch")
    if not source_windows_are_contiguous(source_windows, 1000):
        errors.append("source_windows non-contiguous")
    if not same_float(volume["total_volume"], volume["buy_volume"] + volume["sell_volume"]):
        errors.append("total_volume mismatch")
    if not same_float(volume["delta"], volume["buy_volume"] - volume["sell_volume"]):
        errors.append("delta mismatch")

    footprint_buy = sum(float(level["buy_volume"]) for level in footprint_levels)
    footprint_sell = sum(float(level["sell_volume"]) for level in footprint_levels)
    if not same_float(footprint_buy, volume["buy_volume"]):
        errors.append("footprint buy_volume mismatch")
    if not same_float(footprint_sell, volume["sell_volume"]):
        errors.append("footprint sell_volume mismatch")

    if trade_flow["active_seconds"] + trade_flow["empty_seconds"] != source_1s_count:
        errors.append("active_seconds + empty_seconds mismatch")
    if len(micro_behavior["delta_sequence"]) != source_1s_count:
        errors.append("delta_sequence length mismatch")
    if len(micro_behavior["price_sequence"]) != source_1s_count:
        errors.append("price_sequence length mismatch")
    if len(micro_behavior["trade_count_sequence"]) != source_1s_count:
        errors.append("trade_count_sequence length mismatch")
    if len(micro_behavior["dominant_side_sequence"]) != source_1s_count:
        errors.append("dominant_side_sequence length mismatch")

    return errors


def follow_input_file(engine: RollingWindowEngine) -> None:
    while not INPUT_FILE.exists():
        print(f"Waiting for input file: {INPUT_FILE}", flush=True)
        time.sleep(1)

    with INPUT_FILE.open("r", encoding="utf-8") as handle:
        while True:
            line = handle.readline()
            if not line:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            stripped = line.strip()
            if not stripped:
                continue

            try:
                engine.process_1s(json.loads(stripped))
            except json.JSONDecodeError as exc:
                print(f"Invalid JSONL line ignored: {exc}", flush=True)
            except KeyError as exc:
                print(f"Missing input field ignored: {exc}", flush=True)
            except Exception as exc:
                print(f"Rolling processing error: {exc}", flush=True)


def print_rolling_output(rolling_dna: dict[str, Any]) -> None:
    if FULL_PRINT:
        print(json.dumps(rolling_dna, indent=2, ensure_ascii=False), flush=True)
        return

    close = rolling_dna["ohlc"]["close"]
    price = close["price"] if close is not None else None
    print(
        f"[ROLLING {rolling_dna['window_type']}] "
        f"ts={rolling_dna['window_start_ts']}-{rolling_dna['window_end_ts']} "
        f"price={format_price(price)} "
        f"trades={rolling_dna['trade_flow']['trade_count']} "
        f"delta={format_float(rolling_dna['volume']['delta'], 6)} "
        f"levels={len(rolling_dna['footprint']['price_levels'])} "
        f"dominant={rolling_dna['depth_flow']['dominant_side']}",
        flush=True,
    )


def extract_1s_price(candle: dict[str, Any]) -> float | None:
    if candle["has_trade"]:
        return candle["close"]["price"]
    return candle["carry_forward_price"]


def source_windows_are_contiguous(source_windows: list[int], step_ms: int) -> bool:
    return all(
        int(source_windows[index]) + step_ms == int(source_windows[index + 1])
        for index in range(len(source_windows) - 1)
    )


def format_price(value: float | None) -> str:
    if value is None:
        return "null"
    return f"{float(value):.2f}"


def format_float(value: float | None, places: int) -> str:
    if value is None:
        return "null"
    return f"{float(value):.{places}f}".rstrip("0").rstrip(".")


def delta_state(delta: float) -> str:
    if delta > 0:
        return "positive"
    if delta < 0:
        return "negative"
    return "neutral"


def dominant_depth_side(bid_update_count: int, ask_update_count: int) -> str:
    if bid_update_count > ask_update_count:
        return "bid"
    if ask_update_count > bid_update_count:
        return "ask"
    return "neutral"


def same_float(left: float, right: float) -> bool:
    return abs(float(left) - float(right)) <= EPSILON


def main() -> None:
    writer = RollingWindowWriter()
    engine = RollingWindowEngine(writer)
    try:
        follow_input_file(engine)
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
    finally:
        writer.close()


if __name__ == "__main__":
    main()
