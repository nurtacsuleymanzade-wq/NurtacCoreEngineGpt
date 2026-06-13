import json
import time
from pathlib import Path
from typing import Any


SYMBOL = "BTCUSDT"
INPUT_FILE = Path("data") / "one_second_combined_dna.jsonl"
DATA_DIR = Path("data")

FULL_PRINT = False
POLL_INTERVAL_SECONDS = 0.25
EPSILON = 1e-9

TIMEFRAMES = {
    "1M": {
        "source_timeframe": "1S",
        "source_count": 60,
        "duration_ms": 60_000,
        "output": DATA_DIR / "aligned_1m_candle_dna.jsonl",
    },
    "5M": {
        "source_timeframe": "1M",
        "source_count": 5,
        "duration_ms": 300_000,
        "output": DATA_DIR / "aligned_5m_candle_dna.jsonl",
    },
    "15M": {
        "source_timeframe": "5M",
        "source_count": 3,
        "duration_ms": 900_000,
        "output": DATA_DIR / "aligned_15m_candle_dna.jsonl",
    },
    "1H": {
        "source_timeframe": "15M",
        "source_count": 4,
        "duration_ms": 3_600_000,
        "output": DATA_DIR / "aligned_1h_candle_dna.jsonl",
    },
    "4H": {
        "source_timeframe": "1H",
        "source_count": 4,
        "duration_ms": 14_400_000,
        "output": DATA_DIR / "aligned_4h_candle_dna.jsonl",
    },
    "1D": {
        "source_timeframe": "4H",
        "source_count": 6,
        "duration_ms": 86_400_000,
        "output": DATA_DIR / "aligned_1d_candle_dna.jsonl",
    },
}

PIPELINE = ["1M", "5M", "15M", "1H", "4H", "1D"]


class AlignedCandleWriter:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.files = {
            timeframe: config["output"].open("a", encoding="utf-8")
            for timeframe, config in TIMEFRAMES.items()
        }

    def write(self, timeframe: str, payload: dict[str, Any]) -> None:
        handle = self.files[timeframe]
        handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        handle.flush()

    def close(self) -> None:
        for handle in self.files.values():
            handle.close()


class AlignedCandleEngine:
    def __init__(self, writer: AlignedCandleWriter) -> None:
        self.writer = writer
        self.buckets: dict[str, dict[int, list[dict[str, Any]]]] = {
            timeframe: {}
            for timeframe in PIPELINE
        }
        self.emitted: dict[str, set[int]] = {
            timeframe: set()
            for timeframe in PIPELINE
        }

    def process_1s(self, combined_dna: dict[str, Any]) -> None:
        unit = normalize_1s_unit(combined_dna)
        self._add_source_unit("1M", unit)

    def _add_source_unit(self, target_timeframe: str, source_unit: dict[str, Any]) -> None:
        config = TIMEFRAMES[target_timeframe]
        window_start_ts = aligned_start(
            int(source_unit["window_start_ts"]),
            int(config["duration_ms"]),
        )
        if window_start_ts in self.emitted[target_timeframe]:
            print(
                f"Late source ignored for finalized aligned {target_timeframe}: {window_start_ts}",
                flush=True,
            )
            return

        bucket = self.buckets[target_timeframe].setdefault(window_start_ts, [])
        source_start_ts = int(source_unit["window_start_ts"])
        if any(int(item["window_start_ts"]) == source_start_ts for item in bucket):
            print(
                f"Duplicate source ignored for aligned {target_timeframe}: {source_start_ts}",
                flush=True,
            )
            return

        bucket.append(source_unit)
        bucket.sort(key=lambda item: int(item["window_start_ts"]))

        expected_count = int(config["source_count"])
        if len(bucket) == expected_count:
            if not source_units_are_contiguous(target_timeframe, window_start_ts, bucket):
                if target_timeframe == "1M":
                    print(
                        "Aligned 1M skipped due to incomplete or non-contiguous 1S source: "
                        f"{window_start_ts}",
                        flush=True,
                    )
                else:
                    print(
                        f"Aligned {target_timeframe} skipped due to non-contiguous "
                        f"{config['source_timeframe']} source: {window_start_ts}",
                        flush=True,
                    )
                self.emitted[target_timeframe].add(window_start_ts)
                self.buckets[target_timeframe].pop(window_start_ts, None)
                return

            candle = build_aligned_candle(target_timeframe, window_start_ts, bucket)
            errors = validate_aligned_candle(candle)
            if errors:
                print(
                    f"Validation failed for aligned {target_timeframe} "
                    f"{candle['window_start_ts']}-{candle['window_end_ts']}: "
                    f"{'; '.join(errors)}",
                    flush=True,
                )
                self.emitted[target_timeframe].add(window_start_ts)
                self.buckets[target_timeframe].pop(window_start_ts, None)
                return

            self.writer.write(target_timeframe, candle)
            self.emitted[target_timeframe].add(window_start_ts)
            self.buckets[target_timeframe].pop(window_start_ts, None)
            print_aligned_output(candle)
            self._feed_next_timeframe(target_timeframe, candle)
        elif len(bucket) > expected_count:
            print(
                f"Too many sources for aligned {target_timeframe} window {window_start_ts}; "
                f"expected {expected_count}, got {len(bucket)}",
                flush=True,
            )

    def _feed_next_timeframe(self, current_timeframe: str, candle: dict[str, Any]) -> None:
        index = PIPELINE.index(current_timeframe)
        if index + 1 >= len(PIPELINE):
            return
        self._add_source_unit(PIPELINE[index + 1], candle)


def build_aligned_candle(
    timeframe: str,
    window_start_ts: int,
    source_units: list[dict[str, Any]],
) -> dict[str, Any]:
    config = TIMEFRAMES[timeframe]
    source_count = int(config["source_count"])
    duration_ms = int(config["duration_ms"])
    source_timeframe = str(config["source_timeframe"])

    buy_volume = sum(float(unit["volume"]["buy_volume"]) for unit in source_units)
    sell_volume = sum(float(unit["volume"]["sell_volume"]) for unit in source_units)
    total_volume = buy_volume + sell_volume
    delta = buy_volume - sell_volume

    trade_counts = [int(unit["trade_flow"]["trade_count"]) for unit in source_units]
    active_units = sum(1 for unit in source_units if unit_has_trade(unit))
    empty_units = source_count - active_units
    footprint_levels = build_footprint_price_levels(source_units)
    profile = build_profile(footprint_levels, find_close_price(source_units))

    bid_update_count = sum(int(unit["depth_flow"]["bid_update_count"]) for unit in source_units)
    ask_update_count = sum(int(unit["depth_flow"]["ask_update_count"]) for unit in source_units)
    balance = bid_update_count - ask_update_count
    depth_total = bid_update_count + ask_update_count

    return {
        "symbol": SYMBOL,
        "timeframe": timeframe,
        "window_start_ts": window_start_ts,
        "window_end_ts": window_start_ts + duration_ms - 1,
        "source_count": len(source_units),
        "source_timeframe": source_timeframe,
        "ohlc": build_ohlc(source_units),
        "volume": {
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "total_volume": total_volume,
            "delta": delta,
            "delta_state": delta_state(delta),
        },
        "trade_flow": {
            "trade_count": sum(trade_counts),
            "active_units": active_units,
            "empty_units": empty_units,
            "avg_trade_count_per_unit": sum(trade_counts) / source_count,
            "max_trade_count_unit": max(trade_counts),
            "min_trade_count_unit": min(trade_counts),
        },
        "footprint": {
            "price_levels": footprint_levels,
        },
        "profile": profile,
        "depth_flow": {
            "bid_update_count": bid_update_count,
            "ask_update_count": ask_update_count,
            "dominant_side": dominant_depth_side(bid_update_count, ask_update_count),
            "balance": balance,
            "imbalance": balance / depth_total if depth_total > 0 else 0.0,
            "ratio": bid_update_count / ask_update_count if ask_update_count > 0 else None,
        },
        "source_refs": {
            "source_window_start_ts": [
                int(unit["window_start_ts"])
                for unit in source_units
            ],
            "source_timeframe": source_timeframe,
        },
    }


def normalize_1s_unit(combined_dna: dict[str, Any]) -> dict[str, Any]:
    candle = combined_dna["candle_dna"]
    depth = combined_dna["depth_mutation_dna"]
    return {
        "symbol": combined_dna["symbol"],
        "timeframe": "1S",
        "window_start_ts": int(combined_dna["window_start_ts"]),
        "window_end_ts": int(combined_dna["window_end_ts"]),
        "ohlc": {
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
        },
        "volume": {
            "buy_volume": float(candle["buy_volume"]),
            "sell_volume": float(candle["sell_volume"]),
            "total_volume": float(candle["total_volume"]),
            "delta": float(candle["delta"]),
            "delta_state": candle["delta_state"],
        },
        "trade_flow": {
            "trade_count": int(candle["trade_count"]),
        },
        "footprint": combined_dna["footprint_dna"],
        "depth_flow": {
            "bid_update_count": int(depth["bid_update_count"]),
            "ask_update_count": int(depth["ask_update_count"]),
        },
    }


def build_ohlc(source_units: list[dict[str, Any]]) -> dict[str, Any]:
    trade_units = [unit for unit in source_units if unit_has_trade(unit)]
    if not trade_units:
        return {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
        }

    high = trade_units[0]["ohlc"]["high"]
    low = trade_units[0]["ohlc"]["low"]
    for unit in trade_units[1:]:
        unit_high = unit["ohlc"]["high"]
        unit_low = unit["ohlc"]["low"]
        if unit_high["price"] > high["price"]:
            high = unit_high
        if unit_low["price"] < low["price"]:
            low = unit_low

    return {
        "open": trade_units[0]["ohlc"]["open"],
        "high": high,
        "low": low,
        "close": trade_units[-1]["ohlc"]["close"],
    }


def build_footprint_price_levels(source_units: list[dict[str, Any]]) -> list[dict[str, Any]]:
    levels: dict[float, dict[str, float | int]] = {}
    for unit in source_units:
        for level in unit["footprint"]["price_levels"]:
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


def build_profile(price_levels: list[dict[str, Any]], close_price: float | None) -> dict[str, Any]:
    if not price_levels:
        return {
            "poc": None,
            "vah": None,
            "val": None,
            "value_area_volume": 0.0,
            "value_area_ratio": 0.70,
            "hvn": [],
            "lvn": [],
        }

    total_volume = sum(float(level["total_volume"]) for level in price_levels)
    poc = select_poc(price_levels, close_price)
    ordered_ascending = sorted(price_levels, key=lambda level: float(level["price"]))
    poc_index = next(
        index
        for index, level in enumerate(ordered_ascending)
        if same_float(level["price"], poc["price"])
    )

    selected_indexes = {poc_index}
    value_area_volume = float(ordered_ascending[poc_index]["total_volume"])
    upper_index = poc_index + 1
    lower_index = poc_index - 1
    target_volume = total_volume * 0.70

    while value_area_volume < target_volume and (
        lower_index >= 0 or upper_index < len(ordered_ascending)
    ):
        choose_upper = choose_value_area_side(
            ordered_ascending,
            lower_index,
            upper_index,
            close_price,
        )
        if choose_upper:
            selected_indexes.add(upper_index)
            value_area_volume += float(ordered_ascending[upper_index]["total_volume"])
            upper_index += 1
        else:
            selected_indexes.add(lower_index)
            value_area_volume += float(ordered_ascending[lower_index]["total_volume"])
            lower_index -= 1

    selected_prices = [
        float(ordered_ascending[index]["price"])
        for index in selected_indexes
    ]
    average_volume = total_volume / len(price_levels)

    return {
        "poc": {
            "price": float(poc["price"]),
            "total_volume": float(poc["total_volume"]),
        },
        "vah": max(selected_prices),
        "val": min(selected_prices),
        "value_area_volume": value_area_volume,
        "value_area_ratio": 0.70,
        "hvn": [
            {"price": float(level["price"]), "total_volume": float(level["total_volume"])}
            for level in price_levels
            if float(level["total_volume"]) >= average_volume * 1.5
        ],
        "lvn": [
            {"price": float(level["price"]), "total_volume": float(level["total_volume"])}
            for level in price_levels
            if float(level["total_volume"]) <= average_volume * 0.5
        ],
    }


def select_poc(
    price_levels: list[dict[str, Any]],
    close_price: float | None,
) -> dict[str, Any]:
    max_volume = max(float(level["total_volume"]) for level in price_levels)
    candidates = [
        level
        for level in price_levels
        if same_float(level["total_volume"], max_volume)
    ]
    if close_price is None:
        return max(candidates, key=lambda level: float(level["price"]))
    return min(candidates, key=lambda level: abs(float(level["price"]) - close_price))


def choose_value_area_side(
    ordered_ascending: list[dict[str, Any]],
    lower_index: int,
    upper_index: int,
    close_price: float | None,
) -> bool:
    if lower_index < 0:
        return True
    if upper_index >= len(ordered_ascending):
        return False

    lower = ordered_ascending[lower_index]
    upper = ordered_ascending[upper_index]
    lower_volume = float(lower["total_volume"])
    upper_volume = float(upper["total_volume"])
    if upper_volume > lower_volume:
        return True
    if lower_volume > upper_volume:
        return False
    if close_price is None:
        return float(upper["price"]) > float(lower["price"])
    return abs(float(upper["price"]) - close_price) < abs(float(lower["price"]) - close_price)


def validate_aligned_candle(candle: dict[str, Any]) -> list[str]:
    errors = []
    config = TIMEFRAMES[candle["timeframe"]]
    expected_count = int(config["source_count"])
    duration_ms = int(config["duration_ms"])
    source_refs = candle["source_refs"]["source_window_start_ts"]
    volume = candle["volume"]
    footprint_levels = candle["footprint"]["price_levels"]
    trade_flow = candle["trade_flow"]
    ohlc = candle["ohlc"]
    profile = candle["profile"]

    if int(candle["source_count"]) != expected_count:
        errors.append("source_count mismatch")
    if len(source_refs) != int(candle["source_count"]):
        errors.append("source_window_start_ts length mismatch")
    if not source_refs_are_contiguous(source_refs, int(candle["window_start_ts"]), source_step_ms(candle["timeframe"])):
        errors.append("source_window_start_ts non-contiguous")
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

    if trade_flow["active_units"] + trade_flow["empty_units"] != int(candle["source_count"]):
        errors.append("active_units + empty_units mismatch")
    if ohlc["high"] is not None and ohlc["low"] is not None:
        if ohlc["high"]["price"] < ohlc["low"]["price"]:
            errors.append("high.price < low.price")
    if profile["poc"] is not None:
        footprint_prices = {float(level["price"]) for level in footprint_levels}
        if float(profile["poc"]["price"]) not in footprint_prices:
            errors.append("poc price missing from footprint")
    if profile["vah"] is not None and profile["val"] is not None:
        if profile["vah"] < profile["val"]:
            errors.append("vah < val")

    expected_end = int(candle["window_start_ts"]) + duration_ms - 1
    if int(candle["window_end_ts"]) != expected_end:
        errors.append("window_end_ts boundary mismatch")
    return errors


def follow_input_file(engine: AlignedCandleEngine) -> None:
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
                print(f"Aligned processing error: {exc}", flush=True)


def print_aligned_output(candle: dict[str, Any]) -> None:
    if FULL_PRINT:
        print(json.dumps(candle, indent=2, ensure_ascii=False), flush=True)
        return

    close = candle["ohlc"]["close"]
    close_price = close["price"] if close is not None else None
    poc = candle["profile"]["poc"]
    poc_price = poc["price"] if poc is not None else None
    print(
        f"[ALIGNED {candle['timeframe']}] "
        f"ts={candle['window_start_ts']}-{candle['window_end_ts']} "
        f"close={format_price(close_price)} "
        f"trades={candle['trade_flow']['trade_count']} "
        f"delta={format_float(candle['volume']['delta'], 6)} "
        f"poc={format_price(poc_price)} "
        f"vah={format_price(candle['profile']['vah'])} "
        f"val={format_price(candle['profile']['val'])}",
        flush=True,
    )


def find_close_price(source_units: list[dict[str, Any]]) -> float | None:
    for unit in reversed(source_units):
        close = unit["ohlc"]["close"]
        if close is not None:
            return float(close["price"])
    return None


def unit_has_trade(unit: dict[str, Any]) -> bool:
    return unit["ohlc"]["open"] is not None


def aligned_start(timestamp_ms: int, duration_ms: int) -> int:
    return (timestamp_ms // duration_ms) * duration_ms


def source_units_are_contiguous(
    target_timeframe: str,
    window_start_ts: int,
    source_units: list[dict[str, Any]],
) -> bool:
    source_refs = [int(unit["window_start_ts"]) for unit in source_units]
    return source_refs_are_contiguous(
        source_refs,
        window_start_ts,
        source_step_ms(target_timeframe),
    )


def source_refs_are_contiguous(source_refs: list[int], expected_start_ts: int, step_ms: int) -> bool:
    return source_refs == [
        expected_start_ts + (index * step_ms)
        for index in range(len(source_refs))
    ]


def source_step_ms(target_timeframe: str) -> int:
    source_timeframe = str(TIMEFRAMES[target_timeframe]["source_timeframe"])
    if source_timeframe == "1S":
        return 1000
    return int(TIMEFRAMES[source_timeframe]["duration_ms"])


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
    writer = AlignedCandleWriter()
    engine = AlignedCandleEngine(writer)
    try:
        follow_input_file(engine)
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
    finally:
        writer.close()


if __name__ == "__main__":
    main()
