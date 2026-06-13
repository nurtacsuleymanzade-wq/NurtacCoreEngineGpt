import json
import math
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
LAYER0_FILE = DATA_DIR / "one_second_combined_dna.jsonl"
LAYER2_FILE = DATA_DIR / "aligned_1m_candle_dna.jsonl"
REPORT_FILE = DATA_DIR / "layer2_verification_report.json"

ABS_TOL = 1e-9


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                records.append(json.loads(stripped))
    return records


def same_float(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return math.isclose(float(left), float(right), abs_tol=ABS_TOL)


def same_point(left: dict[str, Any] | None, right: dict[str, Any] | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return (
        same_float(left.get("price"), right.get("price"))
        and int(left.get("time")) == int(right.get("time"))
        and left.get("side") == right.get("side")
    )


def build_ohlc(source_1s: list[dict[str, Any]]) -> dict[str, Any]:
    trade_candles = [
        item["candle_dna"]
        for item in source_1s
        if item["candle_dna"]["has_trade"]
    ]
    if not trade_candles:
        return {"open": None, "high": None, "low": None, "close": None}

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


def build_volume_and_trade_flow(source_1s: list[dict[str, Any]]) -> dict[str, Any]:
    buy_volume = sum(float(item["candle_dna"]["buy_volume"]) for item in source_1s)
    sell_volume = sum(float(item["candle_dna"]["sell_volume"]) for item in source_1s)
    total_volume = buy_volume + sell_volume
    delta = buy_volume - sell_volume
    trade_count = sum(int(item["candle_dna"]["trade_count"]) for item in source_1s)
    return {
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "total_volume": total_volume,
        "delta": delta,
        "trade_count": trade_count,
    }


def build_footprint(source_1s: list[dict[str, Any]]) -> list[dict[str, Any]]:
    levels: dict[float, dict[str, float | int]] = {}
    for item in source_1s:
        for level in item["footprint_dna"]["price_levels"]:
            price = float(level["price"])
            aggregate = levels.setdefault(
                price,
                {"buy_volume": 0.0, "sell_volume": 0.0, "trade_count": 0},
            )
            aggregate["buy_volume"] = float(aggregate["buy_volume"]) + float(level["buy_volume"])
            aggregate["sell_volume"] = float(aggregate["sell_volume"]) + float(level["sell_volume"])
            aggregate["trade_count"] = int(aggregate["trade_count"]) + int(level["trade_count"])

    footprint = []
    for price in sorted(levels.keys(), reverse=True):
        aggregate = levels[price]
        buy_volume = float(aggregate["buy_volume"])
        sell_volume = float(aggregate["sell_volume"])
        total_volume = buy_volume + sell_volume
        delta = buy_volume - sell_volume
        footprint.append(
            {
                "price": price,
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
                "total_volume": total_volume,
                "delta": delta,
                "trade_count": int(aggregate["trade_count"]),
            }
        )
    return footprint


def find_close_price(ohlc: dict[str, Any]) -> float | None:
    close = ohlc.get("close")
    if close is None:
        return None
    return float(close["price"])


def select_poc(
    footprint: list[dict[str, Any]],
    close_price: float | None,
) -> dict[str, Any] | None:
    if not footprint:
        return None
    max_volume = max(float(level["total_volume"]) for level in footprint)
    candidates = [
        level
        for level in footprint
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


def build_profile(
    footprint: list[dict[str, Any]],
    close_price: float | None,
) -> dict[str, Any]:
    if not footprint:
        return {
            "poc": None,
            "vah": None,
            "val": None,
            "value_area_volume": 0.0,
        }

    total_volume = sum(float(level["total_volume"]) for level in footprint)
    poc = select_poc(footprint, close_price)
    ordered_ascending = sorted(footprint, key=lambda level: float(level["price"]))
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
    return {
        "poc": {
            "price": float(poc["price"]),
            "total_volume": float(poc["total_volume"]),
        },
        "vah": max(selected_prices),
        "val": min(selected_prices),
        "value_area_volume": value_area_volume,
    }


def compare_footprint(
    expected: list[dict[str, Any]],
    actual: list[dict[str, Any]],
    errors: list[str],
) -> None:
    if len(expected) != len(actual):
        errors.append("footprint price level count mismatch")
        return

    actual_by_price = {float(level["price"]): level for level in actual}
    for expected_level in expected:
        price = float(expected_level["price"])
        actual_level = actual_by_price.get(price)
        if actual_level is None:
            errors.append(f"footprint missing price {price}")
            continue
        for key in ("buy_volume", "sell_volume", "total_volume", "delta"):
            if not same_float(expected_level[key], actual_level[key]):
                errors.append(f"footprint {price} {key} mismatch")
        if int(expected_level["trade_count"]) != int(actual_level["trade_count"]):
            errors.append(f"footprint {price} trade_count mismatch")


def value_area_approx_check(
    profile: dict[str, Any],
    footprint: list[dict[str, Any]],
) -> bool:
    poc = profile.get("poc")
    vah = profile.get("vah")
    val = profile.get("val")
    if not footprint:
        return poc is None and vah is None and val is None
    if poc is None or vah is None or val is None:
        return False
    if not (float(val) <= float(poc["price"]) <= float(vah)):
        return False
    if float(vah) < float(val):
        return False

    total_volume = sum(float(level["total_volume"]) for level in footprint)
    area_volume = sum(
        float(level["total_volume"])
        for level in footprint
        if float(val) <= float(level["price"]) <= float(vah)
    )
    return area_volume + ABS_TOL >= total_volume * 0.70


def verify_candle(
    candle_1m: dict[str, Any],
    layer0_by_ts: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    window_start = int(candle_1m["window_start_ts"])
    window_end = int(candle_1m["window_end_ts"])
    source_1s = [
        layer0_by_ts[ts]
        for ts in sorted(layer0_by_ts)
        if window_start <= ts < window_end
    ]

    errors: list[str] = []
    warnings: list[str] = []

    source_refs = candle_1m.get("source_refs", {}).get("source_window_start_ts")
    source_ts_list = [int(item["window_start_ts"]) for item in source_1s]
    if source_refs is not None and [int(ts) for ts in source_refs] != source_ts_list:
        errors.append("source_refs mismatch")

    expected_ohlc = build_ohlc(source_1s)
    actual_ohlc = candle_1m["ohlc"]
    for key in ("open", "high", "low", "close"):
        if not same_point(expected_ohlc[key], actual_ohlc[key]):
            errors.append(f"ohlc {key} mismatch")

    expected_volume = build_volume_and_trade_flow(source_1s)
    actual_volume = candle_1m["volume"]
    for key in ("buy_volume", "sell_volume", "total_volume", "delta"):
        if not same_float(expected_volume[key], actual_volume[key]):
            errors.append(f"volume {key} mismatch")

    actual_trade_count = int(candle_1m["trade_flow"]["trade_count"])
    if expected_volume["trade_count"] != actual_trade_count:
        errors.append("trade_count mismatch")

    expected_footprint = build_footprint(source_1s)
    compare_footprint(expected_footprint, candle_1m["footprint"]["price_levels"], errors)

    expected_profile = build_profile(expected_footprint, find_close_price(expected_ohlc))
    actual_profile = candle_1m.get("profile", {})
    actual_poc = actual_profile.get("poc")
    expected_poc = expected_profile.get("poc")
    if expected_poc is None or actual_poc is None:
        if expected_poc is not None or actual_poc is not None:
            errors.append("poc mismatch")
    elif not (
        same_float(expected_poc["price"], actual_poc["price"])
        and same_float(expected_poc["total_volume"], actual_poc["total_volume"])
    ):
        errors.append("poc mismatch")

    if "vah" in actual_profile and "val" in actual_profile:
        exact_match = (
            same_float(expected_profile["vah"], actual_profile["vah"])
            and same_float(expected_profile["val"], actual_profile["val"])
        )
        if not exact_match:
            errors.append("vah_val exact_match failed")
        if not value_area_approx_check(actual_profile, expected_footprint):
            errors.append("vah_val approximate_range_check failed")

    if "data_quality" in candle_1m:
        quality = candle_1m["data_quality"]
        expected_source_count = int(quality.get("expected_source_count"))
        actual_source_count = int(quality.get("actual_source_count"))
        missing_source_count = int(quality.get("missing_source_count"))
        coverage_ratio = float(quality.get("coverage_ratio"))
        quality_state = quality.get("quality_state")
        recomputed_actual = len(source_1s)
        recomputed_missing = max(0, expected_source_count - recomputed_actual)
        recomputed_ratio = recomputed_actual / expected_source_count if expected_source_count else 0.0
        if actual_source_count != recomputed_actual:
            errors.append("data_quality actual_source_count mismatch")
        if missing_source_count != recomputed_missing:
            errors.append("data_quality missing_source_count mismatch")
        if not same_float(coverage_ratio, recomputed_ratio):
            errors.append("data_quality coverage_ratio mismatch")
        expected_state = "complete" if recomputed_ratio == 1.0 else "partial" if recomputed_ratio > 0 else "empty"
        if quality_state != expected_state:
            errors.append("data_quality quality_state mismatch")
    else:
        warnings.append("missing_data_quality_field")

    status = "failed" if errors else "passed"
    return {
        "window_start_ts": window_start,
        "window_end_ts": window_end,
        "source_1s_count": len(source_1s),
        "status": status,
        "errors": errors,
        "warnings": warnings,
    }


def write_report(report: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def main() -> None:
    layer0_records = load_jsonl(LAYER0_FILE)
    layer2_records = load_jsonl(LAYER2_FILE)
    layer0_by_ts = {
        int(record["window_start_ts"]): record
        for record in layer0_records
    }

    results = [
        verify_candle(candle_1m, layer0_by_ts)
        for candle_1m in layer2_records
    ]
    passed = sum(1 for result in results if result["status"] == "passed")
    failed = sum(1 for result in results if result["status"] == "failed")
    warnings = sum(len(result["warnings"]) for result in results)

    report = {
        "input_files": {
            "layer0": "data/one_second_combined_dna.jsonl",
            "layer2": "data/aligned_1m_candle_dna.jsonl",
        },
        "checked_1m_candles": len(results),
        "passed": passed,
        "failed": failed,
        "warnings": warnings,
        "results": results,
        "test_passed": failed == 0,
    }
    write_report(report)

    print("LAYER-2 VERIFICATION COMPLETE", flush=True)
    print(f"checked_1m_candles={len(results)}", flush=True)
    print(f"passed={passed}", flush=True)
    print(f"failed={failed}", flush=True)
    print(f"warnings={warnings}", flush=True)
    print(r"report=data\layer2_verification_report.json", flush=True)


if __name__ == "__main__":
    main()
