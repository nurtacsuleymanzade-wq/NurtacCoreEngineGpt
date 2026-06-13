import json
import math
import random
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
CONTEXT_FILE = DATA_DIR / "context_dna.jsonl"
REPORT_FILE = DATA_DIR / "context_math_verification.json"

SOURCE_FILES = {
    "1S": DATA_DIR / "one_second_combined_dna.jsonl",
    "3S": DATA_DIR / "rolling_3s_dna.jsonl",
    "5S": DATA_DIR / "rolling_5s_dna.jsonl",
    "15S": DATA_DIR / "rolling_15s_dna.jsonl",
    "1M": DATA_DIR / "aligned_1m_candle_dna.jsonl",
}

SAMPLE_COUNTS = {
    "1S": 50,
    "3S": 20,
    "5S": 20,
    "15S": 20,
    "1M": 10,
}

ATR_TOL = 0.0001
VWAP_TOL = 0.0001
CVD_TOL = 0.0
PERCENTILE_TOL = 0.1
RANDOM_SEED = 20260614


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))
    return rows


def point_price(point: dict[str, Any] | None) -> float | None:
    if point is None:
        return None
    return float(point["price"])


def normalize_source(row: dict[str, Any], timeframe: str) -> dict[str, Any]:
    if timeframe == "1S":
        candle = row["candle_dna"]
        return {
            "window_start_ts": int(row["window_start_ts"]),
            "high": point_price(candle["high"]),
            "low": point_price(candle["low"]),
            "close": point_price(candle["close"]),
            "total_volume": float(candle["total_volume"]),
            "delta": float(candle["buy_volume"]) - float(candle["sell_volume"]),
        }

    return {
        "window_start_ts": int(row["window_start_ts"]),
        "high": point_price(row["ohlc"]["high"]),
        "low": point_price(row["ohlc"]["low"]),
        "close": point_price(row["ohlc"]["close"]),
        "total_volume": float(row["volume"]["total_volume"]),
        "delta": float(row["volume"]["buy_volume"]) - float(row["volume"]["sell_volume"]),
    }


def percentile_rank(values: list[float], current: float) -> float:
    if not values:
        return 0.0
    below = sum(1 for value in values if value < current)
    equal = sum(1 for value in values if math.isclose(value, current, abs_tol=1e-9))
    return ((below + 0.5 * equal) / len(values)) * 100


def build_reference_series(timeframe: str) -> dict[int, dict[str, Any]]:
    source_rows = [normalize_source(row, timeframe) for row in load_jsonl(SOURCE_FILES[timeframe])]
    source_rows.sort(key=lambda row: row["window_start_ts"])

    reference = {}
    previous_close = None
    tr_values: list[float] = []
    atr14 = None
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    cvd = 0.0
    volume_history: list[float] = []

    for row in source_rows:
        ts = row["window_start_ts"]
        high = row["high"]
        low = row["low"]
        close = row["close"]
        volume = row["total_volume"]

        if high is None or low is None or close is None:
            atr = None
        else:
            high_low = high - low
            if previous_close is None:
                tr = high_low
            else:
                tr = max(high_low, abs(high - previous_close), abs(low - previous_close))
            tr_values.append(tr)
            if len(tr_values) < 14:
                atr14 = None
            elif len(tr_values) == 14:
                atr14 = sum(tr_values[-14:]) / 14
            else:
                atr14 = ((atr14 * 13) + tr) / 14 if atr14 is not None else sum(tr_values[-14:]) / 14
            atr = atr14
            previous_close = close

        if high is not None and low is not None and close is not None and volume > 0:
            typical_price = (high + low + close) / 3
            cumulative_pv += typical_price * volume
            cumulative_volume += volume
        vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else None

        cvd += row["delta"]
        volume_history.append(volume)
        volume_period_20 = volume_history[-20:]
        reference[ts] = {
            "atr14": atr,
            "vwap": vwap,
            "cvd": cvd,
            "volume_percentile_20": percentile_rank(volume_period_20, volume),
        }

    return reference


def close_enough(expected: float | None, actual: float | None, tolerance: float) -> bool:
    if expected is None or actual is None:
        return expected is None and actual is None
    return abs(float(expected) - float(actual)) <= tolerance


def context_actuals(row: dict[str, Any]) -> dict[str, Any]:
    context = row["context"]
    return {
        "atr14": context["atr"]["14"]["atr"],
        "vwap": context["vwap"]["vwap"],
        "cvd": context["cvd"]["cvd"],
        "volume_percentile_20": context["volume_context"]["20"]["percentile"],
    }


def sample_context_rows() -> list[dict[str, Any]]:
    rows_by_tf = {timeframe: [] for timeframe in SAMPLE_COUNTS}
    for row in load_jsonl(CONTEXT_FILE):
        timeframe = row.get("timeframe")
        if timeframe in rows_by_tf:
            rows_by_tf[timeframe].append(row)

    random.seed(RANDOM_SEED)
    sampled = []
    for timeframe, count in SAMPLE_COUNTS.items():
        rows = rows_by_tf[timeframe]
        sampled.extend(random.sample(rows, min(count, len(rows))))
    return sampled


def main() -> None:
    references = {timeframe: build_reference_series(timeframe) for timeframe in SAMPLE_COUNTS}
    sampled_rows = sample_context_rows()
    failures = []
    checked_rows = 0
    passed_rows = 0

    for row in sampled_rows:
        timeframe = row["timeframe"]
        window_ts = int(row["source_window_ts"])
        expected = references[timeframe].get(window_ts)
        actual = context_actuals(row)
        checked_rows += 1

        row_failures = []
        if expected is None:
            row_failures.append({"field": "source", "expected": "source row exists", "actual": None})
        else:
            checks = [
                ("atr14", ATR_TOL),
                ("vwap", VWAP_TOL),
                ("cvd", CVD_TOL),
                ("volume_percentile_20", PERCENTILE_TOL),
            ]
            for field, tolerance in checks:
                if not close_enough(expected[field], actual[field], tolerance):
                    row_failures.append(
                        {
                            "field": field,
                            "expected": expected[field],
                            "actual": actual[field],
                        }
                    )

        if row_failures:
            for failure in row_failures:
                failures.append(
                    {
                        "timeframe": timeframe,
                        "window_ts": window_ts,
                        "expected": {failure["field"]: failure["expected"]},
                        "actual": {failure["field"]: failure["actual"]},
                    }
                )
        else:
            passed_rows += 1

    report = {
        "checked_rows": checked_rows,
        "passed_rows": passed_rows,
        "failed_rows": checked_rows - passed_rows,
        "failures": failures,
    }
    REPORT_FILE.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print("CONTEXT MATH VERIFICATION COMPLETE", flush=True)
    print(f"checked_rows={report['checked_rows']}", flush=True)
    print(f"passed_rows={report['passed_rows']}", flush=True)
    print(f"failed_rows={report['failed_rows']}", flush=True)
    print(r"report=data\context_math_verification.json", flush=True)


if __name__ == "__main__":
    main()
