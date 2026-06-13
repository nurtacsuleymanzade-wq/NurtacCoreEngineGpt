import json
import math
import re
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
LAYER2_FILE = DATA_DIR / "aligned_1m_candle_dna.jsonl"
REPORT_FILE = DATA_DIR / "layer3_verification_report.json"
ABS_TOL = 1e-9
ATR_PERIOD = 14
DAY_MS = 86_400_000


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


def detect_baseline_file() -> Path | None:
    candidates = [
        DATA_DIR / "historical_baseline_1m.jsonl",
        DATA_DIR / "historical_baseline_dna.jsonl",
    ]
    source_path = ROOT_DIR / "historical_baseline_engine.py"
    if source_path.exists():
        text = source_path.read_text(encoding="utf-8", errors="replace")
        match = re.search(r'OUTPUT_FILE\s*=\s*DATA_DIR\s*/\s*"([^"]+)"', text)
        if match:
            candidates.append(DATA_DIR / match.group(1))

    for path in candidates:
        if path.exists():
            return path
    return None


def same_float(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return math.isclose(float(left), float(right), abs_tol=ABS_TOL)


def day_start(timestamp_ms: int) -> int:
    return (timestamp_ms // DAY_MS) * DAY_MS


def true_range(record: dict[str, Any], previous_close: float | None) -> float | None:
    ohlc = record["ohlc"]
    if ohlc["high"] is None or ohlc["low"] is None:
        return None
    high = float(ohlc["high"]["price"])
    low = float(ohlc["low"]["price"])
    high_low = high - low
    if previous_close is None:
        return high_low
    return max(high_low, abs(high - previous_close), abs(low - previous_close))


def close_price(record: dict[str, Any]) -> float | None:
    close = record["ohlc"]["close"]
    if close is None:
        return None
    return float(close["price"])


def typical_price(record: dict[str, Any]) -> float | None:
    ohlc = record["ohlc"]
    if ohlc["high"] is None or ohlc["low"] is None or ohlc["close"] is None:
        return None
    return (
        float(ohlc["high"]["price"])
        + float(ohlc["low"]["price"])
        + float(ohlc["close"]["price"])
    ) / 3


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


def compute_reference(records: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    refs: dict[int, dict[str, Any]] = {}
    previous_close = None
    tr_values: list[float] = []
    atr_values: list[float] = []
    current_atr = None
    session_start = None
    vwap_num = 0.0
    vwap_den = 0.0
    cvd = 0.0
    volume_values: list[float] = []

    for record in sorted(records, key=lambda item: int(item["window_start_ts"])):
        ts = int(record["window_start_ts"])
        record_session_start = day_start(ts)
        if session_start != record_session_start:
            session_start = record_session_start
            vwap_num = 0.0
            vwap_den = 0.0
            cvd = 0.0

        tr = true_range(record, previous_close)
        if tr is not None:
            tr_values.append(tr)
            if current_atr is None:
                current_atr = tr
            elif len(tr_values) < ATR_PERIOD:
                current_atr = sum(tr_values) / len(tr_values)
            elif len(tr_values) == ATR_PERIOD:
                current_atr = sum(tr_values[-ATR_PERIOD:]) / ATR_PERIOD
            else:
                current_atr = ((current_atr * (ATR_PERIOD - 1)) + tr) / ATR_PERIOD
            atr_values.append(current_atr)

        volume = float(record["volume"]["total_volume"])
        tp = typical_price(record)
        if tp is not None and volume > 0:
            vwap_num += tp * volume
            vwap_den += volume
        session_vwap = vwap_num / vwap_den if vwap_den > 0 else None

        delta = float(record["volume"]["buy_volume"]) - float(record["volume"]["sell_volume"])
        cvd += delta
        volume_values.append(volume)

        medium_volumes = volume_values[-100:]
        medium_atrs = atr_values[-100:]
        refs[ts] = {
            "atr": current_atr if current_atr is not None else 0.0,
            "current_tr": tr if tr is not None else 0.0,
            "atr_percentile_medium": (
                percentile_rank(medium_atrs, current_atr)
                if current_atr is not None and medium_atrs
                else 0.0
            ),
            "atr_z_score_medium": (
                z_score(medium_atrs, current_atr)
                if current_atr is not None and medium_atrs
                else 0.0
            ),
            "session_vwap": session_vwap,
            "cvd": cvd,
            "volume_percentile_medium": percentile_rank(medium_volumes, volume),
            "volume_z_score_medium": z_score(medium_volumes, volume),
        }

        cp = close_price(record)
        if cp is not None:
            previous_close = cp
    return refs


def baseline_ts(row: dict[str, Any]) -> int | None:
    if "record_window" in row and "last_ts" in row["record_window"]:
        return int(row["record_window"]["last_ts"])
    if "window_start_ts" in row:
        return int(row["window_start_ts"])
    return None


def verify_rows(
    baseline_rows: list[dict[str, Any]],
    references: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    report = {
        "checked_rows": 0,
        "atr_passed": 0,
        "atr_failed": 0,
        "vwap_passed": 0,
        "vwap_failed": 0,
        "cvd_passed": 0,
        "cvd_failed": 0,
        "percentile_passed": 0,
        "percentile_failed": 0,
        "zscore_passed": 0,
        "zscore_failed": 0,
        "errors": [],
        "warnings": [],
        "test_passed": True,
    }

    for row in baseline_rows:
        if row.get("timeframe") != "1M":
            continue
        ts = baseline_ts(row)
        if ts is None:
            report["warnings"].append("baseline row without timestamp skipped")
            continue
        reference = references.get(ts)
        if reference is None:
            report["errors"].append({"ts": ts, "field": "reference", "error": "missing aligned 1M source"})
            continue

        report["checked_rows"] += 1
        atr = row.get("atr", {})
        if same_float(atr.get("atr"), reference["atr"]):
            report["atr_passed"] += 1
        else:
            report["atr_failed"] += 1
            report["errors"].append({"ts": ts, "field": "atr", "expected": reference["atr"], "actual": atr.get("atr")})

        vwap = row.get("vwap", {})
        if same_float(vwap.get("session_vwap"), reference["session_vwap"]):
            report["vwap_passed"] += 1
        else:
            report["vwap_failed"] += 1
            report["errors"].append(
                {"ts": ts, "field": "session_vwap", "expected": reference["session_vwap"], "actual": vwap.get("session_vwap")}
            )

        cvd = row.get("cvd", {})
        if same_float(cvd.get("cvd"), reference["cvd"]):
            report["cvd_passed"] += 1
        else:
            report["cvd_failed"] += 1
            report["errors"].append({"ts": ts, "field": "cvd", "expected": reference["cvd"], "actual": cvd.get("cvd")})

        percentile_checked = False
        if "atr_percentile_medium" in atr:
            percentile_checked = True
            if same_float(atr["atr_percentile_medium"], reference["atr_percentile_medium"]):
                report["percentile_passed"] += 1
            else:
                report["percentile_failed"] += 1
                report["errors"].append(
                    {
                        "ts": ts,
                        "field": "atr_percentile_medium",
                        "expected": reference["atr_percentile_medium"],
                        "actual": atr["atr_percentile_medium"],
                    }
                )
        volume_medium = row.get("metrics", {}).get("total_volume", {}).get("medium")
        if volume_medium and "latest_percentile" in volume_medium:
            percentile_checked = True
            if same_float(volume_medium["latest_percentile"], reference["volume_percentile_medium"]):
                report["percentile_passed"] += 1
            else:
                report["percentile_failed"] += 1
                report["errors"].append(
                    {
                        "ts": ts,
                        "field": "volume_percentile_medium",
                        "expected": reference["volume_percentile_medium"],
                        "actual": volume_medium["latest_percentile"],
                    }
                )
        if not percentile_checked:
            report["warnings"].append({"ts": ts, "warning": "no percentile fields found"})

        zscore_checked = False
        if "atr_z_score_medium" in atr:
            zscore_checked = True
            if same_float(atr["atr_z_score_medium"], reference["atr_z_score_medium"]):
                report["zscore_passed"] += 1
            else:
                report["zscore_failed"] += 1
                report["errors"].append(
                    {
                        "ts": ts,
                        "field": "atr_z_score_medium",
                        "expected": reference["atr_z_score_medium"],
                        "actual": atr["atr_z_score_medium"],
                    }
                )
        if volume_medium and "z_score" in volume_medium:
            zscore_checked = True
            if same_float(volume_medium["z_score"], reference["volume_z_score_medium"]):
                report["zscore_passed"] += 1
            else:
                report["zscore_failed"] += 1
                report["errors"].append(
                    {
                        "ts": ts,
                        "field": "volume_z_score_medium",
                        "expected": reference["volume_z_score_medium"],
                        "actual": volume_medium["z_score"],
                    }
                )
        if not zscore_checked:
            report["warnings"].append({"ts": ts, "warning": "no z-score fields found"})

    report["test_passed"] = (
        report["checked_rows"] > 0
        and report["atr_failed"] == 0
        and report["vwap_failed"] == 0
        and report["cvd_failed"] == 0
        and report["percentile_failed"] == 0
        and report["zscore_failed"] == 0
        and not report["errors"]
    )
    return report


def write_report(report: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    baseline_file = detect_baseline_file()
    aligned_records = load_jsonl(LAYER2_FILE)
    if baseline_file is None:
        report = {
            "checked_rows": 0,
            "atr_passed": 0,
            "atr_failed": 0,
            "vwap_passed": 0,
            "vwap_failed": 0,
            "cvd_passed": 0,
            "cvd_failed": 0,
            "percentile_passed": 0,
            "percentile_failed": 0,
            "zscore_passed": 0,
            "zscore_failed": 0,
            "errors": ["Layer-3 baseline input file not found"],
            "warnings": [],
            "test_passed": False,
        }
    else:
        baseline_rows = load_jsonl(baseline_file)
        references = compute_reference(aligned_records)
        report = verify_rows(baseline_rows, references)
        report["warnings"].append({"baseline_file": str(baseline_file.relative_to(ROOT_DIR)).replace("\\", "/")})

    write_report(report)

    print("LAYER-3 VERIFICATION COMPLETE", flush=True)
    print(f"checked_rows={report['checked_rows']}", flush=True)
    print(f"atr_passed={report['atr_passed']}", flush=True)
    print(f"atr_failed={report['atr_failed']}", flush=True)
    print(f"vwap_passed={report['vwap_passed']}", flush=True)
    print(f"vwap_failed={report['vwap_failed']}", flush=True)
    print(f"cvd_passed={report['cvd_passed']}", flush=True)
    print(f"cvd_failed={report['cvd_failed']}", flush=True)
    print(f"test_passed={str(report['test_passed']).lower()}", flush=True)
    print(r"report=data\layer3_verification_report.json", flush=True)


if __name__ == "__main__":
    main()
