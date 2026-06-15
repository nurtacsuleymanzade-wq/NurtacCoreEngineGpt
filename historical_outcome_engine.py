"""Layer-8 future outcome observation engine.

This engine measures subsequent prices. It does not classify outcomes or emit
decisions, setups, signals, scores, probabilities, or numeric thresholds.
"""

import bisect
import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from historical_outcome_contracts import OBSERVATION_WINDOWS, validate_historical_outcome_contracts


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
PRICE_FILES = {
    "one_second_combined_dna": DATA_DIR / "one_second_combined_dna.jsonl",
    "aligned_1m_candle_dna": DATA_DIR / "aligned_1m_candle_dna.jsonl",
}
EVENT_FILES = {
    "detector": DATA_DIR / "detector_events.jsonl",
    "structure": DATA_DIR / "structure_events.jsonl",
    "evidence": DATA_DIR / "evidence_packets.jsonl",
    "volume_profile": DATA_DIR / "volume_profile_events.jsonl",
    "setup": DATA_DIR / "setup_candidates.jsonl",
    "observer": DATA_DIR / "observer_events.jsonl",
}
CONTEXT_FILE = DATA_DIR / "context_dna.jsonl"
OPTIONAL_FILES = {
    "external_market_intelligence": DATA_DIR / "external_market_intelligence.jsonl",
    "coinglass_events": DATA_DIR / "coinglass_events.jsonl",
}
OBSERVATIONS_FILE = DATA_DIR / "historical_outcome_observations.jsonl"
HEALTH_FILE = DATA_DIR / "historical_outcome_health.json"
ERRORS_FILE = DATA_DIR / "historical_outcome_errors.jsonl"
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0


class HistoricalOutcomeEngine:
    def __init__(self) -> None:
        errors = validate_historical_outcome_contracts()
        self.registry_validation_passed = not errors
        if errors: raise RuntimeError("Historical outcome registry invalid: " + "; ".join(errors))
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.prices: dict[str, dict[int, float]] = defaultdict(dict)
        self.price_timestamps: dict[str, list[int]] = defaultdict(list)
        self.pending: dict[str, dict[str, Any]] = {}
        self.completed_ids = load_completed_ids()
        self.seen_source_ids: set[str] = set()
        self.tracked_events = 0
        self.completed_observations = len(self.completed_ids)
        self.last_window_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_heartbeat = time.monotonic()
        self.output_handle = OBSERVATIONS_FILE.open("a", encoding="utf-8")
        self.error_handle = ERRORS_FILE.open("a", encoding="utf-8")
        self.refresh_missing_inputs(); self.write_health()

    def refresh_missing_inputs(self) -> None:
        for path in (*PRICE_FILES.values(), *EVENT_FILES.values(), CONTEXT_FILE):
            label = relative_label(path)
            if path.exists(): self.missing_inputs.discard(label)
            else: self.missing_inputs.add(label)
        for path in OPTIONAL_FILES.values():
            if not path.exists(): self.warnings.add(f"optional_input_missing:{relative_label(path)}")

    def process_price_line(self, source: str, line: str) -> None:
        row = self.parse_line(source, line)
        if row is None: return
        symbol = str(row.get("symbol") or "BTCUSDT")
        ts = safe_int(row.get("window_start_ts")); price = extract_close_price(row)
        if ts is None:
            self.write_error(source, None, "price_timestamp_missing"); return
        if price is None:
            return
        if ts not in self.prices[symbol]: bisect.insort(self.price_timestamps[symbol], ts)
        self.prices[symbol][ts] = price
        self.last_window_ts = max(self.last_window_ts, ts)
        self.measure_pending(symbol)

    def process_event_line(self, source_type: str, line: str) -> None:
        row = self.parse_line(source_type, line)
        if row is None: return
        event = normalize_event(source_type, row)
        if event is None:
            self.write_error(source_type, safe_int(row.get("window_start_ts")), "event_normalization_failed"); return
        source_id = event["source_event_id"]
        if source_id in self.seen_source_ids: return
        self.seen_source_ids.add(source_id); self.tracked_events += 1
        self.last_window_ts = max(self.last_window_ts, event["event_ts"])
        for label, duration in OBSERVATION_WINDOWS.items():
            observation_id = make_observation_id(source_type, source_id, label)
            if observation_id in self.completed_ids: continue
            item = dict(event); item.update({"observation_id": observation_id,
                "observation_window": label, "target_ts": event["event_ts"] + duration})
            self.pending[observation_id] = item
        self.measure_pending(event["symbol"])

    def parse_line(self, source: str, line: str) -> dict[str, Any] | None:
        try: row = json.loads(line)
        except json.JSONDecodeError as exc:
            self.write_error(source, None, f"json_parse_error:{exc}"); return None
        if not isinstance(row, dict):
            self.write_error(source, None, "row_not_object"); return None
        return row

    def measure_pending(self, symbol: str) -> None:
        timestamps = self.price_timestamps.get(symbol, [])
        if not timestamps: return
        latest = timestamps[-1]
        ready = [item for item in self.pending.values() if item["symbol"] == symbol and item["target_ts"] <= latest]
        for item in ready:
            start_index = bisect.bisect_left(timestamps, item["event_ts"])
            end_index = bisect.bisect_right(timestamps, item["target_ts"])
            selected = timestamps[start_index:end_index]
            if not selected: continue
            values = [self.prices[symbol][ts] for ts in selected]
            start_price, close_price = values[0], values[-1]
            payload = {
                "layer":"Layer-8","engine":"HistoricalOutcomeEngine","record_type":"historical_outcome",
                "observation_id":item["observation_id"],"source_type":item["source_type"],
                "source_event_id":item["source_event_id"],"symbol":symbol,"timeframe":item["timeframe"],
                "event_ts":item["event_ts"],"observation_window":item["observation_window"],
                "start_price":start_price,"highest_price":max(values),"lowest_price":min(values),
                "close_price":close_price,"price_path":{"max_excursion":max(values)-start_price,
                "min_excursion":min(values)-start_price,"net_change":close_price-start_price},
                "calibration_status":"uncalibrated","confidence":None,"strength_score":None,
                "thresholds":None,"probability":None,"edge_score":None}
            self.output_handle.write(json.dumps(payload,separators=(",",":")) + "\n"); self.output_handle.flush()
            self.completed_ids.add(item["observation_id"]); self.pending.pop(item["observation_id"], None)
            self.completed_observations += 1

    def write_error(self, source: str, ts: int | None, detail: str) -> None:
        self.error_handle.write(json.dumps({"engine":"HistoricalOutcomeEngine","source":source,
            "window_start_ts":ts,"detail":detail},separators=(",",":")) + "\n"); self.error_handle.flush()
        self.warnings.add(f"error:{source}")

    def tick(self) -> None:
        if time.monotonic() - self.last_heartbeat < HEARTBEAT_SECONDS: return
        self.refresh_missing_inputs(); self.write_health()
        print("Historical Outcome Engine alive", flush=True)
        print(f"tracked_events={self.tracked_events}", flush=True)
        print(f"completed_observations={self.completed_observations}", flush=True)
        print(f"pending_observations={len(self.pending)}", flush=True)
        print(f"last_window_ts={self.last_window_ts}", flush=True)
        self.last_heartbeat = time.monotonic()

    def write_health(self) -> None:
        HEALTH_FILE.write_text(json.dumps({"status":"alive","tracked_events":self.tracked_events,
            "completed_observations":self.completed_observations,"pending_observations":len(self.pending),
            "last_window_ts":self.last_window_ts,"missing_inputs":sorted(self.missing_inputs),
            "warnings":sorted(self.warnings),"registry_validation_passed":self.registry_validation_passed},indent=2)+"\n",encoding="utf-8")

    def close(self) -> None:
        self.write_health(); self.output_handle.close(); self.error_handle.close()


def normalize_event(source_type: str, row: dict[str, Any]) -> dict[str, Any] | None:
    ts = safe_int(row.get("window_start_ts", row.get("source_window_ts")))
    timeframe = str(row.get("timeframe") or "unknown")
    if ts is None: return None
    identifier = row.get("setup_id") or row.get("event_id") or row.get("detector_event_id")
    if not identifier and source_type == "evidence": identifier = f"evidence:{row.get('symbol','BTCUSDT')}:{timeframe}:{ts}"
    if not identifier: identifier = f"{source_type}:{row.get('symbol','BTCUSDT')}:{timeframe}:{ts}:{row.get('event_type','event')}"
    return {"source_type":source_type,"source_event_id":str(identifier),
        "symbol":str(row.get("symbol") or "BTCUSDT"),"timeframe":timeframe,"event_ts":ts}


def extract_close_price(row: dict[str, Any]) -> float | None:
    candle = row.get("candle_dna") if isinstance(row.get("candle_dna"),dict) else row
    ohlc = row.get("ohlc") if isinstance(row.get("ohlc"),dict) else candle
    close = ohlc.get("close")
    if isinstance(close,dict): close=close.get("price")
    if close is None: close=row.get("price")
    try: return float(close) if close is not None else None
    except (TypeError,ValueError,OverflowError): return None


def load_completed_ids() -> set[str]:
    result=set()
    if not OBSERVATIONS_FILE.exists(): return result
    with OBSERVATIONS_FILE.open("r",encoding="utf-8",errors="replace") as handle:
        for line in handle:
            try: row=json.loads(line)
            except json.JSONDecodeError: continue
            if row.get("record_type")=="historical_outcome" and row.get("observation_id"): result.add(str(row["observation_id"]))
    return result


def make_observation_id(source_type: str, source_id: str, window: str) -> str:
    return "hout_" + hashlib.sha256(f"{source_type}|{source_id}|{window}".encode()).hexdigest()[:24]


def safe_int(value: Any) -> int | None:
    try: return int(value) if value is not None else None
    except (TypeError,ValueError,OverflowError): return None


def relative_label(path: Path) -> str:
    try: return str(path.relative_to(ROOT_DIR)).replace("\\","/")
    except ValueError: return str(path).replace("\\","/")


def run() -> None:
    engine=HistoricalOutcomeEngine()
    price_handles={name:None for name in PRICE_FILES}; event_handles={name:None for name in EVENT_FILES}
    passive_files={"context":CONTEXT_FILE,**OPTIONAL_FILES}; passive_handles={name:None for name in passive_files}
    try:
        while True:
            activity=0
            for name,path in PRICE_FILES.items():
                if price_handles[name] is None:
                    if not path.exists(): continue
                    price_handles[name]=path.open("r",encoding="utf-8",errors="replace")
                while True:
                    line=price_handles[name].readline()
                    if not line: break
                    engine.process_price_line(name,line); activity+=1; engine.tick()
            for source,path in EVENT_FILES.items():
                if event_handles[source] is None:
                    if not path.exists(): continue
                    event_handles[source]=path.open("r",encoding="utf-8",errors="replace")
                while True:
                    line=event_handles[source].readline()
                    if not line: break
                    engine.process_event_line(source,line); activity+=1; engine.tick()
            for name,path in passive_files.items():
                if passive_handles[name] is None:
                    if not path.exists(): continue
                    passive_handles[name]=path.open("r",encoding="utf-8",errors="replace")
                while passive_handles[name].readline(): activity+=1; engine.tick()
            engine.tick()
            if activity==0: time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in (*price_handles.values(),*event_handles.values(),*passive_handles.values()):
            if handle: handle.close()
        engine.close()


if __name__=="__main__":
    try: run()
    except KeyboardInterrupt: print("Stopped.",flush=True)
