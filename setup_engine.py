"""Layer-7 structural setup-candidate aggregation engine."""

import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from setup_contracts import ALLOWED_TIMEFRAMES, SETUP_CONTRACTS, validate_setup_contracts

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
INPUT_FILES = {
 "evidence_packets": DATA_DIR / "evidence_packets.jsonl",
 "detector_events": DATA_DIR / "detector_events.jsonl",
 "structure_events": DATA_DIR / "structure_events.jsonl",
 "smart_money_dna": DATA_DIR / "smart_money_dna.jsonl",
 "context_dna": DATA_DIR / "context_dna.jsonl",
 "volume_profile_dna": DATA_DIR / "volume_profile_dna.jsonl",
 "volume_profile_events": DATA_DIR / "volume_profile_events.jsonl",
 "calibration_profiles": DATA_DIR / "calibration_profiles.json",
 "historical_outcome_observations": DATA_DIR / "historical_outcome_observations.jsonl",
 "data_quality": DATA_DIR / "data_quality.jsonl",
}
PRIMARY = {"evidence_packets","detector_events","structure_events","smart_money_dna","context_dna","volume_profile_dna","volume_profile_events"}
CANDIDATES_FILE = DATA_DIR / "setup_candidates.jsonl"
HEALTH_FILE = DATA_DIR / "setup_health.json"
ERRORS_FILE = DATA_DIR / "setup_errors.jsonl"
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0


class SetupEngine:
    def __init__(self) -> None:
        errors = validate_setup_contracts()
        self.registry_validation_passed = not errors
        if errors: raise RuntimeError("Setup registry invalid: " + "; ".join(errors))
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.buckets: dict[tuple[str,str,int], list[dict[str,Any]]] = defaultdict(list)
        self.written_keys = load_written_keys()
        self.input_rows_processed = {name: 0 for name in INPUT_FILES if name != "data_quality"}
        self.setup_candidates_written = 0
        self.last_window_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_heartbeat = time.monotonic()
        self.output_handle = CANDIDATES_FILE.open("a", encoding="utf-8")
        self.error_handle = ERRORS_FILE.open("a", encoding="utf-8")
        self.refresh_inputs(); self.write_health()

    def refresh_inputs(self) -> None:
        for name, path in INPUT_FILES.items():
            label = relative_label(path)
            if path.exists(): self.missing_inputs.discard(label)
            elif name in PRIMARY: self.missing_inputs.add(label)
            else: self.warnings.add(f"optional_input_missing:{label}")

    def process_line(self, source: str, line: str) -> None:
        try: row = json.loads(line)
        except json.JSONDecodeError as exc: self.write_error(source, None, "json_parse_error", str(exc)); return
        if not isinstance(row, dict): self.write_error(source, None, "row_not_object", "JSON object required"); return
        self.input_rows_processed[source] += 1
        normalized = normalize_input(source, row)
        if normalized is None: self.write_error(source, safe_int(row.get("window_start_ts")), "normalization_failed", "grouping fields unavailable"); return
        ts = normalized["window_start_ts"]
        self.last_window_ts = max(self.last_window_ts, ts)
        key = (normalized["symbol"], normalized["timeframe"], ts)
        self.buckets[key].append(normalized)
        self.evaluate_bucket(key)

    def process_json(self, source: str, path: Path) -> None:
        try: row = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc: self.write_error(source, None, "json_parse_error", str(exc)); return
        self.input_rows_processed[source] += 1
        if isinstance(row, dict):
            for bucket in self.buckets.values(): bucket.append({"source": source, "raw": row})

    def evaluate_bucket(self, key: tuple[str,str,int]) -> None:
        symbol, timeframe, ts = key
        if timeframe not in ALLOWED_TIMEFRAMES: self.warnings.add(f"unsupported_timeframe:{timeframe}"); return
        rows = self.buckets[key]
        for contract in SETUP_CONTRACTS:
            for side, groups in contract["required_concepts"].items():
                matched, missing = match_groups(rows, groups)
                if missing: continue
                duplicate = (contract["setup_name"], symbol, timeframe, ts, side)
                if duplicate in self.written_keys: continue
                optional, _ = match_groups(rows, contract["optional_concepts"], optional=True)
                self.write_candidate(contract, duplicate, rows, matched + optional)

    def write_candidate(self, contract: dict[str,Any], key: tuple[str,str,str,int,str], rows: list[dict[str,Any]], matched: list[str]) -> None:
        name, symbol, timeframe, ts, side = key
        snapshot = next((r for r in reversed(rows) if r.get("source") == "volume_profile_dna"), None)
        window_end = next((r.get("window_end_ts") for r in reversed(rows) if r.get("window_end_ts") is not None), None)
        payload = {
         "layer":"Layer-7","engine":"SetupEngine","record_type":"setup_candidate",
         "setup_id":make_setup_id(key),"setup_name":name,"setup_family":contract["setup_family"],
         "symbol":symbol,"timeframe":timeframe,"window_start_ts":ts,"window_end_ts":window_end,"side":side,
         "setup_status":"candidate_not_trade_signal","matched_concepts":unique(matched),"missing_concepts":[],
         "supporting_evidence":refs(rows),"blocking_evidence":blocking(rows, side),
         "detector_refs":source_refs(rows,"detector"),"evidence_refs":source_refs(rows,"evidence"),
         "structure_refs":source_refs(rows,"smart_money"),"volume_profile_refs":source_refs(rows,"volume_profile","volume_profile_dna"),
         "context_refs":source_refs(rows,"context"),"historical_outcome_refs":source_refs(rows,"historical_outcome_observations"),
         "location_context":location_context(snapshot),"auction_context":auction_context(snapshot),
         "calibration_status":"uncalibrated",
         "scores":{"confidence":None,"strength_score":None,"setup_score":None,"edge_score":None,"probability_score":None,"threshold":None},
         "execution_readiness":{"ready_for_execution_plan":False,"reason":"setup_uncalibrated"},
         "risk_readiness":{"ready_for_position_sizing":False,"reason":"no_execution_plan"},
         "validation":{"contract_found":True,"invariants_passed":True,"errors":[]}}
        self.output_handle.write(json.dumps(payload,separators=(",",":")) + "\n"); self.output_handle.flush()
        self.written_keys.add(key); self.setup_candidates_written += 1

    def write_error(self, source: str, ts: int | None, kind: str, message: str) -> None:
        self.error_handle.write(json.dumps({"engine":"SetupEngine","source":source,"window_start_ts":ts,"error_type":kind,"message":message},separators=(",",":")) + "\n"); self.error_handle.flush()
        self.warnings.add(f"{kind}:{source}")

    def tick(self) -> None:
        if time.monotonic() - self.last_heartbeat < HEARTBEAT_SECONDS: return
        self.refresh_inputs(); self.write_health(); self.heartbeat(); self.last_heartbeat = time.monotonic()

    def heartbeat(self) -> None:
        print("Setup Engine alive",flush=True)
        for name in ("evidence_packets","detector_events","structure_events","volume_profile_dna","volume_profile_events"):
            print(f"{name} processed={self.input_rows_processed[name]}",flush=True)
        print(f"setup_candidates_written={self.setup_candidates_written}",flush=True)
        print(f"last_window_ts={self.last_window_ts}",flush=True)

    def write_health(self) -> None:
        HEALTH_FILE.write_text(json.dumps({"status":"alive","input_rows_processed":self.input_rows_processed,
          "setup_candidates_written":self.setup_candidates_written,"last_window_ts":self.last_window_ts,
          "missing_inputs":sorted(self.missing_inputs),"warnings":sorted(self.warnings),
          "registry_validation_passed":self.registry_validation_passed},indent=2) + "\n",encoding="utf-8")

    def close(self) -> None:
        self.write_health(); self.output_handle.close(); self.error_handle.close()


def normalize_input(source: str, row: dict[str,Any]) -> dict[str,Any] | None:
    ts = safe_int(row.get("window_start_ts", row.get("source_window_ts")))
    symbol = str(row.get("symbol") or "BTCUSDT"); timeframe = str(row.get("timeframe") or "")
    if ts is None or not timeframe: return None
    base = {"source":source,"symbol":symbol,"timeframe":timeframe,"window_start_ts":ts,
            "window_end_ts":safe_int(row.get("window_end_ts",row.get("source_window_end_ts"))),"raw":row}
    if source == "evidence_packets":
        summary=row.get("evidence_summary",{}); base.update({"source":"evidence","event_id":str(row.get("packet_id") or f"evidence:{timeframe}:{ts}"),
         "event_types":list(summary.get("event_types",[])),"buy_side_events":list(summary.get("buy_side_events",[])),
         "sell_side_events":list(summary.get("sell_side_events",[])),"neutral_events":list(summary.get("neutral_events",[])),
         "unknown_side_events":list(summary.get("unknown_side_events",[])),"events":list(row.get("evidence_events",[]))})
    elif source in {"detector_events","structure_events","volume_profile_events"}:
        base.update({"source":{"detector_events":"detector","structure_events":"smart_money","volume_profile_events":"volume_profile"}[source],
          "event_id":str(row.get("event_id") or row.get("detector_event_id") or ""),"event_type":str(row.get("event_type") or ""),
          "side":str(row.get("side") or "unknown"),"direction":str(row.get("direction") or "unknown"),
          "level":optional_float(row.get("level")),"zone":row.get("zone") if isinstance(row.get("zone"),dict) else None})
    elif source == "volume_profile_dna":
        profile=row.get("profile",{}); value=profile.get("value_area",{}); location=row.get("location",{})
        base.update({"source":"volume_profile_dna","profile_shape":str(profile.get("profile_shape") or "unknown"),
          "poc":optional_float(profile.get("poc")),"vah":optional_float(value.get("vah")),"val":optional_float(value.get("val")),
          "location_vs_poc":str(location.get("location_vs_poc") or "unknown"),"location_vs_value":str(location.get("location_vs_value") or "unknown"),
          "auction":row.get("auction") if isinstance(row.get("auction"),dict) else {}})
    elif source == "context_dna": base["source"]="context"
    elif source == "smart_money_dna": base["source"]="smart_money_dna"
    elif source == "historical_outcome_observations": base["source"]="historical_outcome_observations"
    return base


def concepts(rows: list[dict[str,Any]]) -> list[dict[str,str]]:
    result=[]
    for row in rows:
        source=row.get("source","")
        if row.get("event_type"): result.append({"name":str(row["event_type"]),"side":str(row.get("side","unknown")),"direction":str(row.get("direction","unknown")),"source":source})
        if source == "evidence":
            for event in row.get("events",[]): result.append({"name":str(event.get("event_type","")),"side":str(event.get("side","unknown")),"direction":str(event.get("direction","unknown")),"source":"evidence"})
            for name in row.get("event_types",[]): result.append({"name":str(name),"side":"unknown","direction":"unknown","source":"evidence"})
        if source == "volume_profile_dna":
            if row.get("poc") is not None: result.append({"name":"poc_level_candidate","side":"neutral","direction":"flat","source":source})
            if row.get("vah") is not None and row.get("val") is not None: result.append({"name":"value_area_candidate","side":"neutral","direction":"flat","source":source})
            shape=row.get("profile_shape");
            if shape and shape != "unknown": result.append({"name":f"{shape}_profile_candidate" if shape not in {"trend_profile"} else "trend_profile_candidate","side":"neutral","direction":"flat","source":source})
            auction=row.get("auction",{})
            for name in ("acceptance_candidate","rejection_candidate","failed_auction_candidate","failed_action_return_to_value_candidate"):
                if auction.get(name) is not None: result.append({"name":name if name.startswith("failed") else name.replace("_candidate","_zone_candidate"),"side":"neutral","direction":"flat","source":source})
        if source == "smart_money_dna":
            for family, values in row.get("raw",{}).get("zones",{}).items():
                if values: result.append({"name":str(family).rstrip("s") + "_candidate","side":"neutral","direction":"unknown","source":source})
    return result


def match_groups(rows: list[dict[str,Any]], groups: list[list[str]], optional: bool=False) -> tuple[list[str],list[str]]:
    matched=[]; missing=[]
    for group in groups:
        found=next((concept for concept in group if matches(rows,concept)),None)
        if found: matched.append(found)
        elif not optional: missing.append(" or ".join(group))
    return matched,missing


def matches(rows: list[dict[str,Any]], expression: str) -> bool:
    parts=expression.split(); name=parts[0]; qualifier=" ".join(parts[1:])
    if name in {"location_vs_value","location_vs_poc","profile_shape"}:
        return any(str(row.get(name)) == qualifier for row in rows if row.get("source")=="volume_profile_dna")
    if name in {"buy_side","sell_side"}:
        target=name.split("_")[0]; return any(item["side"] in {target,"long" if target=="buy" else "short"} for item in concepts(rows))
    for item in concepts(rows):
        event=item["name"]
        if event != name and name.lower() not in event.lower(): continue
        if not qualifier: return True
        if qualifier in {"buy","sell","long","short"}:
            aliases={"buy":{"buy","long"},"long":{"buy","long"},"sell":{"sell","short"},"short":{"sell","short"}}
            if item["side"] in aliases[qualifier]: return True
        elif item["direction"] == qualifier: return True
    return False


def blocking(rows: list[dict[str,Any]], side: str) -> list[dict[str,Any]]:
    if side == "neutral": return []
    opposite={"long":{"sell","short"},"short":{"buy","long"}}[side]; result=[]
    for row in rows:
        if row.get("side") in opposite or row.get("direction") == ("down" if side=="long" else "up"):
            result.append(reference(row))
    snapshot=next((r for r in reversed(rows) if r.get("source")=="volume_profile_dna"),None)
    if snapshot and ((side=="long" and snapshot.get("location_vs_value")=="below_value") or (side=="short" and snapshot.get("location_vs_value")=="above_value")):
        result.append({"source":"volume_profile_dna","reason":"opposite_location_context"})
    return result


def location_context(row: dict[str,Any] | None) -> dict[str,Any]:
    if not row: return {"poc":None,"vah":None,"val":None,"location_vs_poc":"unknown","location_vs_value":"unknown","profile_shape":"unknown"}
    return {key:row.get(key) for key in ("poc","vah","val","location_vs_poc","location_vs_value","profile_shape")}


def auction_context(row: dict[str,Any] | None) -> dict[str,Any]:
    auction=row.get("auction",{}) if row else {}
    return {key:auction.get(key) for key in ("acceptance_candidate","rejection_candidate","failed_auction_candidate","failed_action_return_to_value_candidate")}


def reference(row: dict[str,Any]) -> dict[str,Any]:
    return {"source":row.get("source"),"event_id":row.get("event_id"),"event_type":row.get("event_type"),"window_start_ts":row.get("window_start_ts")}


def refs(rows: list[dict[str,Any]]) -> list[dict[str,Any]]: return unique_dicts([reference(row) for row in rows])
def source_refs(rows: list[dict[str,Any]], *sources: str) -> list[dict[str,Any]]: return unique_dicts([reference(row) for row in rows if row.get("source") in sources])
def unique(values: list[str]) -> list[str]: return list(dict.fromkeys(values))
def unique_dicts(values: list[dict[str,Any]]) -> list[dict[str,Any]]:
    seen=set(); out=[]
    for value in values:
        key=json.dumps(value,sort_keys=True)
        if key not in seen: seen.add(key); out.append(value)
    return out


def make_setup_id(key: tuple[str,str,str,int,str]) -> str:
    return hashlib.sha256("".join(map(str,key)).encode()).hexdigest()


def load_written_keys() -> set[tuple[str,str,str,int,str]]:
    result=set()
    if not CANDIDATES_FILE.exists(): return result
    with CANDIDATES_FILE.open("r",encoding="utf-8",errors="replace") as handle:
        for line in handle:
            try: row=json.loads(line)
            except json.JSONDecodeError: continue
            if row.get("layer") != "Layer-7": continue
            ts=safe_int(row.get("window_start_ts"))
            if ts is not None: result.add((str(row.get("setup_name")),str(row.get("symbol")),str(row.get("timeframe")),ts,str(row.get("side"))))
    return result


def run() -> None:
    engine=SetupEngine(); handles={name:None for name,path in INPUT_FILES.items() if path.suffix==".jsonl"}; json_mtime=None
    try:
        while True:
            activity=0
            profile=INPUT_FILES["calibration_profiles"]
            if profile.exists() and profile.stat().st_mtime != json_mtime:
                engine.process_json("calibration_profiles",profile); json_mtime=profile.stat().st_mtime
            for source,path in INPUT_FILES.items():
                if path.suffix != ".jsonl": continue
                if handles[source] is None:
                    if not path.exists(): continue
                    handles[source]=path.open("r",encoding="utf-8",errors="replace")
                while True:
                    line=handles[source].readline()
                    if not line: break
                    engine.process_line(source,line); activity+=1; engine.tick()
            engine.tick()
            if activity==0: time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in handles.values():
            if handle: handle.close()
        engine.close()


def safe_int(value: Any) -> int | None:
    try: return int(value) if value is not None else None
    except (TypeError,ValueError,OverflowError): return None
def optional_float(value: Any) -> float | None:
    try: return float(value) if value is not None else None
    except (TypeError,ValueError,OverflowError): return None
def relative_label(path: Path) -> str:
    try: return str(path.relative_to(ROOT_DIR)).replace("\\","/")
    except ValueError: return str(path).replace("\\","/")


if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: print("Stopped.",flush=True)
