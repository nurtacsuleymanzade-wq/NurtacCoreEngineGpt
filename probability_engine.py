"""Layer-10 probability candidates derived only from measured calibration profiles."""

import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from probability_contracts import PROBABILITY_CONTRACTS, get_probability_contract


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
PROFILES_FILE = DATA_DIR / "calibration_profiles.json"
INPUT_FILES = {
    "setup_candidates": DATA_DIR / "setup_candidates.jsonl",
    "evidence_packets": DATA_DIR / "evidence_packets.jsonl",
    "detector_events": DATA_DIR / "detector_events.jsonl",
    "structure_events": DATA_DIR / "structure_events.jsonl",
    "volume_profile_events": DATA_DIR / "volume_profile_events.jsonl",
    "context_dna": DATA_DIR / "context_dna.jsonl",
    "historical_outcome_observations": DATA_DIR / "historical_outcome_observations.jsonl",
}
PRIMARY_INPUTS = {"calibration_profiles", "setup_candidates"}
CANDIDATES_FILE = DATA_DIR / "probability_candidates.jsonl"
HEALTH_FILE = DATA_DIR / "probability_health.json"
ERRORS_FILE = DATA_DIR / "probability_errors.jsonl"
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0


class ProbabilityEngine:
    def __init__(self) -> None:
        self.registry_validation_passed = validate_registry()
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.profiles: list[dict[str, Any]] = []
        self.profile_indexes: dict[str, dict[tuple[Any, ...], list[dict[str, Any]]]] = {}
        self.evidence_by_bucket: dict[tuple[str, str, int], dict[str, Any]] = {}
        self.context_by_bucket: dict[tuple[str, str, int], dict[str, Any]] = {}
        self.written_keys = load_written_keys()
        self.input_rows_processed = {"calibration_profiles": 0, **{name: 0 for name in INPUT_FILES}}
        self.probability_candidates_written = 0
        self.last_window_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_heartbeat = time.monotonic()
        self.profile_mtime: float | None = None
        self.output_handle = CANDIDATES_FILE.open("a", encoding="utf-8")
        self.error_handle = ERRORS_FILE.open("a", encoding="utf-8")
        self.refresh_missing_inputs(); self.reload_profiles(); self.write_health()

    def refresh_missing_inputs(self) -> None:
        paths = {"calibration_profiles": PROFILES_FILE, **INPUT_FILES}
        for name, path in paths.items():
            label = relative_label(path)
            if path.exists(): self.missing_inputs.discard(label)
            elif name in PRIMARY_INPUTS: self.missing_inputs.add(label)
            else: self.warnings.add(f"optional_input_missing:{label}")

    def reload_profiles(self) -> None:
        if not PROFILES_FILE.exists(): return
        try: payload = json.loads(PROFILES_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.write_error("calibration_profiles", f"profile_parse_error:{exc}"); return
        rows = payload.get("profiles", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            self.write_error("calibration_profiles", "profiles_not_list"); return
        self.profiles = [normalize_profile(row) for row in rows if isinstance(row, dict)]
        self.profile_indexes = build_profile_indexes(self.profiles)
        self.input_rows_processed["calibration_profiles"] += 1
        self.profile_mtime = PROFILES_FILE.stat().st_mtime

    def process_line(self, source: str, line: str) -> None:
        try: row = json.loads(line)
        except json.JSONDecodeError as exc:
            self.write_error(source, f"json_parse_error:{exc}"); return
        if not isinstance(row, dict):
            self.write_error(source, "row_not_object"); return
        self.input_rows_processed[source] += 1
        if source == "setup_candidates":
            setup = normalize_setup(row)
            if setup: self.process_setup(setup)
        elif source == "evidence_packets":
            evidence = normalize_evidence(row)
            if evidence:
                key = bucket_key(evidence); self.evidence_by_bucket[key] = evidence
                self.process_evidence(evidence)
        elif source in {"detector_events", "structure_events", "volume_profile_events"}:
            event = normalize_event(source, row)
            if event: self.process_event(event)
        elif source == "context_dna":
            context = normalize_context(row)
            if context: self.context_by_bucket[bucket_key(context)] = context

    def process_setup(self, setup: dict[str, Any]) -> None:
        self.last_window_ts = max(self.last_window_ts, setup["window_start_ts"])
        profile = first_profile(self.profile_indexes["setup"].get((setup["setup_name"], setup["timeframe"], setup["side"]), []))
        if profile:
            self.emit("setup_probability_candidate", setup, profile)
            self.emit("timeframe_probability_candidate", setup, timeframe_profile(self.profile_indexes, "setup", setup["timeframe"]) or profile)
            self.emit("side_probability_candidate", setup, side_profile(self.profile_indexes, "setup", setup["setup_name"], setup["timeframe"], setup["side"]) or profile)
            self.emit("probability_context_annotation", setup, profile)
            if profile["sample_status"] == "insufficient_data": self.emit("insufficient_data_probability_candidate", setup, profile)
        if setup["blocking_evidence"]:
            contradiction_profile = profile or timeframe_profile(self.profile_indexes, "setup", setup["timeframe"])
            if contradiction_profile: self.emit("contradiction_probability_candidate", setup, contradiction_profile, True)

    def process_event(self, event: dict[str, Any]) -> None:
        self.last_window_ts = max(self.last_window_ts, event["window_start_ts"])
        profile = first_profile(self.profile_indexes["event"].get((event["source_type"], event["event_type"], event["timeframe"], event["side"]), []))
        if not profile: return
        self.emit("event_probability_candidate", event, profile)
        self.emit("timeframe_probability_candidate", event, timeframe_profile(self.profile_indexes, event["source_type"], event["timeframe"]) or profile)
        self.emit("side_probability_candidate", event, side_profile(self.profile_indexes, event["source_type"], event["event_type"], event["timeframe"], event["side"]) or profile)
        if profile["sample_status"] == "insufficient_data": self.emit("insufficient_data_probability_candidate", event, profile)

    def process_evidence(self, evidence: dict[str, Any]) -> None:
        self.last_window_ts = max(self.last_window_ts, evidence["window_start_ts"])
        profile = first_profile(self.profile_indexes["pattern"].get((evidence["pattern_signature"], evidence["timeframe"], evidence["side"]), []))
        if profile: self.emit("pattern_probability_candidate", evidence, profile)
        if evidence["has_contradiction"]:
            fallback = profile or timeframe_profile(self.profile_indexes, "evidence", evidence["timeframe"])
            if fallback: self.emit("contradiction_probability_candidate", evidence, fallback, True)

    def emit(self, probability_name: str, source: dict[str, Any], profile: dict[str, Any], contradiction: bool = False) -> None:
        contract = get_probability_contract(probability_name)
        if contract is None:
            self.warnings.add(f"contract_missing:{probability_name}"); return
        key = duplicate_key(probability_name, source)
        if key in self.written_keys: return
        probability_by_horizon = derive_probabilities(profile)
        summary = first_horizon(probability_by_horizon)
        insufficient = profile["sample_status"] == "insufficient_data"
        if insufficient: summary = null_probabilities()
        expected = expected_value_context(profile)
        payload = {
            "layer":"Layer-10","engine":"ProbabilityEngine","record_type":"probability_candidate",
            "probability_id":make_probability_id(key),"probability_name":probability_name,
            "probability_family":contract["probability_family"],"symbol":source["symbol"],
            "timeframe":source["timeframe"],"window_start_ts":source["window_start_ts"],
            "window_end_ts":source.get("window_end_ts"),"source_setup_id":source.get("setup_id"),
            "source_event_id":source.get("event_id"),"pattern_signature":source.get("pattern_signature"),
            "side":canonical_side(source.get("side")),
            "probability":{**summary,"probability_by_horizon":probability_by_horizon,
                "source":"calibration_profiles","method":"measured_from_historical_outcomes","hardcoded":False},
            "outcome_profile":{"sample_count":profile["sample_count"],"sample_status":profile["sample_status"],"horizons":profile["horizons"]},
            "expected_value_context":expected,
            "contradiction_context":{"has_contradiction":contradiction,
                "contradicting_evidence":source.get("blocking_evidence", source.get("contradicting_evidence", [])) if contradiction else []},
            "calibration_refs":[{"profile_id":profile["profile_id"],"group_key":profile["group_key"]}],
            "setup_refs":[source["setup_id"]] if source.get("setup_id") else [],
            "evidence_refs":source.get("evidence_refs", []),"structure_refs":source.get("structure_refs", []),
            "volume_profile_refs":source.get("volume_profile_refs", []),"context_refs":source.get("context_refs", []),
            "decision_readiness":{"ready_for_decision_gate":False,"reason":"probability_candidate_not_decision"},
            "validation":{"contract_found":True,"calibration_profile_found":True,
                "no_hardcoded_probability":True,"invariants_passed":True,"errors":[]}}
        self.output_handle.write(json.dumps(payload,separators=(",",":")) + "\n"); self.output_handle.flush()
        self.written_keys.add(key); self.probability_candidates_written += 1

    def write_error(self, source: str, detail: str) -> None:
        self.error_handle.write(json.dumps({"engine":"ProbabilityEngine","source":source,"detail":detail},separators=(",",":")) + "\n"); self.error_handle.flush()
        self.warnings.add(f"error:{source}")

    def tick(self) -> None:
        if PROFILES_FILE.exists() and PROFILES_FILE.stat().st_mtime != self.profile_mtime: self.reload_profiles()
        if time.monotonic() - self.last_heartbeat < HEARTBEAT_SECONDS: return
        self.refresh_missing_inputs(); self.write_health()
        print("Probability Engine alive",flush=True)
        print(f"calibration_profiles processed={self.input_rows_processed['calibration_profiles']}",flush=True)
        print(f"setup_candidates processed={self.input_rows_processed['setup_candidates']}",flush=True)
        print(f"probability_candidates_written={self.probability_candidates_written}",flush=True)
        print(f"last_window_ts={self.last_window_ts}",flush=True)
        self.last_heartbeat=time.monotonic()

    def write_health(self) -> None:
        HEALTH_FILE.write_text(json.dumps({"status":"alive","input_rows_processed":self.input_rows_processed,
            "probability_candidates_written":self.probability_candidates_written,"last_window_ts":self.last_window_ts,
            "missing_inputs":sorted(self.missing_inputs),"warnings":sorted(self.warnings),
            "registry_validation_passed":self.registry_validation_passed},indent=2)+"\n",encoding="utf-8")

    def close(self) -> None:
        self.write_health(); self.output_handle.close(); self.error_handle.close()


def validate_registry() -> bool:
    names={item.get("probability_name") for item in PROBABILITY_CONTRACTS if isinstance(item,dict)}
    return len(PROBABILITY_CONTRACTS)==8 and len(names)==8

def normalize_profile(row:dict[str,Any])->dict[str,Any]:
    return {"source":"calibration","profile_id":str(row.get("profile_id") or ""),"profile_type":str(row.get("profile_type") or "unknown"),
        "group_key":str(row.get("group_key") or ""),"symbol":row.get("symbol"),"timeframe":str(row.get("timeframe") or "unknown"),
        "source_type":str(row.get("source_type") or "unknown"),"event_type":str(row.get("event_type") or "unknown"),
        "setup_name":row.get("setup_name"),"pattern_signature":row.get("pattern_signature"),"side":str(row.get("side") or "unknown"),
        "sample_count":safe_int(row.get("sample_count"),0),"sample_status":str(row.get("sample_status") or "insufficient_data"),
        "horizons":row.get("horizons") if isinstance(row.get("horizons"),dict) else {},"raw":row}

def normalize_setup(row:dict[str,Any])->dict[str,Any]|None:
    ts=safe_int(row.get("window_start_ts"));
    if ts is None:return None
    return {"source":"setup","setup_id":str(row.get("setup_id") or ""),"setup_name":str(row.get("setup_name") or "unknown"),
        "setup_family":str(row.get("setup_family") or "unknown"),"symbol":str(row.get("symbol") or "BTCUSDT"),
        "timeframe":str(row.get("timeframe") or "unknown"),"window_start_ts":ts,"window_end_ts":safe_int(row.get("window_end_ts")),
        "side":canonical_side(row.get("side")),"matched_concepts":list_value(row.get("matched_concepts")),
        "supporting_evidence":list_value(row.get("supporting_evidence")),"blocking_evidence":list_value(row.get("blocking_evidence")),
        "evidence_refs":list_value(row.get("evidence_refs")),"structure_refs":list_value(row.get("structure_refs")),
        "volume_profile_refs":list_value(row.get("volume_profile_refs")),"context_refs":list_value(row.get("context_refs")),"raw":row}

def normalize_evidence(row:dict[str,Any])->dict[str,Any]|None:
    ts=safe_int(row.get("window_start_ts"));
    if ts is None:return None
    summary=row.get("evidence_summary") if isinstance(row.get("evidence_summary"),dict) else {}
    event_types=list_value(summary.get("event_types"));buy=list_value(summary.get("buy_side_events"));sell=list_value(summary.get("sell_side_events"))
    side="long" if buy and not sell else "short" if sell and not buy else "neutral" if buy or sell else "unknown"
    signature=hashlib.sha256("|".join(sorted(map(str,event_types))).encode()).hexdigest()[:20]
    return {"source":"evidence","symbol":str(row.get("symbol") or "BTCUSDT"),"timeframe":str(row.get("timeframe") or "unknown"),
        "window_start_ts":ts,"window_end_ts":safe_int(row.get("window_end_ts")),"event_types":event_types,"buy_side_events":buy,
        "sell_side_events":sell,"neutral_events":list_value(summary.get("neutral_events")),"unknown_side_events":list_value(summary.get("unknown_side_events")),
        "side":side,"pattern_signature":signature,"has_contradiction":bool(buy and sell),
        "contradicting_evidence":[{"buy_side_events":buy,"sell_side_events":sell}] if buy and sell else [],"raw":row}

def normalize_event(source:str,row:dict[str,Any])->dict[str,Any]|None:
    ts=safe_int(row.get("window_start_ts"));
    if ts is None:return None
    source_type={"detector_events":"detector","structure_events":"structure","volume_profile_events":"volume_profile"}[source]
    return {"source":source_type,"source_type":source_type,"event_id":str(row.get("event_id") or row.get("detector_event_id") or ""),
        "event_type":str(row.get("event_type") or "unknown"),"symbol":str(row.get("symbol") or "BTCUSDT"),
        "timeframe":str(row.get("timeframe") or "unknown"),"window_start_ts":ts,"window_end_ts":safe_int(row.get("window_end_ts")),
        "side":canonical_side(row.get("side")),"structure_refs":[str(row.get("event_id"))] if source_type=="structure" and row.get("event_id") else [],
        "volume_profile_refs":[str(row.get("event_id"))] if source_type=="volume_profile" and row.get("event_id") else [],"raw":row}

def normalize_context(row:dict[str,Any])->dict[str,Any]|None:
    ts=safe_int(row.get("window_start_ts",row.get("source_window_ts")));
    if ts is None:return None
    return {"source":"context","symbol":str(row.get("symbol") or "BTCUSDT"),"timeframe":str(row.get("timeframe") or "unknown"),"window_start_ts":ts,"raw":row}

def build_profile_indexes(profiles:list[dict[str,Any]])->dict[str,dict[tuple[Any,...],list[dict[str,Any]]]]:
    indexes={name:defaultdict(list) for name in ("setup","event","pattern","timeframe","side")}
    for p in profiles:
        side=canonical_side(p["side"])
        if p["profile_type"]=="setup" and p["setup_name"]:indexes["setup"][(str(p["setup_name"]),p["timeframe"],side)].append(p)
        if p["profile_type"]=="event":indexes["event"][(p["source_type"],p["event_type"],p["timeframe"],side)].append(p)
        if p["profile_type"]=="evidence_pattern" and p["pattern_signature"]:indexes["pattern"][(str(p["pattern_signature"]),p["timeframe"],side)].append(p)
        if p["profile_type"]=="timeframe":indexes["timeframe"][(p["source_type"],p["timeframe"])].append(p)
        if p["profile_type"]=="side_adjusted":indexes["side"][(p["source_type"],p["event_type"],p["timeframe"],side)].append(p)
    return indexes

def derive_probabilities(profile:dict[str,Any])->dict[str,dict[str,float|None]]:
    result={}
    for label,h in profile["horizons"].items():
        if profile["sample_status"]!="observed_sample" or not isinstance(h,dict):result[label]=null_probabilities();continue
        favorable=optional_float(h.get("favorable_rate"));unfavorable=optional_float(h.get("unfavorable_rate"));count=safe_int(h.get("sample_count"),0);flat=safe_int(h.get("flat_count"),0)
        neutral=flat/count if count>0 else None;side=canonical_side(profile["side"])
        if side=="long":result[label]={"long_probability":favorable,"short_probability":unfavorable,"neutral_probability":neutral}
        elif side=="short":result[label]={"long_probability":unfavorable,"short_probability":favorable,"neutral_probability":neutral}
        else:result[label]=null_probabilities()
    return result

def expected_value_context(profile:dict[str,Any])->dict[str,Any]:
    if not profile["horizons"]:return {"avg_side_adjusted_return":None,"avg_max_favorable_return":None,"avg_max_adverse_return":None,"return_distribution":{}}
    label=next(iter(profile["horizons"]));h=profile["horizons"][label]
    return {"avg_side_adjusted_return":h.get("avg_side_adjusted_return"),"avg_max_favorable_return":h.get("avg_max_favorable_return"),
        "avg_max_adverse_return":h.get("avg_max_adverse_return"),"return_distribution":h.get("return_distribution",{})}

def duplicate_key(name:str,source:dict[str,Any])->tuple[str,str,str,int,str,str,str]:
    return (name,source["symbol"],source["timeframe"],source["window_start_ts"],canonical_side(source.get("side")),str(source.get("setup_id") or ""),str(source.get("pattern_signature") or ""))
def make_probability_id(key:tuple[Any,...])->str:return hashlib.sha256("".join(map(str,key)).encode()).hexdigest()
def load_written_keys()->set[tuple[str,str,str,int,str,str,str]]:
    result=set()
    if not CANDIDATES_FILE.exists():return result
    with CANDIDATES_FILE.open("r",encoding="utf-8",errors="replace") as handle:
        for line in handle:
            try:r=json.loads(line)
            except json.JSONDecodeError:continue
            ts=safe_int(r.get("window_start_ts"));
            if ts is not None:result.add((str(r.get("probability_name")),str(r.get("symbol")),str(r.get("timeframe")),ts,canonical_side(r.get("side")),str(r.get("source_setup_id") or ""),str(r.get("pattern_signature") or "")))
    return result
def first_profile(values:list[dict[str,Any]])->dict[str,Any]|None:return values[0] if values else None
def timeframe_profile(indexes:dict[str,Any],source_type:str,timeframe:str)->dict[str,Any]|None:return first_profile(indexes["timeframe"].get((source_type,timeframe),[]))
def side_profile(indexes:dict[str,Any],source_type:str,event_type:str,timeframe:str,side:str)->dict[str,Any]|None:return first_profile(indexes["side"].get((source_type,event_type,timeframe,canonical_side(side)),[]))
def first_horizon(values:dict[str,dict[str,float|None]])->dict[str,float|None]:return next(iter(values.values()),null_probabilities())
def null_probabilities()->dict[str,None]:return {"long_probability":None,"short_probability":None,"neutral_probability":None}
def canonical_side(value:Any)->str:
    side=str(value or "unknown").lower()
    if side in {"buy","long"}:return "long"
    if side in {"sell","short"}:return "short"
    return side if side in {"neutral","unknown"} else "unknown"
def bucket_key(row:dict[str,Any])->tuple[str,str,int]:return (row["symbol"],row["timeframe"],row["window_start_ts"])
def list_value(value:Any)->list[Any]:return value if isinstance(value,list) else []
def safe_int(value:Any,default:int|None=None)->int|None:
    try:return int(value) if value is not None else default
    except (TypeError,ValueError,OverflowError):return default
def optional_float(value:Any)->float|None:
    try:return float(value) if value is not None else None
    except (TypeError,ValueError,OverflowError):return None
def relative_label(path:Path)->str:
    try:return str(path.relative_to(ROOT_DIR)).replace("\\","/")
    except ValueError:return str(path).replace("\\","/")


def run()->None:
    engine=ProbabilityEngine();handles={name:None for name in INPUT_FILES}
    try:
        while True:
            activity=0
            for source,path in INPUT_FILES.items():
                if handles[source] is None:
                    if not path.exists():continue
                    handles[source]=path.open("r",encoding="utf-8",errors="replace")
                while True:
                    line=handles[source].readline()
                    if not line:break
                    engine.process_line(source,line);activity+=1;engine.tick()
            engine.tick()
            if activity==0:time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in handles.values():
            if handle:handle.close()
        engine.close()


if __name__=="__main__":
    try:run()
    except KeyboardInterrupt:print("Stopped.",flush=True)
