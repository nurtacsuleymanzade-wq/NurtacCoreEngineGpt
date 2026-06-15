"""Layer-9 calibration engine for descriptive statistics measured from outcomes."""

import hashlib
import json
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from calibration_contracts import validate_calibration_contracts

ROOT_DIR=Path(__file__).resolve().parent;DATA_DIR=ROOT_DIR/"data"
OUTCOME_FILE=DATA_DIR/"historical_outcome_observations.jsonl"
OPTIONAL_FILES={"setup_candidates":DATA_DIR/"setup_candidates.jsonl","evidence_packets":DATA_DIR/"evidence_packets.jsonl",
"detector_events":DATA_DIR/"detector_events.jsonl","structure_events":DATA_DIR/"structure_events.jsonl",
"volume_profile_events":DATA_DIR/"volume_profile_events.jsonl","context_dna":DATA_DIR/"context_dna.jsonl"}
PROFILES_FILE=DATA_DIR/"calibration_profiles.json";EVENTS_FILE=DATA_DIR/"calibration_events.jsonl"
HEALTH_FILE=DATA_DIR/"calibration_health.json";ERRORS_FILE=DATA_DIR/"calibration_errors.jsonl"
EXPECTED_HORIZONS=("30s","60s","300s","900s","3600s");POLL_INTERVAL_SECONDS=0.5;WRITE_INTERVAL_SECONDS=60.0
NULL_SCORES={"hardcoded_confidence":None,"hardcoded_probability":None,"hardcoded_strength_score":None,"hardcoded_threshold":None}


class CalibrationEngine:
 def __init__(self)->None:
  errors=validate_calibration_contracts();self.registry_validation_passed=not errors
  if errors:raise RuntimeError("Calibration registry invalid: "+"; ".join(errors))
  DATA_DIR.mkdir(parents=True,exist_ok=True);self.metadata:dict[str,dict[str,Any]]={};self.groups:dict[str,dict[str,Any]]={}
  self.input_rows_processed={"historical_outcome_observations":0,**{name:0 for name in OPTIONAL_FILES}}
  self.profiles_written=0;self.calibration_events_written=0;self.last_observation_ts=0
  self.missing_inputs:set[str]=set();self.warnings:set[str]=set();self.last_write=time.monotonic()
  self.error_handle=ERRORS_FILE.open("a",encoding="utf-8");self.event_handle=EVENTS_FILE.open("a",encoding="utf-8")
  self.previous_counts=load_previous_counts();self.refresh_missing_inputs();self.write_health()

 def refresh_missing_inputs(self)->None:
  if OUTCOME_FILE.exists():self.missing_inputs.discard(relative_label(OUTCOME_FILE))
  else:self.missing_inputs.add(relative_label(OUTCOME_FILE))
  for path in OPTIONAL_FILES.values():
   if not path.exists():self.warnings.add(f"optional_input_missing:{relative_label(path)}")

 def index_metadata_line(self,source:str,line:str)->None:
  row=self.parse(source,line);self.input_rows_processed[source]+=1
  if row is None:return
  metadata=normalize_metadata(source,row)
  if metadata:
   self.metadata[metadata["source_event_id"]]=metadata

 def process_outcome_line(self,line:str)->None:
  row=self.parse("historical_outcome_observations",line);self.input_rows_processed["historical_outcome_observations"]+=1
  if row is None:return
  observation=normalize_outcome(row)
  if observation is None:self.write_error("historical_outcome_observations","outcome_normalization_failed");return
  self.last_observation_ts=max(self.last_observation_ts,observation["event_ts"])
  metadata=self.metadata.get(observation["source_event_id"],{})
  merged={**observation,**metadata,"source_type":observation["source_type"],"symbol":observation["symbol"],"timeframe":observation["timeframe"]}
  for spec in group_specs(merged):self.update_group(spec,merged)

 def update_group(self,spec:dict[str,Any],row:dict[str,Any])->None:
  key=spec["group_key"];group=self.groups.get(key)
  if group is None:
   group={**spec,"source_ids":set(),"horizons":defaultdict(new_horizon)};self.groups[key]=group
  group["source_ids"].add(row["source_event_id"]);h=group["horizons"][row["observation_window"]]
  raw=row["raw_return"];adjusted=side_adjust(raw,row.get("side","unknown"));mfe,mae=side_excursions(row,row.get("side","unknown"))
  h["sample_count"]+=1;h["raw_returns"].append(raw);h["raw_sum"]+=raw
  if adjusted is None:
   h["unknown_count"]+=1
  elif adjusted>0:h["favorable_count"]+=1
  elif adjusted<0:h["unfavorable_count"]+=1
  else:h["flat_count"]+=1
  if adjusted is not None:h["adjusted_returns"].append(adjusted);h["adjusted_sum"]+=adjusted
  if mfe is not None:h["mfe_sum"]+=mfe;h["mfe_count"]+=1
  if mae is not None:h["mae_sum"]+=mae;h["mae_count"]+=1

 def write_profiles(self)->None:
  profiles=[build_profile(group) for group in sorted(self.groups.values(),key=lambda x:x["group_key"])]
  payload={"layer":"Layer-9","engine":"CalibrationEngine","record_type":"calibration_profiles","generated_at":time.time(),"profiles":profiles}
  PROFILES_FILE.write_text(json.dumps(payload,indent=2)+"\n",encoding="utf-8");self.profiles_written=len(profiles)
  for profile in profiles:
   previous=self.previous_counts.get(profile["profile_id"])
   if previous==profile["sample_count"]:continue
   event_id="cal_"+hashlib.sha256(f"{profile['profile_id']}|{profile['sample_count']}".encode()).hexdigest()[:20]
   event={"layer":"Layer-9","engine":"CalibrationEngine","record_type":"calibration_event","event_id":event_id,
    "event_type":"calibration_profile_updated","profile_id":profile["profile_id"],"group_key":profile["group_key"],
    "sample_count":profile["sample_count"],"calibration_status":"measured_from_outcomes","scores":dict(NULL_SCORES),
    "validation":{"contract_found":True,"invariants_passed":True,"errors":[]}}
   self.event_handle.write(json.dumps(event,separators=(",",":"))+"\n");self.calibration_events_written+=1
   self.previous_counts[profile["profile_id"]]=profile["sample_count"]
  self.event_handle.flush();self.write_health();self.last_write=time.monotonic()

 def parse(self,source:str,line:str)->dict[str,Any]|None:
  try:row=json.loads(line)
  except json.JSONDecodeError as exc:self.write_error(source,f"json_parse_error:{exc}");return None
  if not isinstance(row,dict):self.write_error(source,"row_not_object");return None
  return row

 def write_error(self,source:str,detail:str)->None:
  self.error_handle.write(json.dumps({"engine":"CalibrationEngine","source":source,"detail":detail},separators=(",",":"))+"\n");self.error_handle.flush();self.warnings.add(f"error:{source}")

 def tick(self)->None:
  if time.monotonic()-self.last_write>=WRITE_INTERVAL_SECONDS:self.write_profiles()

 def write_health(self)->None:
  HEALTH_FILE.write_text(json.dumps({"status":"alive","input_rows_processed":self.input_rows_processed,
   "profiles_written":self.profiles_written,"calibration_events_written":self.calibration_events_written,
   "last_observation_ts":self.last_observation_ts,"missing_inputs":sorted(self.missing_inputs),
   "warnings":sorted(self.warnings),"registry_validation_passed":self.registry_validation_passed},indent=2)+"\n",encoding="utf-8")

 def close(self)->None:
  self.write_profiles();self.error_handle.close();self.event_handle.close()


def new_horizon()->dict[str,Any]:
 return {"sample_count":0,"favorable_count":0,"unfavorable_count":0,"flat_count":0,"unknown_count":0,
 "raw_returns":[],"raw_sum":0.0,"adjusted_returns":[],"adjusted_sum":0.0,"mfe_sum":0.0,"mfe_count":0,"mae_sum":0.0,"mae_count":0}

def normalize_outcome(row:dict[str,Any])->dict[str,Any]|None:
 try:
  start=float(row["start_price"]);close=float(row["close_price"]);highest=float(row["highest_price"]);lowest=float(row["lowest_price"])
 except (KeyError,TypeError,ValueError,OverflowError):return None
 if start==0:return None
 path=row.get("price_path",{});max_exc=number(path.get("max_excursion"),highest-start);min_exc=number(path.get("min_excursion"),lowest-start)
 return {"source_event_id":str(row.get("source_event_id") or ""),"source_type":str(row.get("source_type") or "unknown"),
 "symbol":str(row.get("symbol") or "unknown"),"timeframe":str(row.get("timeframe") or "unknown"),
 "event_ts":int(row.get("event_ts",0)),"observation_window":str(row.get("observation_window") or "unknown"),
 "raw_return":(close-start)/start,"max_excursion_return":max_exc/start,"min_excursion_return":min_exc/start}

def normalize_metadata(source:str,row:dict[str,Any])->dict[str,Any]|None:
 ts=int(row.get("window_start_ts",row.get("source_window_ts",0)) or 0);tf=str(row.get("timeframe") or "unknown");symbol=str(row.get("symbol") or "BTCUSDT")
 if source=="setup_candidates":identifier=row.get("setup_id");event_type=row.get("setup_name");side=row.get("side");setup_name=event_type;direction="unknown"
 elif source=="detector_events":identifier=row.get("detector_event_id") or row.get("event_id");event_type=row.get("event_type");side=row.get("side");setup_name=None;direction=row.get("direction")
 elif source in {"structure_events","volume_profile_events"}:identifier=row.get("event_id");event_type=row.get("event_type");side=row.get("side");setup_name=None;direction=row.get("direction")
 elif source=="evidence_packets":
  identifier=f"evidence:{symbol}:{tf}:{ts}";summary=row.get("evidence_summary",{});types=sorted(map(str,summary.get("event_types",[])))
  event_type="evidence_packet";side=evidence_side(summary);setup_name=None;direction="unknown"
  signature=hashlib.sha256("|".join(types).encode()).hexdigest()[:20]
  return {"source_event_id":identifier,"event_type":event_type,"side":side,"direction":direction,"setup_name":None,"pattern_signature":signature,"location_context":"unknown"}
 else:return None
 if not identifier:return None
 location="unknown"
 if source=="volume_profile_events":
  zone=row.get("zone");location=json.dumps(zone,sort_keys=True,separators=(",",":")) if isinstance(zone,dict) else "level" if row.get("level") is not None else "unknown"
 return {"source_event_id":str(identifier),"event_type":str(event_type or "unknown"),"side":str(side or "unknown"),
 "direction":str(direction or "unknown"),"setup_name":setup_name,"pattern_signature":None,"location_context":location}

def group_specs(row:dict[str,Any])->list[dict[str,Any]]:
 source=row.get("source_type","unknown");event=row.get("event_type","unknown");tf=row.get("timeframe","unknown");side=row.get("side","unknown");symbol=row.get("symbol","unknown")
 specs=[]
 def add(ptype:str,parts:list[Any],**extra:Any)->None:
  key="|".join(map(str,[ptype,*parts]));specs.append({"profile_type":ptype,"group_key":key,"symbol":extra.get("symbol"),
   "timeframe":tf,"source_type":source,"event_type":event,"setup_name":extra.get("setup_name"),
   "pattern_signature":extra.get("pattern_signature"),"side":side})
 add("event",[source,event,tf,side]);add("event",[symbol,tf,event,side],symbol=symbol)
 add("timeframe",[source,tf]);add("side_adjusted",[source,event,tf,side])
 if source=="setup":add("setup",[row.get("setup_name",event),tf,side],setup_name=row.get("setup_name",event))
 if source=="structure":add("structure",[event,row.get("direction","unknown"),tf])
 if source=="volume_profile":add("volume_profile",[event,row.get("location_context","unknown")])
 if source=="evidence":add("evidence_pattern",[row.get("pattern_signature","unknown"),tf,side],pattern_signature=row.get("pattern_signature"))
 return specs

def build_profile(group:dict[str,Any])->dict[str,Any]:
 horizons={};missing=[]
 for label in EXPECTED_HORIZONS:
  h=group["horizons"].get(label)
  if not h or h["sample_count"]==0:missing.append(label);continue
  count=h["sample_count"];raw=sorted(h["raw_returns"]);adjusted=sorted(h["adjusted_returns"])
  horizons[label]={"sample_count":count,"favorable_count":h["favorable_count"],"unfavorable_count":h["unfavorable_count"],
   "flat_count":h["flat_count"],"unknown_count":h["unknown_count"],"favorable_rate":h["favorable_count"]/count,
   "unfavorable_rate":h["unfavorable_count"]/count,"avg_raw_return":h["raw_sum"]/count,
   "median_raw_return":statistics.median(raw),"avg_side_adjusted_return":h["adjusted_sum"]/len(adjusted) if adjusted else None,
   "median_side_adjusted_return":statistics.median(adjusted) if adjusted else None,
   "avg_max_favorable_return":h["mfe_sum"]/h["mfe_count"] if h["mfe_count"] else None,
   "avg_max_adverse_return":h["mae_sum"]/h["mae_count"] if h["mae_count"] else None,
   "return_distribution":{"min":raw[0],"max":raw[-1],"p25":percentile(raw,0.25),"p50":percentile(raw,0.5),"p75":percentile(raw,0.75)}}
 sample_count=len(group["source_ids"]);profile_id="cp_"+hashlib.sha256(group["group_key"].encode()).hexdigest()[:24]
 return {"profile_id":profile_id,"profile_type":group["profile_type"],"group_key":group["group_key"],"symbol":group.get("symbol"),
 "timeframe":group["timeframe"],"source_type":group["source_type"],"event_type":group["event_type"],
 "setup_name":group.get("setup_name"),"pattern_signature":group.get("pattern_signature"),"side":group["side"],
 "sample_count":sample_count,"sample_status":"observed_sample" if sample_count>0 else "insufficient_data",
 "missing_horizons":missing,"horizons":horizons,"calibration_status":"measured_from_outcomes","scores":dict(NULL_SCORES),
 "validation":{"source_observations_found":sample_count>0,"no_hardcoded_values":True,"errors":[]}}

def side_adjust(raw:float,side:str)->float|None:
 if side in {"buy","long"}:return raw
 if side in {"sell","short"}:return -raw
 return None
def side_excursions(row:dict[str,Any],side:str)->tuple[float|None,float|None]:
 up=row["max_excursion_return"];down=row["min_excursion_return"]
 if side in {"buy","long"}:return up,down
 if side in {"sell","short"}:return -down,-up
 return None,None
def percentile(values:list[float],q:float)->float:
 if len(values)==1:return values[0]
 pos=(len(values)-1)*q;low=int(pos);high=min(low+1,len(values)-1);fraction=pos-low
 return values[low]+(values[high]-values[low])*fraction
def number(value:Any,default:float)->float:
 try:return float(value)
 except (TypeError,ValueError,OverflowError):return default
def evidence_side(summary:dict[str,Any])->str:
 buy=bool(summary.get("buy_side_events"));sell=bool(summary.get("sell_side_events"))
 return "buy" if buy and not sell else "sell" if sell and not buy else "neutral" if buy or sell else "unknown"
def load_previous_counts()->dict[str,int]:
 if not PROFILES_FILE.exists():return {}
 try:payload=json.loads(PROFILES_FILE.read_text(encoding="utf-8"))
 except (OSError,json.JSONDecodeError):return {}
 return {str(p.get("profile_id")):int(p.get("sample_count",0)) for p in payload.get("profiles",[]) if isinstance(p,dict)}
def relative_label(path:Path)->str:
 try:return str(path.relative_to(ROOT_DIR)).replace("\\","/")
 except ValueError:return str(path).replace("\\","/")

def run()->None:
 engine=CalibrationEngine();metadata_handles={name:None for name in OPTIONAL_FILES};outcome_handle=None
 try:
  # Metadata is indexed first so outcome source IDs can be enriched deterministically.
  for name,path in OPTIONAL_FILES.items():
   if not path.exists():continue
   metadata_handles[name]=path.open("r",encoding="utf-8",errors="replace")
   for line in metadata_handles[name]:engine.index_metadata_line(name,line);engine.tick()
  if OUTCOME_FILE.exists():outcome_handle=OUTCOME_FILE.open("r",encoding="utf-8",errors="replace")
  while True:
   activity=0
   if outcome_handle is None and OUTCOME_FILE.exists():outcome_handle=OUTCOME_FILE.open("r",encoding="utf-8",errors="replace")
   if outcome_handle:
    while True:
     line=outcome_handle.readline()
     if not line:break
     engine.process_outcome_line(line);activity+=1;engine.tick()
   for name,path in OPTIONAL_FILES.items():
    handle=metadata_handles[name]
    if handle is None:
     if not path.exists():continue
     handle=path.open("r",encoding="utf-8",errors="replace");metadata_handles[name]=handle
    while True:
     line=handle.readline()
     if not line:break
     engine.index_metadata_line(name,line);activity+=1;engine.tick()
   engine.tick()
   if activity==0:time.sleep(POLL_INTERVAL_SECONDS)
 finally:
  if outcome_handle:outcome_handle.close()
  for handle in metadata_handles.values():
   if handle:handle.close()
  engine.close()

if __name__=="__main__":
 try:run()
 except KeyboardInterrupt:print("Stopped.",flush=True)
