"""Verify Layer-10 probability candidates against calibration profiles."""

import json
from collections import deque
from pathlib import Path
from typing import Any

from probability_contracts import get_probability_contract

ROOT=Path(__file__).resolve().parent;DATA=ROOT/"data";CANDIDATES=DATA/"probability_candidates.jsonl";PROFILES=DATA/"calibration_profiles.json";HEALTH=DATA/"probability_health.json";REPORT=DATA/"probability_engine_verification_report.json"

def verify()->dict[str,Any]:
    errors=[];rows=deque(maxlen=100);profiles={}
    if PROFILES.exists():
        try:payload=json.loads(PROFILES.read_text(encoding="utf-8"))
        except json.JSONDecodeError:payload={};errors.append("calibration_profiles.json invalid")
        profiles={str(p.get("profile_id")):p for p in payload.get("profiles",[]) if isinstance(p,dict)}
    if not CANDIDATES.exists():errors.append("probability_candidates.jsonl is missing")
    else:
        with CANDIDATES.open("r",encoding="utf-8",errors="replace") as handle:
            for number,line in enumerate(handle,1):
                try:row=json.loads(line)
                except json.JSONDecodeError:continue
                if isinstance(row,dict):rows.append((number,row))
    if not rows:errors.append("no readable probability candidates")
    for number,row in rows:
        label=f"line {number}";name=str(row.get("probability_name"))
        if get_probability_contract(name) is None:errors.append(f"{label}: probability_name missing from registry")
        probability=row.get("probability",{})
        if probability.get("method")!="measured_from_historical_outcomes":errors.append(f"{label}: invalid probability method")
        if probability.get("hardcoded") is not False:errors.append(f"{label}: probability.hardcoded must be false")
        if row.get("validation",{}).get("no_hardcoded_probability") is not True:errors.append(f"{label}: no_hardcoded_probability must be true")
        if row.get("decision_readiness",{}).get("ready_for_decision_gate") is not False:errors.append(f"{label}: decision readiness must be false")
        summary={k:probability.get(k) for k in ("long_probability","short_probability","neutral_probability")}
        if row.get("outcome_profile",{}).get("sample_status")=="insufficient_data" and any(v is not None for v in summary.values()):errors.append(f"{label}: insufficient data probabilities must be null")
        refs=row.get("calibration_refs",[]);profile_id=str(refs[0].get("profile_id")) if refs and isinstance(refs[0],dict) else "";profile=profiles.get(profile_id)
        if profile is None:errors.append(f"{label}: calibration profile reference not found");continue
        expected=derive(profile);actual=probability.get("probability_by_horizon",{})
        if actual!=expected:errors.append(f"{label}: probability values do not match calibration profile")
    if not HEALTH.exists():errors.append("probability_health.json is missing")
    else:
        try:health=json.loads(HEALTH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:health={};errors.append("probability_health.json invalid")
        if health.get("registry_validation_passed") is not True:errors.append("registry_validation_passed must be true")
    report={"checked_candidates":len(rows),"failed":len(errors),"errors":errors,"test_passed":not errors}
    REPORT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8");return report

def derive(profile:dict[str,Any])->dict[str,dict[str,float|None]]:
    result={};side=canonical(profile.get("side"));status=profile.get("sample_status")
    for label,h in profile.get("horizons",{}).items():
        if status!="observed_sample" or not isinstance(h,dict):result[label]=nulls();continue
        favorable=h.get("favorable_rate");unfavorable=h.get("unfavorable_rate");count=h.get("sample_count",0);flat=h.get("flat_count",0);neutral=flat/count if isinstance(count,int) and count>0 else None
        if side=="long":result[label]={"long_probability":favorable,"short_probability":unfavorable,"neutral_probability":neutral}
        elif side=="short":result[label]={"long_probability":unfavorable,"short_probability":favorable,"neutral_probability":neutral}
        else:result[label]=nulls()
    return result
def canonical(value:Any)->str:
    side=str(value or "unknown").lower();return "long" if side in {"buy","long"} else "short" if side in {"sell","short"} else side
def nulls()->dict[str,None]:return {"long_probability":None,"short_probability":None,"neutral_probability":None}

def main()->int:
    r=verify();print("PROBABILITY ENGINE VERIFICATION COMPLETE");print(f"checked_candidates={r['checked_candidates']}");print(f"failed={r['failed']}");print(f"test_passed={str(r['test_passed']).lower()}");print("report=data/probability_engine_verification_report.json");return 0 if r["test_passed"] else 1
if __name__=="__main__":raise SystemExit(main())
