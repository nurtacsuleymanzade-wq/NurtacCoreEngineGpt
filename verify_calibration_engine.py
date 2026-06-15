"""Verify Layer-9 measured calibration profiles."""
import json
from pathlib import Path
from typing import Any

ROOT=Path(__file__).resolve().parent;DATA=ROOT/"data";PROFILES=DATA/"calibration_profiles.json";HEALTH=DATA/"calibration_health.json";REPORT=DATA/"calibration_engine_verification_report.json"

def verify()->dict[str,Any]:
 errors=[];profiles=[]
 if not PROFILES.exists():errors.append("calibration_profiles.json is missing")
 else:
  try:payload=json.loads(PROFILES.read_text(encoding="utf-8"))
  except json.JSONDecodeError:payload={};errors.append("calibration_profiles.json is invalid")
  profiles=payload.get("profiles",[])
  if not isinstance(profiles,list):errors.append("profiles must be a list");profiles=[]
 for index,p in enumerate(profiles):
  label=f"profile[{index}]"
  if p.get("calibration_status")!="measured_from_outcomes":errors.append(f"{label}: invalid calibration_status")
  scores=p.get("scores",{})
  for field in ("hardcoded_confidence","hardcoded_probability","hardcoded_strength_score","hardcoded_threshold"):
   if scores.get(field) is not None:errors.append(f"{label}: scores.{field} must be null")
  if not isinstance(p.get("sample_count"),int):errors.append(f"{label}: sample_count must be int")
  if p.get("sample_status") not in {"observed_sample","insufficient_data"}:errors.append(f"{label}: invalid sample_status")
  for horizon,h in p.get("horizons",{}).items():
   count=h.get("sample_count");favorable=h.get("favorable_count");rate=h.get("favorable_rate")
   expected=favorable/count if isinstance(count,int) and count>0 and isinstance(favorable,int) else None
   if expected is None or not isinstance(rate,(int,float)) or abs(rate-expected)>1e-12:errors.append(f"{label}.{horizon}: favorable_rate not derived from counts")
 if not HEALTH.exists():errors.append("calibration_health.json is missing")
 else:
  try:health=json.loads(HEALTH.read_text(encoding="utf-8"))
  except json.JSONDecodeError:health={};errors.append("calibration_health.json is invalid")
  if health.get("registry_validation_passed") is not True:errors.append("registry_validation_passed must be true")
 report={"checked_profiles":len(profiles),"failed":len(errors),"errors":errors,"test_passed":not errors}
 REPORT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8");return report

def main()->int:
 r=verify();print("CALIBRATION ENGINE VERIFICATION COMPLETE");print(f"checked_profiles={r['checked_profiles']}");print(f"failed={r['failed']}");print(f"test_passed={str(r['test_passed']).lower()}");print("report=data/calibration_engine_verification_report.json");return 0 if r["test_passed"] else 1
if __name__=="__main__":raise SystemExit(main())
