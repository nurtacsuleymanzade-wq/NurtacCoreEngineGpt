"""Verify Layer-8 historical outcome engine outputs."""
import json
from collections import deque
from pathlib import Path
from typing import Any

ROOT=Path(__file__).resolve().parent;DATA=ROOT/"data";OUT=DATA/"historical_outcome_observations.jsonl";HEALTH=DATA/"historical_outcome_health.json";ERRORS=DATA/"historical_outcome_errors.jsonl";REPORT=DATA/"historical_outcome_engine_verification_report.json"

def verify()->dict[str,Any]:
 errors=[];rows=deque(maxlen=100)
 for path in (OUT,HEALTH,ERRORS):
  if not path.exists(): errors.append(f"{path.name} is missing")
 if OUT.exists():
  with OUT.open("r",encoding="utf-8",errors="replace") as handle:
   for number,line in enumerate(handle,1):
    try:row=json.loads(line)
    except json.JSONDecodeError:continue
    if isinstance(row,dict) and row.get("record_type")=="historical_outcome":rows.append((number,row))
 if not rows: errors.append("no readable historical outcome observations")
 for number,row in rows:
  prefix=f"line {number}"
  if row.get("calibration_status")!="uncalibrated":errors.append(f"{prefix}: invalid calibration_status")
  for field in ("confidence","strength_score","thresholds","probability","edge_score"):
   if row.get(field) is not None:errors.append(f"{prefix}: {field} must be null")
  for forbidden in ("success","failure","won","lost","trade_decision","entry","stop_loss","take_profit","leverage","position_size"):
   if forbidden in row:errors.append(f"{prefix}: prohibited field {forbidden}")
 if HEALTH.exists():
  try:health=json.loads(HEALTH.read_text(encoding="utf-8"))
  except json.JSONDecodeError:health={};errors.append("health is invalid JSON")
  if health.get("registry_validation_passed") is not True:errors.append("registry_validation_passed must be true")
 report={"checked_observations":len(rows),"failed":len(errors),"errors":errors,"test_passed":not errors}
 REPORT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8");return report

def main()->int:
 r=verify();print("HISTORICAL OUTCOME ENGINE VERIFICATION COMPLETE");print(f"checked_observations={r['checked_observations']}");print(f"failed={r['failed']}");print(f"test_passed={str(r['test_passed']).lower()}");print("report=data/historical_outcome_engine_verification_report.json");return 0 if r["test_passed"] else 1
if __name__=="__main__":raise SystemExit(main())
