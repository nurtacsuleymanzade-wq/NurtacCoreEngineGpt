"""Verify Layer-7 setup engine outputs."""
import json
from collections import deque
from pathlib import Path
from typing import Any
from setup_contracts import get_setup_contract

ROOT=Path(__file__).resolve().parent; DATA=ROOT/"data"; CANDIDATES=DATA/"setup_candidates.jsonl"; HEALTH=DATA/"setup_health.json"; REPORT=DATA/"setup_engine_verification_report.json"
PROHIBITED={"entry","entry_price","stop_loss","sl","take_profit","tp","leverage","position_size","position_sizing"}

def verify()->dict[str,Any]:
 errors=[]; rows=deque(maxlen=100)
 if not CANDIDATES.exists(): errors.append("setup_candidates.jsonl is missing")
 else:
  with CANDIDATES.open("r",encoding="utf-8",errors="replace") as handle:
   for number,line in enumerate(handle,1):
    try: row=json.loads(line)
    except json.JSONDecodeError: continue
    if isinstance(row,dict) and row.get("layer")=="Layer-7": rows.append((number,row))
 if not rows: errors.append("no readable Layer-7 setup candidates")
 for number,row in rows:
  prefix=f"line {number}"
  if get_setup_contract(str(row.get("setup_name"))) is None: errors.append(f"{prefix}: setup_name missing from registry")
  if row.get("setup_status")!="candidate_not_trade_signal": errors.append(f"{prefix}: invalid setup_status")
  scores=row.get("scores",{})
  for field in ("confidence","strength_score","setup_score","edge_score","probability_score","threshold"):
   if scores.get(field) is not None: errors.append(f"{prefix}: scores.{field} must be null")
  if row.get("execution_readiness",{}).get("ready_for_execution_plan") is not False: errors.append(f"{prefix}: execution readiness must be false")
  if row.get("risk_readiness",{}).get("ready_for_position_sizing") is not False: errors.append(f"{prefix}: risk readiness must be false")
  bad=PROHIBITED & row.keys()
  if bad: errors.append(f"{prefix}: prohibited fields: {', '.join(sorted(bad))}")
  validation=row.get("validation",{})
  if validation.get("contract_found") is not True: errors.append(f"{prefix}: contract_found must be true")
  if validation.get("invariants_passed") is not True: errors.append(f"{prefix}: invariants_passed must be true")
 if not HEALTH.exists(): errors.append("setup_health.json is missing")
 else:
  try: health=json.loads(HEALTH.read_text(encoding="utf-8"))
  except json.JSONDecodeError: health={}; errors.append("setup_health.json invalid")
  if health.get("registry_validation_passed") is not True: errors.append("registry_validation_passed must be true")
 report={"checked_setups":len(rows),"failed":len(errors),"errors":errors,"test_passed":not errors}
 REPORT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8"); return report

def main()->int:
 r=verify(); print("SETUP ENGINE VERIFICATION COMPLETE"); print(f"checked_setups={r['checked_setups']}"); print(f"failed={r['failed']}"); print(f"test_passed={str(r['test_passed']).lower()}"); print("report=data/setup_engine_verification_report.json"); return 0 if r["test_passed"] else 1
if __name__=="__main__": raise SystemExit(main())
