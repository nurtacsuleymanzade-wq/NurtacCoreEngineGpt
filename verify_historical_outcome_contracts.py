"""Verify Layer-8 historical outcome contracts."""
import json
from pathlib import Path
from typing import Any
import historical_outcome_contracts as contracts

ROOT=Path(__file__).resolve().parent; REPORT=ROOT/"data"/"historical_outcome_contract_verification_report.json"
EXPECTED={"future_outcome_observation","event_outcome_observation","setup_outcome_observation","observer_outcome_observation","structure_outcome_observation","volume_profile_outcome_observation","detector_outcome_observation","evidence_outcome_observation"}

def verify()->dict[str,Any]:
 errors=[]; registry=getattr(contracts,"HISTORICAL_OUTCOME_CONTRACTS",None)
 if not isinstance(registry,list): registry=[]; errors.append("registry missing or invalid")
 names=[x.get("contract_name") for x in registry if isinstance(x,dict)]
 if set(names)!=EXPECTED: errors.append("required contracts are incomplete")
 if len(names)!=len(set(names)): errors.append("contract names are not unique")
 for item in registry:
  for field in ("confidence","strength_score","thresholds","probability","edge_score"):
   if item.get(field) is not None: errors.append(f"{item.get('contract_name')}: {field} must be null")
 report={"checked_contracts":len(registry),"passed":len(registry) if not errors else 0,"failed":0 if not errors else len(registry),"errors":errors,"test_passed":not errors}
 REPORT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8"); return report

def main()->int:
 r=verify();print("HISTORICAL OUTCOME CONTRACT VERIFICATION COMPLETE");print(f"checked_contracts={r['checked_contracts']}");print(f"passed={r['passed']}");print(f"failed={r['failed']}");print(f"test_passed={str(r['test_passed']).lower()}");print("report=data/historical_outcome_contract_verification_report.json");return 0 if r["test_passed"] else 1
if __name__=="__main__":raise SystemExit(main())
