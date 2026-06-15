"""Verify Layer-9 calibration contracts."""
import json
from pathlib import Path
from typing import Any
import calibration_contracts

ROOT=Path(__file__).resolve().parent;REPORT=ROOT/"data"/"calibration_contract_verification_report.json"
EXPECTED={"event_calibration_profile","setup_calibration_profile","structure_calibration_profile",
"volume_profile_calibration_profile","evidence_pattern_calibration_profile","timeframe_calibration_profile",
"side_adjusted_outcome_profile","insufficient_data_profile"}
REQUIRED={"contract_name","contract_family","input_sources","required_fields","optional_fields","calibration_formula",
"learned_from","calibration_status","hardcoded_probability","hardcoded_confidence","hardcoded_threshold",
"output_schema","validation_invariants","forbidden_behavior"}
OUTPUT={"profile_id","profile_type","group_key","symbol","timeframe","source_type","event_type",
"setup_name","pattern_signature","side","sample_count","sample_status","missing_horizons","horizons",
"calibration_status","scores","validation"}

def verify()->dict[str,Any]:
 errors=[];items=getattr(calibration_contracts,"CALIBRATION_CONTRACTS",None)
 if not isinstance(items,list):items=[];errors.append("CALIBRATION_CONTRACTS missing or invalid")
 names=[x.get("contract_name") for x in items if isinstance(x,dict)]
 if len(items)!=8:errors.append(f"expected 8 contracts, found {len(items)}")
 if len(names)!=len(set(names)):errors.append("contract_name values are not unique")
 if EXPECTED-set(names):errors.append("required contracts are incomplete")
 for item in items:
  name=str(item.get("contract_name"));missing=REQUIRED-item.keys()
  if missing:errors.append(f"{name}: missing fields")
  for field in ("hardcoded_probability","hardcoded_confidence","hardcoded_threshold"):
   if item.get(field) is not None:errors.append(f"{name}: {field} must be null")
  schema=item.get("output_schema",{});
  if OUTPUT-schema.keys():errors.append(f"{name}: output_schema incomplete")
  text=" ".join(map(str,item.get("forbidden_behavior",[]))).lower()
  for phrase in ("no trade decision","no setup","no entry","no hardcoded probability","no hardcoded confidence","no hardcoded threshold"):
   if phrase not in text:errors.append(f"{name}: forbidden behavior missing {phrase}")
 report={"checked_contracts":len(items),"passed":len(items) if not errors else 0,"failed":0 if not errors else len(items),"errors":errors,"test_passed":not errors}
 REPORT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8");return report

def main()->int:
 r=verify();print("CALIBRATION CONTRACT VERIFICATION COMPLETE");print(f"checked_contracts={r['checked_contracts']}");print(f"passed={r['passed']}");print(f"failed={r['failed']}");print(f"test_passed={str(r['test_passed']).lower()}");print("report=data/calibration_contract_verification_report.json");return 0 if r["test_passed"] else 1
if __name__=="__main__":raise SystemExit(main())
