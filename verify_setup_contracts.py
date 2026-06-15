"""Verify Layer-7 setup contracts."""
import json
from pathlib import Path
from typing import Any
import setup_contracts

ROOT=Path(__file__).resolve().parent; REPORT=ROOT/"data"/"setup_contract_verification_report.json"
EXPECTED={"failed_auction_reversal_candidate","failed_action_return_to_value_candidate","long_trap_reversal_candidate","short_trap_reversal_candidate","initiative_breakout_candidate","initiative_continuation_candidate","responsive_rotation_candidate","absorption_reversal_candidate","poc_reclaim_candidate","value_area_reclaim_candidate","lvn_rejection_candidate","hvn_acceptance_candidate","acceptance_continuation_candidate","trend_pullback_continuation_candidate","balance_breakout_candidate","balance_return_candidate","liquidity_sweep_reversal_candidate","liquidity_sweep_continuation_candidate","delta_divergence_reversal_candidate","auction_completion_candidate"}
REQUIRED={"setup_name","setup_family","input_sources","required_concepts","optional_concepts","setup_logic","allowed_timeframes","calibration_status","confidence","strength_score","thresholds","output_schema","validation_invariants","forbidden_behavior"}
OUTPUT={"layer","engine","record_type","setup_id","setup_name","setup_family","symbol","timeframe","window_start_ts","window_end_ts","side","setup_status","matched_concepts","missing_concepts","supporting_evidence","blocking_evidence","detector_refs","evidence_refs","structure_refs","volume_profile_refs","context_refs","historical_outcome_refs","location_context","auction_context","calibration_status","scores","execution_readiness","risk_readiness","validation"}
FORBIDDEN=("no trade decision","no setup execution","no entry","no stop loss","no take profit","no leverage","no position size","no confidence","no strength score","no threshold")

def verify()->dict[str,Any]:
 errors=[]; contracts=getattr(setup_contracts,"SETUP_CONTRACTS",None)
 if not isinstance(contracts,list): contracts=[]; errors.append("SETUP_CONTRACTS missing or invalid")
 names=[x.get("setup_name") for x in contracts if isinstance(x,dict)]
 if len(contracts)!=20: errors.append(f"expected 20 contracts, found {len(contracts)}")
 if len(names)!=len(set(names)): errors.append("setup_name values are not unique")
 missing=EXPECTED-set(names)
 if missing: errors.append("missing contracts: "+", ".join(sorted(missing)))
 for item in contracts:
  if not isinstance(item,dict): errors.append("contract is not dictionary"); continue
  name=str(item.get("setup_name")); absent=REQUIRED-item.keys()
  if absent: errors.append(f"{name}: missing fields: {', '.join(sorted(absent))}")
  if item.get("calibration_status")!="uncalibrated": errors.append(f"{name}: invalid calibration_status")
  for field in ("confidence","strength_score","thresholds"):
   if item.get(field) is not None: errors.append(f"{name}: {field} must be null")
  schema=item.get("output_schema",{}); absent_output=OUTPUT-schema.keys()
  if absent_output: errors.append(f"{name}: output_schema missing: {', '.join(sorted(absent_output))}")
  text=" ".join(map(str,item.get("forbidden_behavior",[]))).lower()
  for phrase in FORBIDDEN:
   if phrase not in text: errors.append(f"{name}: forbidden_behavior missing {phrase}")
 report={"checked_contracts":len(contracts),"passed":len(contracts) if not errors else 0,"failed":0 if not errors else len(contracts),"errors":errors,"test_passed":not errors}
 REPORT.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8"); return report

def main()->int:
 r=verify(); print("SETUP CONTRACT VERIFICATION COMPLETE"); print(f"checked_contracts={r['checked_contracts']}"); print(f"passed={r['passed']}"); print(f"failed={r['failed']}"); print(f"test_passed={str(r['test_passed']).lower()}"); print("report=data/setup_contract_verification_report.json"); return 0 if r["test_passed"] else 1
if __name__=="__main__": raise SystemExit(main())
