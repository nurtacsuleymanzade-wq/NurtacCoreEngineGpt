"""Layer-9 measured historical-outcome calibration contract registry."""

from copy import deepcopy
from typing import Any

INPUT_SOURCES = ["data/historical_outcome_observations.jsonl", "data/setup_candidates.jsonl",
    "data/evidence_packets.jsonl", "data/detector_events.jsonl", "data/structure_events.jsonl",
    "data/volume_profile_events.jsonl", "data/context_dna.jsonl"]

OUTPUT_SCHEMA = {"profile_id":"string","profile_type":"event|setup|structure|volume_profile|evidence_pattern|timeframe|side_adjusted",
    "group_key":"string","symbol":"string|null","timeframe":"string","source_type":"string",
    "event_type":"string","setup_name":None,"pattern_signature":None,
    "side":"buy|sell|long|short|neutral|unknown","sample_count":int,
    "sample_status":"observed_sample|insufficient_data","missing_horizons":[],"horizons":{},
    "calibration_status":"measured_from_outcomes",
    "scores":{"hardcoded_confidence":None,"hardcoded_probability":None,
        "hardcoded_strength_score":None,"hardcoded_threshold":None},
    "validation":{"source_observations_found":"bool","no_hardcoded_values":True,"errors":[]}}

FORBIDDEN = ["no trade decision","no setup","no entry","no stop loss","no take profit",
    "no leverage","no position sizing","no hardcoded probability","no hardcoded confidence",
    "no hardcoded strength score","no hardcoded threshold","no decision gate"]


def _contract(name: str, family: str, formula: str, learned_from: str) -> dict[str, Any]:
    return {"contract_name":name,"contract_family":family,"input_sources":list(INPUT_SOURCES),
        "required_fields":["source_type","source_event_id","symbol","timeframe","observation_window",
            "start_price","highest_price","lowest_price","close_price","price_path"],
        "optional_fields":["event_type","setup_name","pattern_signature","side","direction","location_context"],
        "calibration_formula":formula,"learned_from":learned_from,
        "calibration_status":"measured_from_outcomes","hardcoded_probability":None,
        "hardcoded_confidence":None,"hardcoded_threshold":None,"output_schema":deepcopy(OUTPUT_SCHEMA),
        "validation_invariants":["all metrics derive from historical outcome rows",
            "sample status is a data availability label only","hardcoded score and threshold fields remain null"],
        "forbidden_behavior":list(FORBIDDEN)}


CALIBRATION_CONTRACTS = [
 _contract("event_calibration_profile","event","Group measured outcomes by source type, event type, timeframe, and side.","historical_outcome_observations"),
 _contract("setup_calibration_profile","setup","Group measured setup outcomes by setup name, timeframe, and side.","historical outcomes joined to setup candidates"),
 _contract("structure_calibration_profile","structure","Group measured structure outcomes by event type, direction, and timeframe.","historical outcomes joined to structure events"),
 _contract("volume_profile_calibration_profile","volume_profile","Group measured volume-profile outcomes by event type and location context.","historical outcomes joined to volume profile events and context"),
 _contract("evidence_pattern_calibration_profile","evidence_pattern","Group measured evidence outcomes by deterministic evidence pattern signature.","historical outcomes joined to evidence packets"),
 _contract("timeframe_calibration_profile","timeframe","Group measured outcomes by source type and timeframe.","historical_outcome_observations"),
 _contract("side_adjusted_outcome_profile","side_adjusted","Measure returns after deterministic side sign adjustment.","historical outcomes and observed source side"),
 _contract("insufficient_data_profile","sample_status","Represent groups with no measured samples without decision semantics.","historical_outcome_observations"),
]


def get_calibration_contract(contract_name: str) -> dict[str, Any] | None:
    return next((item for item in CALIBRATION_CONTRACTS if item["contract_name"] == contract_name), None)


def validate_calibration_contracts() -> list[str]:
    required={"contract_name","contract_family","input_sources","required_fields","optional_fields",
        "calibration_formula","learned_from","calibration_status","hardcoded_probability",
        "hardcoded_confidence","hardcoded_threshold","output_schema","validation_invariants","forbidden_behavior"}
    errors=[];names=set()
    for item in CALIBRATION_CONTRACTS:
        name=str(item.get("contract_name") or "")
        if not name:errors.append("empty contract_name")
        if name in names:errors.append(f"duplicate contract_name: {name}")
        names.add(name);missing=required-item.keys()
        if missing:errors.append(f"{name}: missing fields: {', '.join(sorted(missing))}")
        for field in ("hardcoded_probability","hardcoded_confidence","hardcoded_threshold"):
            if item.get(field) is not None:errors.append(f"{name}: {field} must be null")
    return errors


if __name__=="__main__":
    for item in CALIBRATION_CONTRACTS:print(item["contract_name"])
