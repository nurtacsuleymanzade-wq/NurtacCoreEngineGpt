"""Layer-6B historical outcome observation contracts."""

from copy import deepcopy
from typing import Any


FORWARD_HORIZONS = {
    "30s": 30000,
    "60s": 60000,
    "180s": 180000,
    "300s": 300000,
    "900s": 900000,
    "3600s": 3600000,
}

_OUTPUT_SCHEMA = {
    "layer": "Layer-6B",
    "engine": "HistoricalOutcomeEngine",
    "record_type": "historical_outcome_observation",
    "calibration_status": "observed_not_scored",
    "observation_id": "string",
    "event_id": "string",
    "pattern_signature": "string",
    "pattern_key": "string",
    "pattern_components": [],
    "source": "detector|evidence|smart_money|observer|composite",
    "symbol": "string",
    "timeframe": "string",
    "event_type": "string",
    "side": "string",
    "direction": "string",
    "event_window_start_ts": int,
    "event_window_end_ts": "int|null",
    "reference": {"price": float, "price_ts": int},
    "outcomes": {},
    "source_event": {},
    "data_quality": {},
    "scores": {
        "confidence": None,
        "strength_score": None,
        "edge_score": None,
        "probability_score": None,
        "threshold": None,
        "decision_score": None,
    },
    "validation": {
        "reference_price_valid": True,
        "all_horizons_measured": True,
        "future_leakage_detected": False,
        "errors": [],
    },
}

_INPUTS = [
    "data/one_second_combined_dna.jsonl",
    "data/detector_events.jsonl",
    "data/evidence_packets.jsonl",
    "data/structure_events.jsonl",
    "data/observer_events.jsonl",
    "data/context_dna.jsonl",
    "data/smart_money_dna.jsonl",
    "data/data_quality.jsonl",
]

_FORBIDDEN = [
    "no trade decision",
    "no setup",
    "no confidence",
    "no strength score",
    "no edge score",
    "no probability score",
    "no numeric threshold",
    "no long short signal",
    "no entry stop loss or take profit",
    "no future leakage",
]


def _contract(name: str, family: str, formula: dict[str, Any]) -> dict[str, Any]:
    schema = deepcopy(_OUTPUT_SCHEMA)
    if name == "calibration_profile_summary" or name == "insufficient_sample_profile":
        schema["record_type"] = "calibration_profile_summary"
    return {
        "contract_name": name,
        "contract_family": family,
        "input_sources": list(_INPUTS),
        "required_fields": ["symbol", "timeframe", "window_start_ts", "event_type"],
        "optional_fields": ["window_end_ts", "side", "direction", "source_refs", "data_quality"],
        "observation_formula": formula,
        "forward_horizons": dict(FORWARD_HORIZONS),
        "calibration_status": "observed_not_scored",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": schema,
        "validation_invariants": [
            "reference price timestamp is not after event timestamp",
            "future price timestamp is not before target timestamp",
            "all score fields remain null",
            "forward horizons are measurement horizons and not trade thresholds",
        ],
        "forbidden_behavior": list(_FORBIDDEN),
    }


CALIBRATION_CONTRACTS = [
    _contract("detector_event_outcome_observation", "event_outcome", {"source": "detector", "measurement": "forward returns by fixed horizon"}),
    _contract("evidence_packet_outcome_observation", "packet_outcome", {"source": "evidence", "measurement": "forward returns for grouped evidence"}),
    _contract("structure_event_outcome_observation", "structure_outcome", {"source": "smart_money", "measurement": "forward returns for structure candidate"}),
    _contract("observer_event_outcome_observation", "watch_outcome", {"source": "observer", "measurement": "forward returns for watch-state candidate"}),
    _contract("composite_pattern_outcome_observation", "composite_outcome", {"components": "alphabetically sorted event types at identical symbol/timeframe/window_start_ts"}),
    _contract("calibration_profile_summary", "profile_summary", {"grouping": "symbol,timeframe,source,event_type,side,direction,pattern_signature"}),
    _contract("insufficient_sample_profile", "profile_quality", {"sample_status": "insufficient_data when sample_count < 30; reporting label only"}),
    _contract("data_quality_outcome_annotation", "quality_annotation", {"annotation": "preserve source data quality state without scoring"}),
]


def get_calibration_contract(contract_name: str) -> dict[str, Any] | None:
    for contract in CALIBRATION_CONTRACTS:
        if contract["contract_name"] == contract_name:
            return contract
    return None


def validate_calibration_contracts() -> list[str]:
    required = {"contract_name", "contract_family", "input_sources", "required_fields", "optional_fields", "observation_formula", "forward_horizons", "calibration_status", "confidence", "strength_score", "thresholds", "output_schema", "validation_invariants", "forbidden_behavior"}
    errors: list[str] = []
    names: set[str] = set()
    for contract in CALIBRATION_CONTRACTS:
        name = str(contract.get("contract_name"))
        if name in names:
            errors.append(f"duplicate contract_name: {name}")
        names.add(name)
        missing = sorted(required - contract.keys())
        if missing:
            errors.append(f"{name}: missing fields: {', '.join(missing)}")
        if contract.get("forward_horizons") != FORWARD_HORIZONS:
            errors.append(f"{name}: invalid forward horizons")
        if contract.get("calibration_status") != "observed_not_scored":
            errors.append(f"{name}: invalid calibration status")
        if any(contract.get(field) is not None for field in ("confidence", "strength_score", "thresholds")):
            errors.append(f"{name}: score or thresholds must be null")
    return errors


if __name__ == "__main__":
    for item in CALIBRATION_CONTRACTS:
        print(item["contract_name"])
