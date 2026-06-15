"""Layer-6C uncalibrated volume and market-profile contract registry."""

from copy import deepcopy
from typing import Any


INPUT_SOURCES = [
    "data/one_second_combined_dna.jsonl", "data/rolling_3s_dna.jsonl",
    "data/rolling_5s_dna.jsonl", "data/rolling_15s_dna.jsonl",
    "data/aligned_1m_candle_dna.jsonl", "data/context_dna.jsonl",
    "data/detector_events.jsonl", "data/structure_events.jsonl",
    "data/evidence_packets.jsonl", "data/smart_money_dna.jsonl",
    "data/data_quality.jsonl",
]

SNAPSHOT_SCHEMA = {
    "layer": "Layer-6C", "engine": "VolumeProfileEngine",
    "record_type": "volume_profile_snapshot", "symbol": "string",
    "timeframe": "string", "window_start_ts": int, "window_end_ts": "int|null",
    "profile": {"poc": None, "value_area": {}, "hvn_levels": [], "lvn_levels": [],
                "balance_zone": None, "market_state": "unknown", "profile_shape": "unknown",
                "volume_by_price": {}, "time_by_price": {}},
    "location": {}, "auction": {}, "memory_refs": [],
    "calibration_status": "uncalibrated",
    "scores": {"confidence": None, "strength_score": None,
               "profile_score": None, "threshold": None},
    "data_quality": {}, "validation": {"input_valid": "bool", "errors": []},
}

EVENT_SCHEMA = {
    "layer": "Layer-6C", "engine": "VolumeProfileEngine",
    "record_type": "volume_profile_event", "event_id": "string",
    "symbol": "string", "timeframe": "string", "window_start_ts": int,
    "window_end_ts": "int|null", "event_type": "string",
    "side": "long|short|neutral|unknown", "direction": "up|down|flat|unknown",
    "level": None, "zone": None, "reason": {}, "source_refs": [],
    "calibration_status": "uncalibrated", "confidence": None,
    "strength_score": None, "thresholds": None,
    "validation": {"contract_found": "bool", "invariants_passed": "bool", "errors": []},
}

FORBIDDEN_BEHAVIOR = [
    "no trade decision", "no setup", "no long short signal", "no entry",
    "no stop loss", "no take profit", "no leverage", "no position sizing",
    "no confidence", "no strength score", "no profile score", "no edge score",
    "no probability score", "no threshold",
]

INVARIANTS = [
    "concept_name is not empty", "concept_family is not empty",
    "input_sources is a list", "required_fields is a list",
    "optional_fields is a list", "measurement_formula is defined",
    "calibration_status is uncalibrated", "confidence is null",
    "strength_score is null", "thresholds is null",
    "all score and threshold fields remain null",
    "measurements produce location and auction context only",
]


def _contract(name: str, family: str, formula: str, event: bool = True) -> dict[str, Any]:
    schema = deepcopy(EVENT_SCHEMA if event else SNAPSHOT_SCHEMA)
    if event:
        schema["event_type"] = name
    return {
        "concept_name": name, "concept_family": family,
        "input_sources": list(INPUT_SOURCES),
        "required_fields": ["symbol", "timeframe", "window_start_ts"],
        "optional_fields": ["window_end_ts", "close", "total_volume", "footprint_levels", "data_quality"],
        "measurement_formula": formula, "calibration_status": "uncalibrated",
        "confidence": None, "strength_score": None, "thresholds": None,
        "output_schema": schema, "validation_invariants": list(INVARIANTS),
        "forbidden_behavior": list(FORBIDDEN_BEHAVIOR),
    }


VOLUME_PROFILE_CONTRACTS = [
    _contract("volume_profile_snapshot", "profile", "Aggregate observed volume and close-time by exact price string.", False),
    _contract("poc_level_candidate", "profile_level", "Select the greatest accumulated-volume bin; ties use latest touch."),
    _contract("value_area_candidate", "profile_zone", "Use POC and adjacent observed bins sharing the POC volume."),
    _contract("hvn_candidate", "profile_level", "Select strict local maxima against adjacent observed price bins."),
    _contract("lvn_candidate", "profile_level", "Select strict local minima against adjacent observed price bins."),
    _contract("balance_zone_candidate", "auction_zone", "Describe visited observed price bins around the current POC."),
    _contract("acceptance_zone_candidate", "auction_state", "Current and previous closes occur inside the same value or balance zone."),
    _contract("rejection_zone_candidate", "auction_state", "A close outside value is followed by a close back inside value."),
    _contract("volume_memory_zone_candidate", "memory", "Persist deduplicated POC, node, balance, value, and failed-auction zones."),
    _contract("poc_shift_candidate", "profile_change", "Current POC differs from the previous POC."),
    _contract("value_area_touch_candidate", "profile_touch", "Current candle range intersects the current value area."),
    _contract("hvn_touch_candidate", "profile_touch", "Current candle range contains an HVN price."),
    _contract("lvn_touch_candidate", "profile_touch", "Current candle range contains an LVN price."),
    _contract("p_shape_profile_candidate", "profile_shape", "More observed bins lie above POC than below POC."),
    _contract("b_shape_profile_candidate", "profile_shape", "More observed bins lie below POC than above POC."),
    _contract("d_shape_profile_candidate", "profile_shape", "Observed-bin counts balance around one central POC."),
    _contract("b_distribution_profile_candidate", "profile_shape", "At least two separated strict local maxima are present."),
    _contract("trend_profile_candidate", "profile_shape", "POC shifts toward newly observed bins while prior value is not retested."),
    _contract("failed_auction_candidate", "auction_failure", "Price closes outside value and then returns inside value."),
    _contract("failed_action_return_to_value_candidate", "auction_failure", "An escape attempt returns to prior POC, value, or balance memory."),
]


def get_volume_profile_contract(concept_name: str) -> dict[str, Any] | None:
    for contract in VOLUME_PROFILE_CONTRACTS:
        if contract["concept_name"] == concept_name:
            return contract
    return None


def validate_volume_profile_contracts() -> list[str]:
    required = {"concept_name", "concept_family", "input_sources", "required_fields",
                "optional_fields", "measurement_formula", "calibration_status", "confidence",
                "strength_score", "thresholds", "output_schema", "validation_invariants",
                "forbidden_behavior"}
    errors: list[str] = []
    names: set[str] = set()
    for contract in VOLUME_PROFILE_CONTRACTS:
        name = str(contract.get("concept_name") or "")
        if not name: errors.append("empty concept_name")
        if name in names: errors.append(f"duplicate concept_name: {name}")
        names.add(name)
        missing = sorted(required - contract.keys())
        if missing: errors.append(f"{name}: missing fields: {', '.join(missing)}")
        if contract.get("calibration_status") != "uncalibrated": errors.append(f"{name}: invalid calibration_status")
        if any(contract.get(field) is not None for field in ("confidence", "strength_score", "thresholds")):
            errors.append(f"{name}: score or threshold fields must be null")
    return errors


if __name__ == "__main__":
    for item in VOLUME_PROFILE_CONTRACTS:
        print(item["concept_name"])
