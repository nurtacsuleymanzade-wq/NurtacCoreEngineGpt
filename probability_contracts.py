"""Layer-10 probability-candidate contract registry.

The registry defines schemas and calibration dependencies only. It performs no
file I/O and creates no probability values, decisions, orders, or executions.
"""

from copy import deepcopy
from typing import Any


INPUT_SOURCES = [
    "data/calibration_profiles.json",
    "data/setup_candidates.jsonl",
    "data/evidence_packets.jsonl",
    "data/volume_profile_dna.jsonl",
    "data/volume_profile_events.jsonl",
    "data/structure_events.jsonl",
    "data/context_dna.jsonl",
    "data/detector_events.jsonl",
    "data/historical_outcome_observations.jsonl",
]

ALLOWED_TIMEFRAMES = ["1S", "3S", "5S", "15S", "1M", "5M", "15M", "1H"]

OUTPUT_SCHEMA = {
    "layer": "Layer-10",
    "engine": "ProbabilityEngine",
    "record_type": "probability_candidate",
    "probability_id": "string",
    "probability_name": "string",
    "probability_family": "string",
    "symbol": "BTCUSDT",
    "timeframe": "string",
    "window_start_ts": int,
    "window_end_ts": "int|null",
    "source_setup_id": None,
    "source_event_id": None,
    "pattern_signature": None,
    "side": "long|short|neutral|unknown",
    "probability": {
        "long_probability": "null|float",
        "short_probability": "null|float",
        "neutral_probability": "null|float",
        "source": "calibration_profiles",
        "method": "measured_from_historical_outcomes",
        "hardcoded": False,
    },
    "outcome_profile": {
        "sample_count": int,
        "sample_status": "observed_sample|insufficient_data",
        "horizons": {},
    },
    "expected_value_context": {
        "avg_side_adjusted_return": "null|float",
        "avg_max_favorable_return": "null|float",
        "avg_max_adverse_return": "null|float",
        "return_distribution": {},
    },
    "contradiction_context": {
        "has_contradiction": False,
        "contradicting_evidence": [],
    },
    "calibration_refs": [],
    "setup_refs": [],
    "evidence_refs": [],
    "structure_refs": [],
    "volume_profile_refs": [],
    "context_refs": [],
    "decision_readiness": {
        "ready_for_decision_gate": False,
        "reason": "probability_candidate_not_decision",
    },
    "validation": {
        "contract_found": "bool",
        "calibration_profile_found": "bool",
        "no_hardcoded_probability": "bool",
        "invariants_passed": "bool",
        "errors": [],
    },
}

VALIDATION_INVARIANTS = [
    "probability_name is not empty",
    "probability_family is not empty",
    "input_sources is a list",
    "required_inputs is a list",
    "optional_inputs is a list",
    "probability_logic is defined",
    "learned_from is calibration_profiles or historical_outcomes",
    "allowed_timeframes is a list",
    "calibration_dependency is explicit",
    "output_schema contains all required fields",
    "probability values derive only from measured calibration statistics",
    "ready_for_decision_gate remains false",
    "the contract produces no trade decision, order, or execution",
]

FORBIDDEN_BEHAVIOR = [
    "no trade decision",
    "no execution",
    "no order",
    "no entry",
    "no stop loss",
    "no take profit",
    "no leverage",
    "no position sizing",
    "no hardcoded probability",
    "no hardcoded confidence",
    "no hardcoded threshold",
    "no hardcoded strength score",
    "no manual probability",
    "no heuristic probability",
]


def _contract(
    name: str,
    family: str,
    required_inputs: list[str],
    optional_inputs: list[str],
    purpose: str,
    derivation: str,
) -> dict[str, Any]:
    schema = deepcopy(OUTPUT_SCHEMA)
    schema["probability_name"] = name
    schema["probability_family"] = family
    return {
        "probability_name": name,
        "probability_family": family,
        "input_sources": list(INPUT_SOURCES),
        "required_inputs": required_inputs,
        "optional_inputs": optional_inputs,
        "probability_logic": {
            "purpose": purpose,
            "derivation": derivation,
            "allowed_measurements": [
                "favorable_rate",
                "unfavorable_rate",
                "side_adjusted_return_distribution",
                "sample_count",
                "sample_status",
                "historical_outcome_profile",
                "calibration_profile",
            ],
            "decision_semantics": "probability candidate only; not a decision gate",
        },
        "learned_from": ["calibration_profiles", "historical_outcomes"],
        "allowed_timeframes": list(ALLOWED_TIMEFRAMES),
        "calibration_dependency": {
            "required": True,
            "source": "data/calibration_profiles.json",
            "status": "measured_from_outcomes",
            "missing_profile_behavior": "leave probability values null",
        },
        "output_schema": schema,
        "validation_invariants": list(VALIDATION_INVARIANTS),
        "forbidden_behavior": list(FORBIDDEN_BEHAVIOR),
    }


PROBABILITY_CONTRACTS = [
    _contract(
        "setup_probability_candidate",
        "setup_based_probability",
        ["setup_candidate", "calibration_profile"],
        ["pattern_signature", "historical_outcome_profile", "setup_refs"],
        "Attach a measured outcome probability candidate to a setup candidate.",
        "Match setup_name, timeframe, and side or pattern_signature to a measured calibration profile.",
    ),
    _contract(
        "event_probability_candidate",
        "event_based_probability",
        ["source_event", "calibration_profile"],
        ["detector_event", "structure_event", "volume_profile_event", "event_refs"],
        "Attach measured outcome context to Detector, Smart Money, or Volume Profile events.",
        "Match source type, event type, timeframe, and observed side to a measured calibration profile.",
    ),
    _contract(
        "pattern_probability_candidate",
        "pattern_based_probability",
        ["pattern_signature", "calibration_profile"],
        ["evidence_packet", "historical_outcome_profile", "evidence_refs"],
        "Represent measured historical outcomes for a combined evidence pattern.",
        "Use the calibration profile identified by pattern_signature, timeframe, and side.",
    ),
    _contract(
        "timeframe_probability_candidate",
        "timeframe_based_probability",
        ["timeframe", "calibration_profile"],
        ["source_event", "setup_candidate", "timeframe_profile"],
        "Represent measured outcome differences for the same event or setup by timeframe.",
        "Read timeframe-grouped measured rates and return distributions without cross-timeframe scoring.",
    ),
    _contract(
        "side_probability_candidate",
        "side_based_probability",
        ["side", "calibration_profile"],
        ["side_adjusted_outcome_profile", "source_event", "setup_candidate"],
        "Attach measured favorable and unfavorable outcome distributions by observed side.",
        "Map measured favorable_rate and unfavorable_rate according to the calibration profile side.",
    ),
    _contract(
        "contradiction_probability_candidate",
        "contradiction_adjustment_candidate",
        ["evidence_context", "calibration_profile"],
        ["long_supporting_evidence", "short_supporting_evidence", "blocking_evidence"],
        "Annotate simultaneous opposing evidence without modifying a probability value.",
        "Preserve measured probability values and record contradiction metadata only.",
    ),
    _contract(
        "insufficient_data_probability_candidate",
        "insufficient_data",
        ["calibration_profile", "sample_status"],
        ["missing_horizons", "sample_count", "calibration_refs"],
        "Represent a calibration profile whose sample status is insufficient_data.",
        "Leave all probability values null when the measured calibration profile has insufficient_data status.",
    ),
    _contract(
        "probability_context_annotation",
        "probability_metadata",
        ["probability_candidate", "calibration_profile"],
        ["calibration_refs", "evidence_refs", "context_refs", "data_quality_context"],
        "Attach measured-profile references and contextual metadata to a probability candidate.",
        "Copy references and measured context without creating or changing probability values.",
    ),
]


def get_probability_contract(probability_name: str) -> dict[str, Any] | None:
    for contract in PROBABILITY_CONTRACTS:
        if contract["probability_name"] == probability_name:
            return contract
    return None


if __name__ == "__main__":
    for item in PROBABILITY_CONTRACTS:
        print(item["probability_name"])
