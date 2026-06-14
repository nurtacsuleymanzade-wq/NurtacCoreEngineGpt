"""Layer-7 contracts for uncalibrated watch-state observations."""

from copy import deepcopy
from typing import Any


_OUTPUT_SCHEMA = {
    "layer": "Layer-7",
    "engine": "ObserverEngine",
    "record_type": "observer_event",
    "event_id": "string",
    "symbol": "string",
    "timeframe": "string",
    "window_start_ts": int,
    "window_end_ts": "int|null",
    "event_type": "string",
    "watch_side": "long|short|neutral|unknown",
    "watch_status": "watching|waiting_for_trigger|condition_satisfied|invalidated",
    "calibration_status": "uncalibrated",
    "confidence": None,
    "strength_score": None,
    "thresholds": None,
    "reason": {},
    "source_refs": {},
    "supporting_events": [],
    "opposing_events": [],
    "validation": {
        "contract_found": "bool",
        "invariants_passed": "bool",
        "errors": [],
    },
}

_INPUT_SOURCES = [
    "data/evidence_packets.jsonl",
    "data/structure_events.jsonl",
    "data/detector_events.jsonl",
    "data/context_dna.jsonl",
    "data/smart_money_dna.jsonl",
]

_COMMON_INVARIANTS = [
    "calibration_status is uncalibrated",
    "confidence is null",
    "strength_score is null",
    "thresholds is null",
    "decision readiness for setup remains false",
]

_COMMON_FORBIDDEN = [
    "no trade decision",
    "no setup",
    "no confidence",
    "no strength score",
    "no numeric threshold",
    "no long short trade signal",
    "no entry",
    "no stop loss",
    "no take profit",
]


def _contract(
    name: str,
    family: str,
    logic: dict[str, Any],
    required: list[str],
    optional: list[str] | None = None,
    invariants: list[str] | None = None,
    forbidden: list[str] | None = None,
) -> dict[str, Any]:
    output_schema = deepcopy(_OUTPUT_SCHEMA)
    output_schema["event_type"] = name
    return {
        "observer_name": name,
        "observer_family": family,
        "input_sources": list(_INPUT_SOURCES),
        "required_fields": required,
        "optional_fields": optional or [],
        "observation_logic": logic,
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": output_schema,
        "validation_invariants": (invariants or []) + list(_COMMON_INVARIANTS),
        "forbidden_behavior": (forbidden or []) + list(_COMMON_FORBIDDEN),
    }


OBSERVER_CONTRACTS = [
    _contract(
        "long_watch_candidate",
        "directional_watch",
        {
            "candidate_logic": "buy-side detector event, buy-side structure event, or non-empty evidence buy_side_events",
            "interpretation": "watch-only; not a setup or trade signal",
        },
        ["symbol", "timeframe", "window_start_ts", "buy_side_observation"],
        ["window_end_ts", "context_refs", "data_quality"],
    ),
    _contract(
        "short_watch_candidate",
        "directional_watch",
        {
            "candidate_logic": "sell-side detector event, sell-side structure event, or non-empty evidence sell_side_events",
            "interpretation": "watch-only; not a setup or trade signal",
        },
        ["symbol", "timeframe", "window_start_ts", "sell_side_observation"],
        ["window_end_ts", "context_refs", "data_quality"],
    ),
    _contract(
        "neutral_watch_candidate",
        "non_directional_watch",
        {
            "candidate_logic": "buy and sell observations coexist, or only neutral/unknown observations are present",
            "interpretation": "classification only; no directional decision",
        },
        ["symbol", "timeframe", "window_start_ts"],
        ["buy_side_observation", "sell_side_observation", "neutral_observation", "unknown_observation"],
    ),
    _contract(
        "long_condition_satisfied_candidate",
        "watch_continuation",
        {
            "candidate_logic": "an open long watch receives a later buy-side observation",
            "interpretation": "the watched condition continues; this is not setup readiness",
        },
        ["current_watch_side", "previous_watch_event", "buy_side_observation"],
        ["context_refs", "data_quality"],
    ),
    _contract(
        "short_condition_satisfied_candidate",
        "watch_continuation",
        {
            "candidate_logic": "an open short watch receives a later sell-side observation",
            "interpretation": "the watched condition continues; this is not setup readiness",
        },
        ["current_watch_side", "previous_watch_event", "sell_side_observation"],
        ["context_refs", "data_quality"],
    ),
    _contract(
        "invalidation_candidate",
        "watch_invalidation",
        {
            "long_watch_logic": "an open long watch receives a sell-side observation",
            "short_watch_logic": "an open short watch receives a buy-side observation",
            "interpretation": "watch-state invalidation only",
        },
        ["current_watch_side", "opposing_observation"],
        ["context_refs", "data_quality"],
    ),
    _contract(
        "wait_for_trigger_candidate",
        "watch_pending",
        {
            "candidate_logic": "a watch state exists and no continuation condition has yet been observed",
            "interpretation": "pending observation only; no trigger threshold is defined",
        },
        ["current_watch_side", "current_watch_status"],
        ["supporting_events", "context_refs", "data_quality"],
        forbidden=["do not define a numeric trigger"],
    ),
]


def get_observer_contract(observer_name: str) -> dict[str, Any] | None:
    for contract in OBSERVER_CONTRACTS:
        if contract["observer_name"] == observer_name:
            return contract
    return None


def validate_observer_contracts() -> list[str]:
    errors: list[str] = []
    required_fields = {
        "observer_name",
        "observer_family",
        "input_sources",
        "required_fields",
        "optional_fields",
        "observation_logic",
        "calibration_status",
        "confidence",
        "strength_score",
        "thresholds",
        "output_schema",
        "validation_invariants",
        "forbidden_behavior",
    }
    names: set[str] = set()
    for contract in OBSERVER_CONTRACTS:
        name = str(contract.get("observer_name"))
        if name in names:
            errors.append(f"duplicate observer_name: {name}")
        names.add(name)
        missing = sorted(required_fields - contract.keys())
        if missing:
            errors.append(f"{name}: missing fields: {', '.join(missing)}")
        if contract.get("calibration_status") != "uncalibrated":
            errors.append(f"{name}: calibration_status must be uncalibrated")
        if any(contract.get(field) is not None for field in ("confidence", "strength_score", "thresholds")):
            errors.append(f"{name}: score and threshold fields must be null")
    return errors


if __name__ == "__main__":
    for item in OBSERVER_CONTRACTS:
        print(item["observer_name"])
