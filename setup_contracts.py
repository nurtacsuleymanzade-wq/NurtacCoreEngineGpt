"""Layer-8 uncalibrated setup-candidate contract registry.

The registry defines candidate families only. It performs no file I/O and
does not produce execution, risk, score, threshold, or trade decisions.
"""

from copy import deepcopy
from typing import Any


INPUT_SOURCES = [
    "data/evidence_packets.jsonl",
    "data/observer_events.jsonl",
    "data/observer_states.jsonl",
    "data/structure_events.jsonl",
    "data/smart_money_dna.jsonl",
    "data/context_dna.jsonl",
    "data/historical_outcome_observations.jsonl",
    "data/calibration_profiles.json",
]

ALLOWED_TIMEFRAMES = ["1S", "3S", "5S", "15S", "1M", "5M", "15M", "1H"]

_OUTPUT_SCHEMA = {
    "layer": "Layer-8",
    "engine": "SetupEngine",
    "record_type": "setup_candidate",
    "setup_id": "string",
    "setup_name": "string",
    "setup_family": "string",
    "symbol": "string",
    "timeframe": "string",
    "window_start_ts": int,
    "window_end_ts": "int|null",
    "side": "long|short|neutral|unknown",
    "setup_status": "candidate_not_trade_signal",
    "supporting_evidence": [],
    "blocking_evidence": [],
    "observer_refs": [],
    "structure_refs": [],
    "context_refs": [],
    "historical_outcome_refs": [],
    "calibration_status": "uncalibrated",
    "scores": {
        "confidence": None,
        "strength_score": None,
        "setup_score": None,
        "edge_score": None,
        "probability_score": None,
        "threshold": None,
    },
    "execution_readiness": {
        "ready_for_execution_plan": False,
        "reason": "setup_uncalibrated",
    },
    "risk_readiness": {
        "ready_for_position_sizing": False,
        "reason": "no_execution_plan",
    },
    "validation": {
        "contract_found": "bool",
        "invariants_passed": "bool",
        "errors": [],
    },
}

_COMMON_INVARIANTS = [
    "setup_name is not empty",
    "setup_family is not empty",
    "input_sources is a list",
    "required_inputs is a list",
    "optional_inputs is a list",
    "allowed_timeframes is a list",
    "calibration_status is uncalibrated",
    "confidence is null",
    "strength_score is null",
    "thresholds is null",
    "all output score fields are null",
    "ready_for_execution_plan is false",
    "ready_for_position_sizing is false",
    "the contract produces no execution or risk parameters",
    "the contract produces no trade decision",
]

_COMMON_FORBIDDEN = [
    "no trade decision",
    "no execution",
    "no entry",
    "no stop loss",
    "no take profit",
    "no leverage",
    "no position sizing",
    "no confidence",
    "no strength score",
    "no setup score",
    "no edge score",
    "no probability score",
    "no numeric threshold",
]


def _contract(
    name: str,
    family: str,
    required_inputs: list[str],
    optional_inputs: list[str],
    long_components: list[str],
    short_components: list[str],
    purpose: str,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    schema = deepcopy(_OUTPUT_SCHEMA)
    schema["setup_name"] = name
    schema["setup_family"] = family
    return {
        "setup_name": name,
        "setup_family": family,
        "input_sources": list(INPUT_SOURCES),
        "required_inputs": required_inputs,
        "optional_inputs": optional_inputs,
        "setup_logic": {
            "purpose": purpose,
            "long_conceptual_components": long_components,
            "short_conceptual_components": short_components,
            "interpretation": "uncalibrated setup candidate only; not a trade signal",
            "notes": notes or [],
        },
        "allowed_timeframes": list(ALLOWED_TIMEFRAMES),
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": schema,
        "validation_invariants": list(_COMMON_INVARIANTS),
        "forbidden_behavior": list(_COMMON_FORBIDDEN),
    }


SETUP_CONTRACTS = [
    _contract(
        "initiative_continuation_candidate",
        "continuation",
        ["initiative_flow_candidate", "momentum_candidate", "delta_imbalance_candidate", "observer_condition_satisfied_candidate"],
        ["BOS_candidate", "HH_candidate", "HL_candidate", "LH_candidate", "LL_candidate", "historical_outcome_refs"],
        ["initiative_buyer_candidate", "momentum_candidate", "delta_imbalance_candidate:buy", "long_condition_satisfied_candidate", "BOS_candidate:up|HH_candidate|HL_candidate"],
        ["initiative_seller_candidate", "momentum_candidate", "delta_imbalance_candidate:sell", "short_condition_satisfied_candidate", "BOS_candidate:down|LH_candidate|LL_candidate"],
        "Describe directional aggression and aligned price movement continuing on the same side.",
    ),
    _contract(
        "absorption_reversal_candidate",
        "reversal",
        ["absorption_candidate", "responsive_flow_candidate", "trapped_trader_candidate", "observer_watch_candidate"],
        ["fractal_high_candidate", "fractal_low_candidate", "HL_candidate", "LH_candidate", "historical_outcome_refs"],
        ["absorption_candidate:buy", "responsive_buyer_candidate", "trapped_seller_candidate", "fractal_low_candidate|HL_candidate", "long_watch_candidate|long_condition_satisfied_candidate"],
        ["absorption_candidate:sell", "responsive_seller_candidate", "trapped_buyer_candidate", "fractal_high_candidate|LH_candidate", "short_watch_candidate|short_condition_satisfied_candidate"],
        "Describe failed aggressive continuation with an opposite-side response candidate.",
    ),
    _contract(
        "sweep_reclaim_candidate",
        "liquidity_reclaim",
        ["sweep_candidate", "local_swing_or_equal_level_candidate", "responsive_flow_candidate", "observer_watch_candidate"],
        ["trapped_trader_candidate", "historical_outcome_refs"],
        ["sweep_candidate", "equal_low_candidate|fractal_low_candidate", "responsive_buyer_candidate", "trapped_seller_candidate", "long_watch_candidate"],
        ["sweep_candidate", "equal_high_candidate|fractal_high_candidate", "responsive_seller_candidate", "trapped_buyer_candidate", "short_watch_candidate"],
        "Describe a liquidity movement followed by a reclaim-side observation.",
        ["reclaim price computation belongs to a later calibrated engine"],
    ),
    _contract(
        "breakout_continuation_candidate",
        "breakout",
        ["BOS_candidate", "MSB_candidate", "initiative_flow_candidate", "momentum_candidate", "observer_condition_satisfied_candidate"],
        ["historical_outcome_refs", "context_refs"],
        ["BOS_candidate:up", "MSB_candidate:up", "initiative_buyer_candidate", "momentum_candidate", "long_condition_satisfied_candidate"],
        ["BOS_candidate:down", "MSB_candidate:down", "initiative_seller_candidate", "momentum_candidate", "short_condition_satisfied_candidate"],
        "Describe structural break alignment with same-side observed flow.",
    ),
    _contract(
        "pullback_mitigation_candidate",
        "mitigation",
        ["order_block_candidate", "imbalance_candidate", "mitigation_candidate", "responsive_or_initiative_flow_candidate", "observer_watch_candidate"],
        ["historical_outcome_refs", "context_refs"],
        ["order_block_candidate:bullish", "imbalance_candidate:bullish", "mitigation_candidate", "responsive_buyer_candidate|initiative_buyer_candidate", "long_watch_candidate"],
        ["order_block_candidate:bearish", "imbalance_candidate:bearish", "mitigation_candidate", "responsive_seller_candidate|initiative_seller_candidate", "short_watch_candidate"],
        "Describe a candidate reaction after revisiting a recorded structure zone.",
    ),
    _contract(
        "trap_reversal_candidate",
        "trap_reversal",
        ["trapped_trader_candidate", "responsive_flow_candidate", "absorption_candidate", "structure_change_candidate", "observer_condition_satisfied_candidate"],
        ["historical_outcome_refs", "context_refs"],
        ["trapped_seller_candidate", "responsive_buyer_candidate", "absorption_candidate:buy", "CHoCH_candidate:up|HL_candidate", "long_condition_satisfied_candidate"],
        ["trapped_buyer_candidate", "responsive_seller_candidate", "absorption_candidate:sell", "CHoCH_candidate:down|LH_candidate", "short_condition_satisfied_candidate"],
        "Describe opposite-direction closing behavior after failed aggression.",
    ),
    _contract(
        "premium_alignment_candidate",
        "multi_timeframe_alignment",
        ["setup_candidates_from_distinct_timeframes", "structure_bias_or_BOS", "evidence_side_classification", "observer_condition_satisfied_candidate"],
        ["historical_outcome_refs", "calibration_profile_refs", "context_refs"],
        ["long candidate families on at least two distinct timeframes", "structure_bias:up|BOS_candidate:up", "evidence:buy_side", "long_condition_satisfied_candidate"],
        ["short candidate families on at least two distinct timeframes", "structure_bias:down|BOS_candidate:down", "evidence:sell_side", "short_condition_satisfied_candidate"],
        "Describe deterministic multi-timeframe directional alignment without reliability weighting.",
        ["two distinct timeframes is a structural composition rule, not a trade threshold"],
    ),
]


def get_setup_contract(setup_name: str) -> dict[str, Any] | None:
    for contract in SETUP_CONTRACTS:
        if contract["setup_name"] == setup_name:
            return contract
    return None


def validate_setup_contracts() -> list[str]:
    required_fields = {
        "setup_name", "setup_family", "input_sources", "required_inputs",
        "optional_inputs", "setup_logic", "allowed_timeframes",
        "calibration_status", "confidence", "strength_score", "thresholds",
        "output_schema", "validation_invariants", "forbidden_behavior",
    }
    errors: list[str] = []
    names: set[str] = set()
    for contract in SETUP_CONTRACTS:
        name = str(contract.get("setup_name") or "")
        if not name:
            errors.append("empty setup_name")
        if name in names:
            errors.append(f"duplicate setup_name: {name}")
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
    for item in SETUP_CONTRACTS:
        print(item["setup_name"])
