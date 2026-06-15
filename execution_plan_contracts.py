"""Layer-9 uncalibrated execution-plan candidate contract registry.

The registry defines candidate schemas and plan families only. It performs no
file I/O and produces no trades, orders, risk parameters, scores, or thresholds.
"""

from copy import deepcopy
from typing import Any


INPUT_SOURCES = [
    "data/setup_candidates.jsonl",
    "data/context_dna.jsonl",
    "data/smart_money_dna.jsonl",
    "data/structure_events.jsonl",
    "data/evidence_packets.jsonl",
    "data/calibration_profiles.json",
    "data/historical_outcome_observations.jsonl",
]

ALLOWED_TIMEFRAMES = ["1S", "3S", "5S", "15S", "1M", "5M", "15M", "1H"]

_OUTPUT_SCHEMA = {
    "layer": "Layer-9",
    "engine": "ExecutionPlanEngine",
    "record_type": "execution_plan_candidate",
    "plan_id": "string",
    "plan_name": "string",
    "plan_family": "string",
    "setup_id": "string",
    "setup_name": "string",
    "setup_family": "string",
    "symbol": "BTCUSDT",
    "timeframe": "string",
    "window_start_ts": int,
    "window_end_ts": "int|null",
    "side": "long|short|neutral|unknown",
    "entry": {
        "entry_type": "market_follow|reclaim|pullback_retest|mitigation_zone|breakout_retest|sweep_reversal|unknown",
        "entry_price_candidate": None,
        "entry_zone_candidate": None,
        "entry_reason": {},
        "entry_status": "candidate_not_order",
    },
    "stop_loss": {
        "sl_type": "structure_invalidation|swing_invalidation|zone_invalidation|volatility_context|unknown",
        "sl_price_candidate": None,
        "sl_zone_candidate": None,
        "sl_reason": {},
        "sl_status": "candidate_not_order",
    },
    "take_profit": {
        "tp_type": "nearest_liquidity|structure_target|range_target|vwap_target|unknown",
        "tp_price_candidates": [],
        "tp_zone_candidates": [],
        "tp_reason": {},
        "tp_status": "candidate_not_order",
    },
    "invalidation": {
        "invalidation_type": "structure_break|setup_failure|zone_failure|opposite_signal|unknown",
        "invalidation_price_candidate": None,
        "invalidation_reason": {},
    },
    "supporting_evidence": [],
    "blocking_evidence": [],
    "setup_refs": [],
    "structure_refs": [],
    "context_refs": [],
    "historical_outcome_refs": [],
    "calibration_status": "uncalibrated",
    "scores": {
        "confidence": None,
        "strength_score": None,
        "execution_score": None,
        "edge_score": None,
        "probability_score": None,
        "threshold": None,
    },
    "order_readiness": {
        "ready_for_order": False,
        "reason": "execution_plan_uncalibrated",
    },
    "risk_readiness": {
        "ready_for_risk_engine": False,
        "reason": "execution_plan_uncalibrated",
    },
    "validation": {
        "contract_found": "bool",
        "invariants_passed": "bool",
        "errors": [],
    },
}

_COMMON_INVARIANTS = [
    "plan_name is not empty",
    "plan_family is not empty",
    "input_sources is a list",
    "required_inputs is a list",
    "optional_inputs is a list",
    "allowed_setup_families is a list",
    "allowed_timeframes is a list",
    "entry_model is defined",
    "stop_model is defined",
    "target_model is defined",
    "invalidation_model is defined",
    "calibration_status is uncalibrated",
    "confidence is null",
    "strength_score is null",
    "thresholds is null",
    "output_schema contains all required fields",
    "forbidden_behavior explicitly declares all prohibitions",
    "the contract produces no real order",
    "the contract produces no position size or leverage",
    "the contract produces no confidence, score, or threshold",
]

_COMMON_FORBIDDEN = [
    "no trade decision",
    "no execution",
    "no order",
    "no market order",
    "no limit order",
    "no entry execution",
    "no stop loss execution",
    "no take profit execution",
    "no leverage",
    "no position sizing",
    "no confidence",
    "no strength score",
    "no execution score",
    "no edge score",
    "no probability score",
    "no numeric threshold",
]


def _contract(
    name: str,
    family: str,
    allowed_setup_families: list[str],
    purpose: str,
    entry_model: str,
    stop_model: str,
    target_model: str,
    invalidation_model: str,
) -> dict[str, Any]:
    schema = deepcopy(_OUTPUT_SCHEMA)
    schema["plan_name"] = name
    schema["plan_family"] = family
    return {
        "plan_name": name,
        "plan_family": family,
        "input_sources": list(INPUT_SOURCES),
        "required_inputs": [
            "setup_id", "setup_name", "setup_family", "symbol", "timeframe",
            "window_start_ts", "side", "calibration_status",
        ],
        "optional_inputs": [
            "window_end_ts", "supporting_evidence", "blocking_evidence",
            "structure_refs", "context_refs", "historical_outcome_refs",
        ],
        "plan_logic": {
            "purpose": purpose,
            "interpretation": "uncalibrated execution plan candidate only; not an order or trade decision",
        },
        "allowed_setup_families": allowed_setup_families,
        "allowed_timeframes": list(ALLOWED_TIMEFRAMES),
        "entry_model": {"conceptual_reference": entry_model, "computed_value": None},
        "stop_model": {"conceptual_reference": stop_model, "computed_value": None},
        "target_model": {"conceptual_reference": target_model, "computed_values": []},
        "invalidation_model": {"conceptual_reference": invalidation_model, "computed_value": None},
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": schema,
        "validation_invariants": list(_COMMON_INVARIANTS),
        "forbidden_behavior": list(_COMMON_FORBIDDEN),
    }


EXECUTION_PLAN_CONTRACTS = [
    _contract(
        "market_follow_plan_candidate",
        "momentum_follow",
        ["continuation", "breakout"],
        "Describe a plan candidate that follows initiative or breakout continuation.",
        "Current close, last trade price, or current bucket close may be referenced.",
        "The latest micro swing or setup invalidation zone may be referenced.",
        "Nearby liquidity, range extension, or a historical outcome horizon may be referenced.",
        "Setup failure, opposite structure change, or loss of the continuation context.",
    ),
    _contract(
        "reclaim_entry_plan_candidate",
        "reclaim",
        ["liquidity_reclaim", "trap_reversal", "reversal"],
        "Describe an entry plan candidate after a swept level is reclaimed.",
        "The level reclaimed after a sweep may be referenced.",
        "The area outside the sweep low or high may be referenced.",
        "Opposite liquidity, prior structure, or a VWAP area may be referenced.",
        "Loss of the reclaimed level or failure of the originating setup.",
    ),
    _contract(
        "pullback_retest_plan_candidate",
        "retest",
        ["continuation", "breakout", "mitigation"],
        "Describe a pullback or retest plan candidate following directional movement.",
        "A post-breakout retest or pullback zone may be referenced.",
        "The opposite side of the retested structure may be referenced.",
        "The latest swing, liquidity pool, or value area may be referenced.",
        "Failure to hold the retested structure or invalidation of the source setup.",
    ),
    _contract(
        "mitigation_zone_plan_candidate",
        "zone_mitigation",
        ["mitigation", "reversal", "trap_reversal"],
        "Describe a plan candidate for a return into a mitigation zone.",
        "An order block, imbalance, or mitigation zone may be referenced.",
        "A close outside the zone or zone invalidation may be referenced.",
        "The originating impulse objective or nearby liquidity may be referenced.",
        "Zone failure, adverse structure break, or setup failure.",
    ),
    _contract(
        "breakout_retest_plan_candidate",
        "breakout_retest",
        ["breakout"],
        "Describe a retest plan candidate after a BOS or MSB structure break.",
        "The broken level retested after BOS or MSB may be referenced.",
        "Loss of the broken level after the retest may be referenced.",
        "The next structure high or low, or the next liquidity pool, may be referenced.",
        "Re-entry through the broken level or failure of the breakout setup.",
    ),
    _contract(
        "sweep_reversal_plan_candidate",
        "sweep_reversal",
        ["liquidity_reclaim", "trap_reversal", "reversal"],
        "Describe an opposite-direction plan candidate after a liquidity sweep.",
        "An opposite-direction trigger after an equal high, equal low, or swing liquidity sweep may be referenced.",
        "The area outside the sweep extreme may be referenced.",
        "Mid-range, opposite liquidity, or VWAP may be referenced.",
        "Extension beyond the sweep extreme, opposite signal, or setup failure.",
    ),
    _contract(
        "premium_alignment_plan_candidate",
        "premium_alignment",
        ["multi_timeframe_alignment"],
        "Describe a plan candidate supported by aligned directional context across timeframes.",
        "The lowest-timeframe trigger or pullback/retest after multi-timeframe alignment may be referenced.",
        "Main-timeframe structure invalidation or lower-timeframe invalidation may be referenced.",
        "Higher-timeframe liquidity, structure, or a historical outcome horizon may be referenced.",
        "Loss of multi-timeframe alignment or invalidation of the governing structure.",
    ),
]


def get_execution_plan_contract(plan_name: str) -> dict[str, Any] | None:
    for contract in EXECUTION_PLAN_CONTRACTS:
        if contract["plan_name"] == plan_name:
            return contract
    return None


if __name__ == "__main__":
    for item in EXECUTION_PLAN_CONTRACTS:
        print(item["plan_name"])
