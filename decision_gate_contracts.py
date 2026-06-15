"""Layer-11 Decision Gate contract registry.

This module defines candidate decision schemas only. It performs no file I/O,
order creation, position sizing, or live execution.
"""

from copy import deepcopy
from typing import Any


INPUT_SOURCES = [
    "data/probability_candidates.jsonl",
    "data/setup_candidates.jsonl",
    "data/evidence_packets.jsonl",
    "data/volume_profile_dna.jsonl",
    "data/volume_profile_events.jsonl",
    "data/structure_events.jsonl",
    "data/context_dna.jsonl",
    "data/calibration_profiles.json",
    "data/historical_outcome_observations.jsonl",
    "data/execution_plan_candidates.jsonl",
]

ALLOWED_TIMEFRAMES = ["1S", "3S", "5S", "15S", "1M", "5M", "15M", "1H"]

OUTPUT_SCHEMA = {
    "layer": "Layer-11",
    "engine": "DecisionGate",
    "record_type": "decision_gate_event",
    "decision_id": "string",
    "decision_name": "string",
    "decision_family": "string",
    "symbol": "BTCUSDT",
    "timeframe": "string",
    "window_start_ts": int,
    "window_end_ts": "int|null",
    "side": "long|short|neutral|unknown",
    "decision": "allow_paper_trade|reject|wait|manual_review|execution_plan_required",
    "reason": {},
    "probability_refs": [],
    "setup_refs": [],
    "execution_plan_refs": [],
    "evidence_refs": [],
    "structure_refs": [],
    "volume_profile_refs": [],
    "context_refs": [],
    "calibration_refs": [],
    "gate_checks": {
        "probability_available": "bool",
        "setup_available": "bool",
        "execution_plan_available": "bool",
        "historical_sample_available": "bool",
        "data_quality_ok": "bool",
        "contradiction_detected": "bool",
        "risk_reward_review_required": "bool",
    },
    "scores": {
        "confidence": None,
        "strength_score": None,
        "decision_score": None,
        "threshold": None,
    },
    "order_readiness": {
        "ready_for_order": False,
        "reason": "decision_gate_does_not_execute",
    },
    "paper_trade_readiness": {
        "ready_for_paper_trade": "bool",
        "reason": "string",
    },
    "validation": {
        "contract_found": "bool",
        "invariants_passed": "bool",
        "errors": [],
    },
}

VALIDATION_INVARIANTS = [
    "decision_name is not empty",
    "decision_family is not empty",
    "input_sources is a list",
    "required_inputs is a list",
    "optional_inputs is a list",
    "decision_logic is defined",
    "allowed_timeframes is a list",
    "calibration_dependency is explicit",
    "probability_dependency is explicit",
    "output_schema contains all required fields",
    "order_readiness.ready_for_order remains false",
    "confidence, strength_score, decision_score, and threshold remain null",
    "the contract performs no order creation or live execution",
]

FORBIDDEN_BEHAVIOR = [
    "no live execution",
    "no real order",
    "no market order",
    "no limit order",
    "no leverage",
    "no position sizing",
    "no hardcoded confidence",
    "no hardcoded strength score",
    "no hardcoded threshold",
    "no manual probability",
    "no heuristic probability",
]


def _contract(
    name: str,
    family: str,
    decision: str,
    required_inputs: list[str],
    optional_inputs: list[str],
    purpose: str,
    conditions: list[str],
) -> dict[str, Any]:
    schema = deepcopy(OUTPUT_SCHEMA)
    schema["decision_name"] = name
    schema["decision_family"] = family
    schema["decision"] = decision
    schema["paper_trade_readiness"]["ready_for_paper_trade"] = (
        True if name == "allow_paper_trade_candidate" else False
    )
    schema["paper_trade_readiness"]["reason"] = (
        "paper_trade_candidate_only"
        if name == "allow_paper_trade_candidate"
        else "decision_does_not_allow_paper_trade"
    )
    return {
        "decision_name": name,
        "decision_family": family,
        "input_sources": list(INPUT_SOURCES),
        "required_inputs": required_inputs,
        "optional_inputs": optional_inputs,
        "decision_logic": {
            "purpose": purpose,
            "conditions": conditions,
            "result": decision,
            "execution_semantics": "candidate decision only; never an order or execution",
        },
        "allowed_timeframes": list(ALLOWED_TIMEFRAMES),
        "calibration_dependency": {
            "source": "data/calibration_profiles.json",
            "required_when_applicable": True,
            "accepted_status": "measured_from_outcomes",
            "missing_behavior": "reject, wait, or manual review candidate only",
        },
        "probability_dependency": {
            "source": "data/probability_candidates.jsonl",
            "required_when_applicable": True,
            "accepted_method": "measured_from_historical_outcomes",
            "hardcoded_probability_allowed": False,
            "missing_behavior": "reject, wait, or execution plan required candidate only",
        },
        "output_schema": schema,
        "validation_invariants": list(VALIDATION_INVARIANTS),
        "forbidden_behavior": list(FORBIDDEN_BEHAVIOR),
    }


DECISION_GATE_CONTRACTS = [
    _contract(
        "allow_paper_trade_candidate",
        "paper_trade_gate",
        "allow_paper_trade",
        ["probability_candidate", "setup_candidate", "execution_plan_candidate"],
        ["evidence_packet", "data_quality_context", "calibration_profile"],
        "Define an eligibility candidate for a separate paper-trade engine.",
        [
            "measured probability candidate is available",
            "setup candidate is available",
            "execution plan candidate is available",
            "order readiness remains false",
        ],
    ),
    _contract(
        "reject_trade_candidate",
        "rejection",
        "reject",
        ["decision_context"],
        ["missing_evidence", "contradicting_evidence", "data_quality_context"],
        "Represent rejection when required evidence is missing or contradictory.",
        ["one or more required gate checks do not pass"],
    ),
    _contract(
        "wait_for_confirmation_candidate",
        "waiting",
        "wait",
        ["setup_candidate"],
        ["probability_candidate", "execution_plan_candidate", "confirmation_context"],
        "Represent a wait state while probability, execution, or confirmation context is absent.",
        ["setup exists", "one or more confirmation dependencies are unavailable"],
    ),
    _contract(
        "manual_review_candidate",
        "manual_review",
        "manual_review",
        ["decision_context"],
        ["contradiction_context", "data_quality_context", "review_notes"],
        "Represent a non-executing manual review request for unresolved context.",
        ["context is unresolved", "automatic rejection is not established"],
    ),
    _contract(
        "insufficient_data_reject_candidate",
        "data_insufficiency",
        "reject",
        ["probability_candidate", "sample_status"],
        ["calibration_profile", "missing_horizons"],
        "Represent rejection when measured calibration data is insufficient.",
        ["sample_status is insufficient_data", "no inferred probability is created"],
    ),
    _contract(
        "contradiction_reject_candidate",
        "contradiction",
        "reject",
        ["probability_candidate", "contradiction_context"],
        ["blocking_evidence", "opposing_evidence_refs"],
        "Represent rejection when the probability context explicitly reports contradiction.",
        ["contradiction_context.has_contradiction is true", "no strength score is used"],
    ),
    _contract(
        "data_quality_reject_candidate",
        "data_quality",
        "reject",
        ["data_quality_context"],
        ["gap_events", "missing_inputs", "validation_errors"],
        "Represent rejection when source data is invalid, missing, or contains a gap.",
        ["data quality context reports invalid, gap, or missing input"],
    ),
    _contract(
        "risk_reward_review_candidate",
        "risk_reward_review",
        "manual_review",
        ["execution_plan_candidate"],
        ["risk_context", "target_context", "invalidation_context"],
        "Request review when an execution plan exists but risk/reward context is unresolved.",
        ["execution plan exists", "risk_reward_review_required is true"],
    ),
    _contract(
        "execution_plan_required_candidate",
        "execution_dependency",
        "execution_plan_required",
        ["setup_candidate", "probability_candidate"],
        ["evidence_packet", "location_context", "calibration_profile"],
        "Request an execution plan candidate before any later paper-trade consideration.",
        ["setup and measured probability candidates exist", "execution plan candidate is absent"],
    ),
]


def get_decision_gate_contract(decision_name: str) -> dict[str, Any] | None:
    for contract in DECISION_GATE_CONTRACTS:
        if contract["decision_name"] == decision_name:
            return contract
    return None


if __name__ == "__main__":
    for item in DECISION_GATE_CONTRACTS:
        print(item["decision_name"])
