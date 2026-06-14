"""Declarative Layer-5 evidence packet contracts.

This registry defines evidence organization only. It does not produce trading
decisions, signals, setups, scores, forecasts, or numeric thresholds.
"""

from typing import Any


EVIDENCE_CONTRACTS = [
    {
        "contract_name": "evidence_packet_contract",
        "contract_layer": "Layer-5",
        "input_sources": {
            "primary": "data/evidence_inbox.jsonl",
            "optional": [
                "data/detector_events.jsonl",
                "data/detector_measurements.jsonl",
                "data/context_dna.jsonl",
                "data/data_quality.jsonl",
            ],
        },
        "grouping_key": ["symbol", "timeframe", "window_start_ts"],
        "version_field": "packet_version",
        "required_fields": [
            "symbol",
            "timeframe",
            "window_start_ts",
            "packet_version",
            "calibration_status",
            "evidence_summary",
            "evidence_events",
            "decision_readiness",
            "scores",
            "validation",
        ],
        "optional_fields": [
            "window_end_ts",
            "measurement_refs",
            "detector_event_refs",
            "context_refs",
            "data_quality",
        ],
        "packet_schema": {
            "layer": "Layer-5",
            "engine": "EvidenceEngine",
            "record_type": "evidence_packet",
            "symbol": "non_empty_string",
            "timeframe": "non_empty_string",
            "window_start_ts": int,
            "window_end_ts": "int|null",
            "packet_version": int,
            "calibration_status": "uncalibrated",
            "evidence_summary": {
                "total_events": int,
                "event_types": list,
                "buy_side_events": list,
                "sell_side_events": list,
                "neutral_events": list,
                "unknown_side_events": list,
            },
            "evidence_events": list,
            "measurement_refs": list,
            "detector_event_refs": list,
            "context_refs": list,
            "data_quality": dict,
            "decision_readiness": {
                "ready_for_decision": False,
                "reason": "uncalibrated_evidence",
            },
            "scores": {
                "confidence": None,
                "strength_score": None,
                "directional_score": None,
                "bias_score": None,
            },
            "validation": {
                "input_valid": "bool",
                "events_grouped": "bool",
                "errors": list,
            },
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "scores": None,
        "validation_invariants": [
            "symbol must be a non-empty string",
            "timeframe must be a non-empty string",
            "window_start_ts must be an int",
            "packet_version must be an int",
            "evidence_events must be a list",
            "evidence_summary.total_events == len(evidence_events)",
            "evidence_summary.event_types must contain unique values",
            "buy, sell, neutral, and unknown side lists are classification only and are not decisions",
            "calibration_status must equal uncalibrated",
            "all values in packet_schema.scores must be null",
            "decision_readiness.ready_for_decision must be false",
            "no numeric threshold is used",
            "no trade decision is produced",
        ],
        "forbidden_behavior": [
            "no trade decision",
            "no long short signal",
            "no setup",
            "no entry",
            "no stop loss",
            "no take profit",
            "no confidence score",
            "no strength score",
            "no directional score",
            "no bias score",
            "no numeric threshold",
        ],
    }
]


def get_evidence_contract(contract_name: str) -> dict[str, Any] | None:
    """Return the named evidence contract, or None when it is not registered."""
    for contract in EVIDENCE_CONTRACTS:
        if contract["contract_name"] == contract_name:
            return contract
    return None


if __name__ == "__main__":
    for evidence_contract in EVIDENCE_CONTRACTS:
        print(evidence_contract["contract_name"])
