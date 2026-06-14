"""Contracts for uncalibrated Smart Money candidate measurements."""

from copy import deepcopy
from typing import Any


_OUTPUT_SCHEMA = {
    "layer": "Layer-6A",
    "engine": "SmartMoneyEngine",
    "record_type": "structure_event",
    "event_id": "string",
    "symbol": "string",
    "timeframe": "string",
    "window_start_ts": int,
    "window_end_ts": "int|null",
    "event_type": "string",
    "side": "buy|sell|neutral|unknown",
    "direction": "up|down|flat|unknown",
    "calibration_status": "uncalibrated",
    "confidence": None,
    "strength_score": None,
    "thresholds": None,
    "measurements": {},
    "reason": {},
    "source_refs": {},
    "data_quality": {},
    "validation": {
        "input_valid": "bool",
        "contract_found": "bool",
        "invariants_passed": "bool",
        "errors": [],
    },
}

_COMMON_FORBIDDEN = [
    "no confidence",
    "no strength score",
    "no numeric threshold",
    "no trade decision",
    "no long short signal",
    "no setup",
    "no entry stop loss or take profit",
]


def _contract(
    name: str,
    family: str,
    required: list[str],
    optional: list[str],
    formula: dict[str, Any],
    invariants: list[str],
    forbidden: list[str],
) -> dict[str, Any]:
    schema = deepcopy(_OUTPUT_SCHEMA)
    schema["event_type"] = name
    return {
        "concept_name": name,
        "concept_family": family,
        "input_sources": ["NormalizedCandle", "timeframe_state"],
        "required_fields": required,
        "optional_fields": optional,
        "measurement_formula": formula,
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": schema,
        "validation_invariants": invariants + [
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": forbidden + list(_COMMON_FORBIDDEN),
    }


SMART_MONEY_CONTRACTS = [
    _contract("fractal_high_candidate", "local_swing", ["previous.high", "current.high", "next.high"], [], {"candidate_logic": "current.high > previous.high and current.high > next.high", "side": "sell", "direction": "down|unknown"}, ["three consecutive valid candles are required"], ["not a confirmed top"]),
    _contract("fractal_low_candidate", "local_swing", ["previous.low", "current.low", "next.low"], [], {"candidate_logic": "current.low < previous.low and current.low < next.low", "side": "buy", "direction": "up|unknown"}, ["three consecutive valid candles are required"], ["not a confirmed bottom"]),
    _contract("HH_candidate", "swing_classification", ["current_fractal_high.high", "previous_swing_high.high"], [], {"candidate_logic": "current_fractal_high.high > previous_swing_high.high", "side": "buy", "direction": "up"}, ["both swing highs must exist"], ["does not predict continuation"]),
    _contract("HL_candidate", "swing_classification", ["current_fractal_low.low", "previous_swing_low.low"], [], {"candidate_logic": "current_fractal_low.low > previous_swing_low.low", "side": "buy", "direction": "up"}, ["both swing lows must exist"], ["does not predict continuation"]),
    _contract("LH_candidate", "swing_classification", ["current_fractal_high.high", "previous_swing_high.high"], [], {"candidate_logic": "current_fractal_high.high < previous_swing_high.high", "side": "sell", "direction": "down"}, ["both swing highs must exist"], ["does not predict continuation"]),
    _contract("LL_candidate", "swing_classification", ["current_fractal_low.low", "previous_swing_low.low"], [], {"candidate_logic": "current_fractal_low.low < previous_swing_low.low", "side": "sell", "direction": "down"}, ["both swing lows must exist"], ["does not predict continuation"]),
    _contract("BOS_candidate", "structure_break", ["close", "structure_bias", "previous_swing_high|previous_swing_low"], [], {"up_logic": "structure_bias == up and close > previous_swing_high.high", "down_logic": "structure_bias == down and close < previous_swing_low.low", "unknown_bias": "no event"}, ["structure_bias must not be unknown"], ["not a confirmed continuation break"]),
    _contract("CHoCH_candidate", "structure_change", ["close", "structure_bias", "previous_swing_high|previous_swing_low"], [], {"down_logic": "structure_bias == up and close < previous_swing_low.low", "up_logic": "structure_bias == down and close > previous_swing_high.high", "unknown_bias": "no event"}, ["structure_bias must not be unknown"], ["not a reversal signal"]),
    _contract("MSB_candidate", "structure_break", ["source_structure_event_id"], ["BOS_candidate", "CHoCH_candidate"], {"candidate_logic": "emit when BOS_candidate or CHoCH_candidate is emitted", "reason_field": "source_structure_event_id"}, ["source structure event must exist"], ["not an independent trade signal"]),
    _contract("order_block_candidate", "price_zone", ["previous.open", "previous.close", "previous.high", "previous.low", "current.open", "current.close"], [], {"bullish_logic": "previous.close < previous.open and current.close > current.open", "bearish_logic": "previous.close > previous.open and current.close < current.open", "zone": ["previous.open", "previous.close", "previous.high", "previous.low"]}, ["direction change only; no magnitude rule"], ["not a confirmed order block", "do not define impulse with a numeric threshold"]),
    _contract("breaker_block_candidate", "price_zone", ["close", "stored_order_block.zone"], [], {"bearish_logic": "bullish order block and close < zone.low", "bullish_logic": "bearish order block and close > zone.high"}, ["zone must come from a prior order_block_candidate"], ["not a confirmed breaker block"]),
    _contract("imbalance_candidate", "price_gap", ["candle_1.high", "candle_1.low", "candle_3.high", "candle_3.low"], [], {"bullish_logic": "candle_1.high < candle_3.low", "bearish_logic": "candle_1.low > candle_3.high", "zone": ["gap_low", "gap_high"]}, ["three candles are required"], ["not a confirmed fair value gap"]),
    _contract("mitigation_candidate", "zone_revisit", ["low", "high", "stored_zone.low", "stored_zone.high"], ["order_block_candidate", "imbalance_candidate"], {"candidate_logic": "low <= zone.high and high >= zone.low", "reaction_required": False}, ["zone must predate the revisiting candle"], ["not proof of reaction or mitigation"]),
    _contract("equal_high_candidate", "equal_liquidity", ["current_fractal_high.high", "previous_fractal_high.high"], [], {"candidate_logic": "current_fractal_high.high == previous_fractal_high.high", "comparison": "exact equality", "side": "sell"}, ["no tolerance is used"], ["not confirmed liquidity", "do not add equality tolerance"]),
    _contract("equal_low_candidate", "equal_liquidity", ["current_fractal_low.low", "previous_fractal_low.low"], [], {"candidate_logic": "current_fractal_low.low == previous_fractal_low.low", "comparison": "exact equality", "side": "buy"}, ["no tolerance is used"], ["not confirmed liquidity", "do not add equality tolerance"]),
]


def get_smart_money_contract(concept_name: str) -> dict[str, Any] | None:
    for contract in SMART_MONEY_CONTRACTS:
        if contract["concept_name"] == concept_name:
            return contract
    return None


def validate_smart_money_contracts() -> list[str]:
    errors: list[str] = []
    names: set[str] = set()
    required = {"concept_name", "concept_family", "input_sources", "required_fields", "optional_fields", "measurement_formula", "calibration_status", "confidence", "strength_score", "thresholds", "output_schema", "validation_invariants", "forbidden_behavior"}
    for contract in SMART_MONEY_CONTRACTS:
        name = str(contract.get("concept_name"))
        if name in names:
            errors.append(f"duplicate concept_name: {name}")
        names.add(name)
        missing = sorted(required - contract.keys())
        if missing:
            errors.append(f"{name}: missing fields: {', '.join(missing)}")
        if contract.get("calibration_status") != "uncalibrated":
            errors.append(f"{name}: invalid calibration_status")
        if any(contract.get(field) is not None for field in ("confidence", "strength_score", "thresholds")):
            errors.append(f"{name}: score or thresholds must be null")
    return errors


if __name__ == "__main__":
    for item in SMART_MONEY_CONTRACTS:
        print(item["concept_name"])
