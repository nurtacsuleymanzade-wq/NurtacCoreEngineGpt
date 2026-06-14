"""Declarative Layer-4 detector contracts.

This module defines measurement-only detector candidates. It does not produce
signals, scores, forecasts, trade decisions, or calibrated thresholds.
"""

from copy import deepcopy
from typing import Any


CONTRACT_VERSION = "1.0.0"

NORMALIZED_MARKET_ROW = {
    "symbol": "BTCUSDT",
    "timeframe": "1S|3S|5S|15S|1M",
    "window_start_ts": int,
    "window_end_ts": int,
    "open": "float|null",
    "high": "float|null",
    "low": "float|null",
    "close": "float|null",
    "buy_volume": float,
    "sell_volume": float,
    "total_volume": float,
    "delta": float,
    "trade_count": int,
    "footprint_levels": list,
    "bid_update_count": "int|null",
    "ask_update_count": "int|null",
    "dominant_side": "bid|ask|neutral|unknown",
    "context": "dict|null",
    "data_quality": dict,
}

MARKET_ROW_SOURCES = {
    "1S": "data/one_second_combined_dna.jsonl",
    "3S": "data/rolling_3s_dna.jsonl",
    "5S": "data/rolling_5s_dna.jsonl",
    "15S": "data/rolling_15s_dna.jsonl",
    "1M": "data/aligned_1m_candle_dna.jsonl",
    "context": "data/context_dna.jsonl",
}

_OUTPUT_SCHEMA = {
    "layer": "Layer-4",
    "engine": "DetectorEngine",
    "record_type": "detector_candidate",
    "detector_name": "<name>",
    "detector_family": "<family>",
    "symbol": "BTCUSDT",
    "timeframe": "<timeframe>",
    "window_start_ts": int,
    "window_end_ts": int,
    "event_type": "<event_type>",
    "side": "buy|sell|neutral|unknown",
    "direction": "up|down|flat|unknown",
    "calibration_status": "uncalibrated",
    "confidence": None,
    "strength_score": None,
    "thresholds": None,
    "measurements": {},
    "reason": {},
    "source_refs": {},
    "context_refs": {},
    "data_quality": {},
    "validation": {
        "input_valid": "bool",
        "required_fields_present": "bool",
        "invariants_passed": "bool",
        "errors": [],
    },
}


def _output_schema(
    detector_name: str,
    detector_family: str,
    event_type: Any,
    reason: dict[str, Any] | None = None,
) -> dict[str, Any]:
    schema = deepcopy(_OUTPUT_SCHEMA)
    schema["detector_name"] = detector_name
    schema["detector_family"] = detector_family
    schema["event_type"] = event_type
    if reason is not None:
        schema["reason"] = reason
    return schema


DETECTOR_CONTRACTS = [
    {
        "detector_name": "absorption_candidate",
        "detector_family": "order_flow_reaction",
        "input_sources": ["NormalizedMarketRow", "footprint_levels", "context (optional)"],
        "required_fields": ["open", "close", "high", "low", "delta", "total_volume", "footprint_levels"],
        "optional_fields": ["context.atr", "context.vwap", "context.cvd", "depth_imbalance"],
        "measurement_formula": {
            "price_change": "close - open",
            "range": "high - low",
            "delta_direction": {
                "delta > 0": "buy_aggression",
                "delta < 0": "sell_aggression",
                "delta == 0": "neutral_aggression",
            },
            "price_response": {
                "price_change > 0": "upward_response",
                "price_change < 0": "downward_response",
                "price_change == 0": "no_directional_response",
            },
            "absorption_logic": {
                "buy_side_aggression_absorption_candidate": "delta > 0 and price_response != upward_response",
                "sell_side_aggression_absorption_candidate": "delta < 0 and price_response != downward_response",
            },
            "interpretation": {
                "buy_side": "Aggressive buy flow exists, but price does not confirm upward movement.",
                "sell_side": "Aggressive sell flow exists, but price does not confirm downward movement.",
            },
            "side_mapping": {
                "delta > 0 and price_response != upward_response": "sell",
                "delta < 0 and price_response != downward_response": "buy",
                "otherwise": "neutral",
            },
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema("absorption_candidate", "order_flow_reaction", "absorption_candidate"),
        "validation_invariants": [
            "total_volume >= 0",
            "high >= low when both values exist",
            "open and close are inside the high/low range when all values exist",
            "delta == buy_volume - sell_volume when buy_volume and sell_volume exist",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not label as confirmed absorption.",
            "Do not assign confidence.",
            "Do not use a fixed delta threshold.",
            "Do not use a fixed volume threshold.",
        ],
    },
    {
        "detector_name": "sweep_candidate",
        "detector_family": "liquidity_movement",
        "input_sources": ["NormalizedMarketRow", "footprint_levels"],
        "required_fields": ["high", "low", "open", "close", "footprint_levels", "price_level_count"],
        "optional_fields": ["trade_count", "total_volume", "context.atr", "context.volume_context"],
        "measurement_formula": {
            "range": "high - low",
            "price_level_count": "len(footprint_levels)",
            "direction": {
                "close > open": "up",
                "close < open": "down",
                "close == open": "flat",
            },
            "multi_level_movement": "price_level_count > 1",
            "sweep_candidate_logic": "Price moved across more than one footprint price level within the same bucket.",
            "side_mapping": {"direction == up": "buy", "direction == down": "sell", "direction == flat": "neutral"},
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema("sweep_candidate", "liquidity_movement", "sweep_candidate"),
        "validation_invariants": [
            "footprint_levels is a list",
            "high >= low",
            "if high == low, direction may be flat but the candidate must not imply movement",
            "no fixed range threshold is allowed",
            "no fixed trade_count threshold is allowed",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not call this a liquidation sweep.",
            "Do not call this a stop hunt.",
            "Do not use a range_ticks threshold.",
            "Do not infer hidden liquidity.",
        ],
    },
    {
        "detector_name": "exhaustion_candidate",
        "detector_family": "flow_decay",
        "input_sources": ["NormalizedMarketRow", "context (optional)"],
        "required_fields": ["total_volume", "delta", "open", "close", "high", "low"],
        "optional_fields": ["context.cvd", "context.volume_context", "context.atr"],
        "measurement_formula": {
            "price_change": "close - open",
            "delta_direction": {"delta > 0": "buy_pressure", "delta < 0": "sell_pressure", "delta == 0": "neutral_pressure"},
            "response_direction": {"price_change > 0": "up", "price_change < 0": "down", "price_change == 0": "flat"},
            "exhaustion_candidate_logic": {
                "neutral_exhaustion_candidate": "delta == 0 and total_volume > 0",
                "buy_flow_exhaustion_candidate": "delta > 0 and close <= open",
                "sell_flow_exhaustion_candidate": "delta < 0 and close >= open",
            },
            "side_mapping": {
                "delta > 0 and close <= open": "sell",
                "delta < 0 and close >= open": "buy",
                "delta == 0": "neutral",
            },
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema("exhaustion_candidate", "flow_decay", "exhaustion_candidate"),
        "validation_invariants": [
            "total_volume >= 0",
            "delta is numeric",
            "open and close are valid or the event is marked invalid",
            "no fixed volume threshold is allowed",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not call it confirmed exhaustion.",
            "Do not say the trend ended.",
            "Do not infer reversal.",
            "Do not use an arbitrary range or trade_count threshold.",
        ],
    },
    {
        "detector_name": "iceberg_candidate",
        "detector_family": "hidden_liquidity_candidate",
        "input_sources": ["NormalizedMarketRow", "footprint_levels"],
        "required_fields": ["footprint_levels", "total_volume"],
        "optional_fields": ["max_level_volume", "max_level_price", "max_level_delta", "max_level_trade_count", "depth_imbalance"],
        "measurement_formula": {
            "level_total_volume": "level.buy_volume + level.sell_volume",
            "level_delta": "level.buy_volume - level.sell_volume",
            "level_trade_count": "level.trade_count",
            "max_level": "footprint price level with the highest level_total_volume",
            "footprint_concentration": "max_level.total_volume / total_volume when total_volume > 0, otherwise null",
            "iceberg_candidate_logic": "If one price level concentrates measurable footprint activity, emit a measurement-only candidate.",
            "side_mapping": {
                "max_level_delta > 0": "sell_side_absorption_candidate",
                "max_level_delta < 0": "buy_side_absorption_candidate",
                "max_level_delta == 0": "neutral",
            },
            "warning": "public_stream_cannot_confirm_iceberg",
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema(
            "iceberg_candidate",
            "hidden_liquidity_candidate",
            "iceberg_candidate",
            {"warning": "public_stream_cannot_confirm_iceberg"},
        ),
        "validation_invariants": [
            "footprint_levels is present",
            "max_level is derived from footprint_levels",
            "total_volume >= 0",
            "reason.warning == public_stream_cannot_confirm_iceberg",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not label as a confirmed iceberg.",
            "Do not infer an actual hidden order.",
            "Do not use a fixed concentration threshold.",
            "Do not use a fixed trade_count threshold.",
        ],
    },
    {
        "detector_name": "trapped_trader_candidate",
        "detector_family": "failed_aggression",
        "input_sources": ["NormalizedMarketRow", "context (optional)"],
        "required_fields": ["open", "close", "delta", "total_volume"],
        "optional_fields": ["footprint_levels", "context.cvd", "context.vwap"],
        "measurement_formula": {
            "price_change": "close - open",
            "aggression_side": {"delta > 0": "buyer_aggression", "delta < 0": "seller_aggression", "delta == 0": "neutral"},
            "result_direction": {"price_change > 0": "up", "price_change < 0": "down", "price_change == 0": "flat"},
            "trapped_buyer_candidate_logic": "delta > 0 and price_change < 0",
            "trapped_seller_candidate_logic": "delta < 0 and price_change > 0",
            "interpretation": {
                "trapped_buyer_candidate": "Buyer aggression exists but the candle result is down.",
                "trapped_seller_candidate": "Seller aggression exists but the candle result is up.",
            },
            "side_mapping": {"trapped_buyer_candidate": "sell", "trapped_seller_candidate": "buy"},
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema(
            "trapped_trader_candidate",
            "failed_aggression",
            ["trapped_buyer_candidate", "trapped_seller_candidate"],
        ),
        "validation_invariants": [
            "delta is numeric",
            "open and close are valid",
            "total_volume >= 0",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not say actual traders are trapped.",
            "Do not imply liquidation.",
            "Do not produce a reversal signal.",
            "Do not assign a score.",
        ],
    },
    {
        "detector_name": "initiative_flow_candidate",
        "detector_family": "directional_aggression",
        "input_sources": ["NormalizedMarketRow", "context (optional)"],
        "required_fields": ["open", "close", "delta", "total_volume"],
        "optional_fields": ["footprint_levels", "context.cvd", "context.vwap", "depth_imbalance"],
        "measurement_formula": {
            "price_change": "close - open",
            "initiative_buyer_candidate_logic": "delta > 0 and price_change > 0",
            "initiative_seller_candidate_logic": "delta < 0 and price_change < 0",
            "interpretation": {
                "initiative_buyer_candidate": "Buyer aggression and price direction align upward.",
                "initiative_seller_candidate": "Seller aggression and price direction align downward.",
            },
            "side_mapping": {"initiative_buyer_candidate": "buy", "initiative_seller_candidate": "sell"},
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema(
            "initiative_flow_candidate",
            "directional_aggression",
            ["initiative_buyer_candidate", "initiative_seller_candidate"],
        ),
        "validation_invariants": [
            "open and close are valid",
            "delta is numeric",
            "total_volume >= 0",
            "thresholds is null",
            "confidence is null",
            "strength_score is null",
        ],
        "forbidden_behavior": [
            "Do not call it trend continuation.",
            "Do not call it an entry signal.",
            "Do not infer future price.",
            "Do not assign a score.",
        ],
    },
    {
        "detector_name": "delta_imbalance_candidate",
        "detector_family": "flow_imbalance_measurement",
        "input_sources": ["NormalizedMarketRow", "context (optional)"],
        "required_fields": ["buy_volume", "sell_volume", "total_volume", "delta"],
        "optional_fields": ["context.cvd", "context.volume_context", "footprint_levels"],
        "measurement_formula": {
            "delta": "buy_volume - sell_volume",
            "delta_abs": "abs(delta)",
            "delta_ratio": "delta / total_volume when total_volume > 0, otherwise null",
            "buy_ratio": "buy_volume / total_volume when total_volume > 0, otherwise null",
            "sell_ratio": "sell_volume / total_volume when total_volume > 0, otherwise null",
            "delta_direction": {
                "delta > 0": "buy_dominant_flow",
                "delta < 0": "sell_dominant_flow",
                "delta == 0": "neutral_flow",
            },
            "candidate_logic": (
                "Record measurable delta-side dominance from sign and ratios without deciding imbalance strength."
            ),
            "side_mapping": {"delta > 0": "buy", "delta < 0": "sell", "delta == 0": "neutral"},
            "direction": "unknown",
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema(
            "delta_imbalance_candidate",
            "flow_imbalance_measurement",
            "delta_imbalance_candidate",
        ),
        "validation_invariants": [
            "total_volume >= 0",
            "delta == buy_volume - sell_volume when buy_volume and sell_volume exist",
            "ratios are null when total_volume == 0",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not call this a confirmed imbalance.",
            "Do not infer direction.",
        ],
    },
    {
        "detector_name": "momentum_candidate",
        "detector_family": "price_movement_measurement",
        "input_sources": ["NormalizedMarketRow", "context (optional)"],
        "required_fields": ["open", "high", "low", "close"],
        "optional_fields": ["total_volume", "delta", "context.atr", "context.vwap"],
        "measurement_formula": {
            "price_change": "close - open",
            "range": "high - low",
            "body": "close - open",
            "abs_body": "abs(body)",
            "close_position": "(close - low) / range when range > 0, otherwise null",
            "movement_direction": {
                "price_change > 0": "up",
                "price_change < 0": "down",
                "price_change == 0": "flat",
            },
            "candidate_logic": "Record price movement from the open/close relation without a magnitude threshold.",
            "side_mapping": {
                "movement_direction == up": "buy",
                "movement_direction == down": "sell",
                "movement_direction == flat": "neutral",
            },
            "direction": "up|down|flat",
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema(
            "momentum_candidate",
            "price_movement_measurement",
            "momentum_candidate",
        ),
        "validation_invariants": [
            "high >= low",
            "open and close are inside the high/low range when all values exist",
            "no range threshold is allowed",
            "no tick threshold is allowed",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not call this a trend.",
            "Do not call this a breakout.",
            "Do not infer continuation.",
            "Do not assign a score.",
        ],
    },
    {
        "detector_name": "aggression_burst_candidate",
        "detector_family": "activity_expansion_measurement",
        "input_sources": ["NormalizedMarketRow", "context (optional)"],
        "required_fields": ["trade_count", "total_volume", "delta"],
        "optional_fields": ["context.volume_context", "context.cvd", "footprint_levels"],
        "measurement_formula": {
            "trade_count": "number of trades in the source bucket",
            "total_volume": "buy_volume + sell_volume",
            "delta_abs": "abs(delta)",
            "avg_trade_size": "total_volume / trade_count when trade_count > 0, otherwise null",
            "activity_direction": {
                "delta > 0": "buy_activity",
                "delta < 0": "sell_activity",
                "delta == 0": "neutral_activity",
            },
            "candidate_logic": (
                "Record measurable activity expansion without deciding statistical magnitude; "
                "a historical baseline is required for later calibration."
            ),
            "side_mapping": {"delta > 0": "buy", "delta < 0": "sell", "delta == 0": "neutral"},
            "direction": "unknown",
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema(
            "aggression_burst_candidate",
            "activity_expansion_measurement",
            "aggression_burst_candidate",
        ),
        "validation_invariants": [
            "trade_count >= 0",
            "total_volume >= 0",
            "avg_trade_size is null when trade_count == 0",
            "no trade_count threshold is allowed",
            "no volume threshold is allowed",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not call activity high.",
            "Do not call the burst confirmed.",
            "Do not use a z-score threshold here.",
            "Do not infer entry direction.",
        ],
    },
    {
        "detector_name": "responsive_buyer_candidate",
        "detector_family": "opposite_flow_response",
        "input_sources": ["NormalizedMarketRow", "context (optional)"],
        "required_fields": ["open", "close", "delta"],
        "optional_fields": ["total_volume", "footprint_levels", "context.vwap", "context.cvd"],
        "measurement_formula": {
            "price_change": "close - open",
            "flow_direction": {"delta > 0": "buy_flow", "delta < 0": "sell_flow", "delta == 0": "neutral_flow"},
            "price_response": {
                "price_change > 0": "upward_response",
                "price_change < 0": "downward_response",
                "price_change == 0": "flat_response",
            },
            "candidate_logic": "delta < 0 and price_change >= 0",
            "candidate_meaning": "Sell-side flow is present but price response is not downward.",
            "side": "buy",
            "direction": {
                "price_change > 0": "up",
                "price_change == 0": "flat",
                "otherwise": "unknown",
            },
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema(
            "responsive_buyer_candidate",
            "opposite_flow_response",
            "responsive_buyer_candidate",
        ),
        "validation_invariants": [
            "delta is numeric",
            "open and close are valid",
            "no delta threshold is allowed",
            "no price movement threshold is allowed",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not call it confirmed buyer defense.",
            "Do not infer support.",
            "Do not infer reversal.",
            "Do not assign a score.",
        ],
    },
    {
        "detector_name": "responsive_seller_candidate",
        "detector_family": "opposite_flow_response",
        "input_sources": ["NormalizedMarketRow", "context (optional)"],
        "required_fields": ["open", "close", "delta"],
        "optional_fields": ["total_volume", "footprint_levels", "context.vwap", "context.cvd"],
        "measurement_formula": {
            "price_change": "close - open",
            "flow_direction": {"delta > 0": "buy_flow", "delta < 0": "sell_flow", "delta == 0": "neutral_flow"},
            "price_response": {
                "price_change > 0": "upward_response",
                "price_change < 0": "downward_response",
                "price_change == 0": "flat_response",
            },
            "candidate_logic": "delta > 0 and price_change <= 0",
            "candidate_meaning": "Buy-side flow is present but price response is not upward.",
            "side": "sell",
            "direction": {
                "price_change < 0": "down",
                "price_change == 0": "flat",
                "otherwise": "unknown",
            },
        },
        "calibration_status": "uncalibrated",
        "confidence": None,
        "strength_score": None,
        "thresholds": None,
        "output_schema": _output_schema(
            "responsive_seller_candidate",
            "opposite_flow_response",
            "responsive_seller_candidate",
        ),
        "validation_invariants": [
            "delta is numeric",
            "open and close are valid",
            "no delta threshold is allowed",
            "no price movement threshold is allowed",
            "confidence is null",
            "strength_score is null",
            "thresholds is null",
        ],
        "forbidden_behavior": [
            "Do not call it confirmed seller defense.",
            "Do not infer resistance.",
            "Do not infer reversal.",
            "Do not assign a score.",
        ],
    },
]

_COMMON_FORBIDDEN_BEHAVIOR = [
    "Do not assign confidence.",
    "Do not assign a strength score.",
    "Do not use numeric thresholds.",
    "Do not produce a trade decision.",
]

for _contract in DETECTOR_CONTRACTS:
    _contract["contract_version"] = CONTRACT_VERSION
    for _prohibition in _COMMON_FORBIDDEN_BEHAVIOR:
        if _prohibition not in _contract["forbidden_behavior"]:
            _contract["forbidden_behavior"].append(_prohibition)


def get_detector_contract(detector_name: str) -> dict[str, Any] | None:
    """Return the named detector contract, or None when it is not registered."""
    for contract in DETECTOR_CONTRACTS:
        if contract["detector_name"] == detector_name:
            return contract
    return None


def validate_detector_contracts() -> list[str]:
    """Return registry errors; an empty list means runtime use is allowed."""
    errors: list[str] = []
    required_fields = {
        "detector_name",
        "detector_family",
        "input_sources",
        "required_fields",
        "optional_fields",
        "measurement_formula",
        "calibration_status",
        "confidence",
        "strength_score",
        "thresholds",
        "output_schema",
        "validation_invariants",
        "forbidden_behavior",
        "contract_version",
    }
    names: set[str] = set()
    for index, contract in enumerate(DETECTOR_CONTRACTS):
        label = str(contract.get("detector_name", f"contract[{index}]"))
        missing = sorted(required_fields - contract.keys())
        if missing:
            errors.append(f"{label}: missing fields: {', '.join(missing)}")
        if label in names:
            errors.append(f"{label}: duplicate detector_name")
        names.add(label)
        if contract.get("contract_version") != CONTRACT_VERSION:
            errors.append(f"{label}: invalid contract_version")
        if contract.get("calibration_status") != "uncalibrated":
            errors.append(f"{label}: calibration_status must be uncalibrated")
        if contract.get("confidence") is not None:
            errors.append(f"{label}: confidence must be None")
        if contract.get("strength_score") is not None:
            errors.append(f"{label}: strength_score must be None")
        if contract.get("thresholds") is not None:
            errors.append(f"{label}: thresholds must be None")
        output_schema = contract.get("output_schema")
        if not isinstance(output_schema, dict):
            errors.append(f"{label}: output_schema must be a dict")
            continue
        if output_schema.get("detector_name") != label:
            errors.append(f"{label}: output_schema detector_name mismatch")
        if output_schema.get("calibration_status") != contract.get("calibration_status"):
            errors.append(f"{label}: output_schema calibration_status mismatch")
        if any(output_schema.get(field) is not None for field in ("confidence", "strength_score", "thresholds")):
            errors.append(f"{label}: output_schema score or threshold must be None")
    return errors


if __name__ == "__main__":
    for detector_contract in DETECTOR_CONTRACTS:
        print(detector_contract["detector_name"])
