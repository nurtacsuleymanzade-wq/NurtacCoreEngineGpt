"""Layer-7 uncalibrated setup-candidate contract registry."""

from copy import deepcopy
from typing import Any

ALLOWED_TIMEFRAMES = ["1S", "3S", "5S", "15S", "1M", "5M", "15M", "1H"]
INPUT_SOURCES = [
    "data/evidence_packets.jsonl", "data/detector_events.jsonl",
    "data/structure_events.jsonl", "data/smart_money_dna.jsonl",
    "data/context_dna.jsonl", "data/volume_profile_dna.jsonl",
    "data/volume_profile_events.jsonl", "data/calibration_profiles.json",
    "data/historical_outcome_observations.jsonl", "data/data_quality.jsonl",
]

OUTPUT_SCHEMA = {
    "layer": "Layer-7", "engine": "SetupEngine", "record_type": "setup_candidate",
    "setup_id": "string", "setup_name": "string", "setup_family": "string",
    "symbol": "string", "timeframe": "string", "window_start_ts": int,
    "window_end_ts": "int|null", "side": "long|short|neutral|unknown",
    "setup_status": "candidate_not_trade_signal", "matched_concepts": [],
    "missing_concepts": [], "supporting_evidence": [], "blocking_evidence": [],
    "detector_refs": [], "evidence_refs": [], "structure_refs": [],
    "volume_profile_refs": [], "context_refs": [], "historical_outcome_refs": [],
    "location_context": {"poc": None, "vah": None, "val": None,
        "location_vs_poc": "above|below|at|unknown",
        "location_vs_value": "inside_value|above_value|below_value|unknown",
        "profile_shape": "p_shape|b_shape|d_shape|b_distribution|trend_profile|unknown"},
    "auction_context": {"acceptance_candidate": None, "rejection_candidate": None,
        "failed_auction_candidate": None, "failed_action_return_to_value_candidate": None},
    "calibration_status": "uncalibrated",
    "scores": {"confidence": None, "strength_score": None, "setup_score": None,
        "edge_score": None, "probability_score": None, "threshold": None},
    "execution_readiness": {"ready_for_execution_plan": False, "reason": "setup_uncalibrated"},
    "risk_readiness": {"ready_for_position_sizing": False, "reason": "no_execution_plan"},
    "validation": {"contract_found": "bool", "invariants_passed": "bool", "errors": []},
}

FORBIDDEN = ["no trade decision", "no setup execution", "no entry", "no stop loss",
    "no take profit", "no leverage", "no position size", "no confidence",
    "no strength score", "no setup score", "no edge score", "no probability score",
    "no threshold"]


def g(*alternatives: str) -> list[str]:
    return list(alternatives)


def contract(name: str, family: str, required: dict[str, list[list[str]]], optional: list[list[str]] | None = None) -> dict[str, Any]:
    schema = deepcopy(OUTPUT_SCHEMA); schema["setup_name"] = name; schema["setup_family"] = family
    return {"setup_name": name, "setup_family": family, "input_sources": list(INPUT_SOURCES),
        "required_concepts": required, "optional_concepts": optional or [],
        "setup_logic": "All required concept groups for a side must match; alternatives within a group are OR.",
        "allowed_timeframes": list(ALLOWED_TIMEFRAMES), "calibration_status": "uncalibrated",
        "confidence": None, "strength_score": None, "thresholds": None,
        "output_schema": schema,
        "validation_invariants": ["candidate_not_trade_signal", "same symbol timeframe and window bucket",
            "all scores remain null", "execution readiness remains false", "risk readiness remains false"],
        "forbidden_behavior": list(FORBIDDEN)}


SETUP_CONTRACTS = [
 contract("failed_auction_reversal_candidate", "auction_reversal", {
  "long":[g("failed_auction_candidate"),g("failed_action_return_to_value_candidate"),g("rejection_zone_candidate","location_vs_value below_value","location_vs_poc below"),g("responsive_buyer_candidate","absorption_candidate buy"),g("CHoCH_candidate up","HL_candidate")],
  "short":[g("failed_auction_candidate"),g("failed_action_return_to_value_candidate"),g("rejection_zone_candidate","location_vs_value above_value","location_vs_poc above"),g("responsive_seller_candidate","absorption_candidate sell"),g("CHoCH_candidate down","LH_candidate")]}),
 contract("failed_action_return_to_value_candidate", "return_to_value", {
  "long":[g("failed_action_return_to_value_candidate"),g("value_area_candidate"),g("location_vs_value inside_value"),g("responsive_buyer_candidate","trapped_seller_candidate")],
  "short":[g("failed_action_return_to_value_candidate"),g("value_area_candidate"),g("location_vs_value inside_value"),g("responsive_seller_candidate","trapped_buyer_candidate")]}),
 contract("long_trap_reversal_candidate", "trap_reversal", {"long":[g("sweep_candidate","equal_low_candidate"),g("trapped_seller_candidate"),g("responsive_buyer_candidate","absorption_candidate buy"),g("failed_action_return_to_value_candidate","value_area_reclaim_candidate"),g("CHoCH_candidate up","HL_candidate")]}),
 contract("short_trap_reversal_candidate", "trap_reversal", {"short":[g("sweep_candidate","equal_high_candidate"),g("trapped_buyer_candidate"),g("responsive_seller_candidate","absorption_candidate sell"),g("failed_action_return_to_value_candidate","value_area_reclaim_candidate"),g("CHoCH_candidate down","LH_candidate")]}),
 contract("initiative_breakout_candidate", "breakout", {
  "long":[g("BOS_candidate up","MSB_candidate up"),g("initiative_buyer_candidate"),g("momentum_candidate"),g("trend_profile_candidate","acceptance_continuation_candidate")],
  "short":[g("BOS_candidate down","MSB_candidate down"),g("initiative_seller_candidate"),g("momentum_candidate"),g("trend_profile_candidate","acceptance_continuation_candidate")]}),
 contract("initiative_continuation_candidate", "continuation", {
  "long":[g("initiative_buyer_candidate"),g("delta_imbalance_candidate"),g("momentum_candidate"),g("acceptance_zone_candidate","trend_profile_candidate"),g("HH_candidate","HL_candidate")],
  "short":[g("initiative_seller_candidate"),g("delta_imbalance_candidate"),g("momentum_candidate"),g("acceptance_zone_candidate","trend_profile_candidate"),g("LH_candidate","LL_candidate")]}),
 contract("responsive_rotation_candidate", "rotation", {
  "long":[g("responsive_buyer_candidate"),g("balance_zone_candidate","d_shape_profile_candidate"),g("value_area_touch_candidate"),g("absorption_candidate buy","trapped_seller_candidate")],
  "short":[g("responsive_seller_candidate"),g("balance_zone_candidate","d_shape_profile_candidate"),g("value_area_touch_candidate"),g("absorption_candidate sell","trapped_buyer_candidate")]}),
 contract("absorption_reversal_candidate", "absorption_reversal", {
  "long":[g("absorption_candidate buy"),g("responsive_buyer_candidate"),g("price_down_observation","sell_side"),g("CHoCH_candidate up","HL_candidate")],
  "short":[g("absorption_candidate sell"),g("responsive_seller_candidate"),g("price_up_observation","buy_side"),g("CHoCH_candidate down","LH_candidate")]}, [g("price_does_not_continue")]),
 contract("poc_reclaim_candidate", "poc_reclaim", {
  "long":[g("poc_level_candidate"),g("location_vs_poc above","location_vs_poc at"),g("failed_action_return_to_value_candidate","acceptance_zone_candidate"),g("responsive_buyer_candidate","initiative_buyer_candidate")],
  "short":[g("poc_level_candidate"),g("location_vs_poc below","location_vs_poc at"),g("failed_action_return_to_value_candidate","acceptance_zone_candidate"),g("responsive_seller_candidate","initiative_seller_candidate")]}),
 contract("value_area_reclaim_candidate", "value_reclaim", {
  "long":[g("value_area_candidate"),g("location_vs_value inside_value"),g("failed_auction_candidate","failed_action_return_to_value_candidate"),g("responsive_buyer_candidate","trapped_seller_candidate")],
  "short":[g("value_area_candidate"),g("location_vs_value inside_value"),g("failed_auction_candidate","failed_action_return_to_value_candidate"),g("responsive_seller_candidate","trapped_buyer_candidate")]}),
 contract("lvn_rejection_candidate", "lvn_rejection", {
  "long":[g("lvn_touch_candidate"),g("rejection_zone_candidate"),g("responsive_buyer_candidate","absorption_candidate buy")],
  "short":[g("lvn_touch_candidate"),g("rejection_zone_candidate"),g("responsive_seller_candidate","absorption_candidate sell")]}),
 contract("hvn_acceptance_candidate", "hvn_acceptance", {"neutral":[g("hvn_touch_candidate"),g("acceptance_zone_candidate"),g("d_shape_profile_candidate","balance_zone_candidate")]}),
 contract("acceptance_continuation_candidate", "acceptance_continuation", {
  "long":[g("acceptance_zone_candidate"),g("initiative_buyer_candidate"),g("momentum_candidate"),g("location_vs_value above_value","location_vs_value inside_value"),g("BOS_candidate up","HH_candidate")],
  "short":[g("acceptance_zone_candidate"),g("initiative_seller_candidate"),g("momentum_candidate"),g("location_vs_value below_value","location_vs_value inside_value"),g("BOS_candidate down","LL_candidate")]}),
 contract("trend_pullback_continuation_candidate", "trend_pullback", {
  "long":[g("trend_profile_candidate"),g("HL_candidate"),g("mitigation_candidate","value_area_touch_candidate"),g("initiative_buyer_candidate","responsive_buyer_candidate")],
  "short":[g("trend_profile_candidate"),g("LH_candidate"),g("mitigation_candidate","value_area_touch_candidate"),g("initiative_seller_candidate","responsive_seller_candidate")]}),
 contract("balance_breakout_candidate", "balance_breakout", {
  "long":[g("balance_zone_candidate"),g("BOS_candidate up","MSB_candidate up"),g("initiative_buyer_candidate"),g("trend_profile_candidate")],
  "short":[g("balance_zone_candidate"),g("BOS_candidate down","MSB_candidate down"),g("initiative_seller_candidate"),g("trend_profile_candidate")]}),
 contract("balance_return_candidate", "balance_return", {"neutral":[g("balance_zone_candidate"),g("failed_action_return_to_value_candidate"),g("d_shape_profile_candidate","acceptance_zone_candidate")]}),
 contract("liquidity_sweep_reversal_candidate", "liquidity_reversal", {
  "long":[g("sweep_candidate"),g("equal_low_candidate","fractal_low_candidate"),g("trapped_seller_candidate"),g("responsive_buyer_candidate"),g("CHoCH_candidate up")],
  "short":[g("sweep_candidate"),g("equal_high_candidate","fractal_high_candidate"),g("trapped_buyer_candidate"),g("responsive_seller_candidate"),g("CHoCH_candidate down")]}),
 contract("liquidity_sweep_continuation_candidate", "liquidity_continuation", {
  "long":[g("sweep_candidate"),g("initiative_buyer_candidate"),g("momentum_candidate"),g("BOS_candidate up")],
  "short":[g("sweep_candidate"),g("initiative_seller_candidate"),g("momentum_candidate"),g("BOS_candidate down")]}),
 contract("delta_divergence_reversal_candidate", "delta_divergence", {
  "long":[g("delta_negative_observation"),g("price_down_observation"),g("absorption_candidate buy","responsive_buyer_candidate"),g("CHoCH_candidate up")],
  "short":[g("delta_positive_observation"),g("price_up_observation"),g("absorption_candidate sell","responsive_seller_candidate"),g("CHoCH_candidate down")]}),
 contract("auction_completion_candidate", "auction_completion", {"neutral":[g("d_shape_profile_candidate","balance_zone_candidate"),g("acceptance_zone_candidate"),g("poc_level_candidate"),g("value_area_candidate")]})
]


def get_setup_contract(setup_name: str) -> dict[str, Any] | None:
    return next((item for item in SETUP_CONTRACTS if item["setup_name"] == setup_name), None)


def validate_setup_contracts() -> list[str]:
    required = {"setup_name","setup_family","input_sources","required_concepts","optional_concepts","setup_logic","allowed_timeframes","calibration_status","confidence","strength_score","thresholds","output_schema","validation_invariants","forbidden_behavior"}
    errors=[]; names=set()
    for item in SETUP_CONTRACTS:
        name=item.get("setup_name")
        if not name: errors.append("empty setup_name")
        if name in names: errors.append(f"duplicate setup_name: {name}")
        names.add(name)
        missing=required-item.keys()
        if missing: errors.append(f"{name}: missing {sorted(missing)}")
        if item.get("calibration_status") != "uncalibrated": errors.append(f"{name}: invalid calibration")
        if any(item.get(key) is not None for key in ("confidence","strength_score","thresholds")): errors.append(f"{name}: non-null score")
    return errors


if __name__ == "__main__":
    for item in SETUP_CONTRACTS: print(item["setup_name"])
