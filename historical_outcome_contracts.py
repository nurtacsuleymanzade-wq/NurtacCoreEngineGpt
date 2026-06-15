"""Layer-8 historical outcome observation contract registry."""

from copy import deepcopy
from typing import Any


OBSERVATION_WINDOWS = {"30s": 30_000, "60s": 60_000, "300s": 300_000, "900s": 900_000, "3600s": 3_600_000}

OUTPUT_SCHEMA = {
    "layer": "Layer-8", "engine": "HistoricalOutcomeEngine",
    "record_type": "historical_outcome", "observation_id": "string",
    "source_type": "detector|structure|setup|observer|volume_profile|evidence",
    "source_event_id": "string", "symbol": "BTCUSDT", "timeframe": "string",
    "event_ts": int, "observation_window": "30s|60s|300s|900s|3600s",
    "start_price": float, "highest_price": float, "lowest_price": float,
    "close_price": float,
    "price_path": {"max_excursion": float, "min_excursion": float, "net_change": float},
    "calibration_status": "uncalibrated", "confidence": None,
    "strength_score": None, "thresholds": None, "probability": None,
    "edge_score": None,
}

FORBIDDEN = [
    "no trade decision", "no setup", "no long short signal", "no entry",
    "no stop loss", "no take profit", "no leverage", "no position sizing",
    "no confidence", "no strength score", "no probability", "no edge score",
    "no numeric threshold", "no calibration", "no decision gate",
]


def _contract(name: str, family: str, source_types: list[str]) -> dict[str, Any]:
    return {
        "contract_name": name, "contract_family": family,
        "source_types": source_types, "observation_windows": dict(OBSERVATION_WINDOWS),
        "measurement_formula": {
            "start_price": "first observed close at or after event_ts",
            "highest_price": "maximum observed close through horizon end",
            "lowest_price": "minimum observed close through horizon end",
            "close_price": "last observed close at or before horizon end",
            "max_excursion": "highest_price - start_price",
            "min_excursion": "lowest_price - start_price",
            "net_change": "close_price - start_price",
        },
        "calibration_status": "uncalibrated", "confidence": None,
        "strength_score": None, "thresholds": None, "probability": None,
        "edge_score": None, "output_schema": deepcopy(OUTPUT_SCHEMA),
        "validation_invariants": [
            "observation window is descriptive and not a threshold",
            "event timestamp is not after observation start",
            "all score probability and threshold fields remain null",
            "outcome contains no success failure win or loss classification",
        ],
        "forbidden_behavior": list(FORBIDDEN),
    }


HISTORICAL_OUTCOME_CONTRACTS = [
    _contract("future_outcome_observation", "future_outcome", ["detector", "structure", "setup", "observer", "volume_profile", "evidence"]),
    _contract("event_outcome_observation", "event_outcome", ["detector", "structure", "observer", "volume_profile"]),
    _contract("setup_outcome_observation", "setup_outcome", ["setup"]),
    _contract("observer_outcome_observation", "observer_outcome", ["observer"]),
    _contract("structure_outcome_observation", "structure_outcome", ["structure"]),
    _contract("volume_profile_outcome_observation", "volume_profile_outcome", ["volume_profile"]),
    _contract("detector_outcome_observation", "detector_outcome", ["detector"]),
    _contract("evidence_outcome_observation", "evidence_outcome", ["evidence"]),
]


def get_historical_outcome_contract(contract_name: str) -> dict[str, Any] | None:
    return next((item for item in HISTORICAL_OUTCOME_CONTRACTS if item["contract_name"] == contract_name), None)


def validate_historical_outcome_contracts() -> list[str]:
    required = {"contract_name", "contract_family", "source_types", "observation_windows",
        "measurement_formula", "calibration_status", "confidence", "strength_score",
        "thresholds", "probability", "edge_score", "output_schema",
        "validation_invariants", "forbidden_behavior"}
    errors: list[str] = []; names: set[str] = set()
    for item in HISTORICAL_OUTCOME_CONTRACTS:
        name = str(item.get("contract_name") or "")
        if not name: errors.append("empty contract_name")
        if name in names: errors.append(f"duplicate contract_name: {name}")
        names.add(name)
        missing = required - item.keys()
        if missing: errors.append(f"{name}: missing fields: {', '.join(sorted(missing))}")
        if item.get("calibration_status") != "uncalibrated": errors.append(f"{name}: invalid calibration_status")
        for field in ("confidence", "strength_score", "thresholds", "probability", "edge_score"):
            if item.get(field) is not None: errors.append(f"{name}: {field} must be null")
    return errors


if __name__ == "__main__":
    for item in HISTORICAL_OUTCOME_CONTRACTS: print(item["contract_name"])
