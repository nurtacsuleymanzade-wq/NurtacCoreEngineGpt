"""Layer-8 uncalibrated setup-candidate aggregation engine.

The engine performs structural matching only. It does not produce execution,
risk, score, threshold, probability, or trade decisions.
"""

import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from setup_contracts import ALLOWED_TIMEFRAMES, SETUP_CONTRACTS, get_setup_contract, validate_setup_contracts


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
INPUT_FILES = {
    "observer_events": DATA_DIR / "observer_events.jsonl",
    "observer_states": DATA_DIR / "observer_states.jsonl",
    "evidence_packets": DATA_DIR / "evidence_packets.jsonl",
    "structure_events": DATA_DIR / "structure_events.jsonl",
    "smart_money_dna": DATA_DIR / "smart_money_dna.jsonl",
    "context_dna": DATA_DIR / "context_dna.jsonl",
    "historical_outcome_observations": DATA_DIR / "historical_outcome_observations.jsonl",
    "calibration_profiles": DATA_DIR / "calibration_profiles.json",
    "detector_events": DATA_DIR / "detector_events.jsonl",
    "data_quality": DATA_DIR / "data_quality.jsonl",
}
PRIMARY_INPUTS = set(INPUT_FILES) - {"detector_events", "data_quality"}
CANDIDATES_FILE = DATA_DIR / "setup_candidates.jsonl"
HEALTH_FILE = DATA_DIR / "setup_health.json"
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0
NULL_SCORES = {
    "confidence": None,
    "strength_score": None,
    "setup_score": None,
    "edge_score": None,
    "probability_score": None,
    "threshold": None,
}

BucketKey = tuple[str, str, int]
DuplicateKey = tuple[str, str, str, int, str]

SIDE_COMPONENTS = {
    "initiative_continuation_candidate": {
        "long": {"initiative_buyer_candidate", "momentum_candidate:buy", "momentum_candidate:up", "delta_imbalance_candidate:buy", "long_condition_satisfied_candidate", "BOS_candidate:up", "HH_candidate", "HL_candidate"},
        "short": {"initiative_seller_candidate", "momentum_candidate:sell", "momentum_candidate:down", "delta_imbalance_candidate:sell", "short_condition_satisfied_candidate", "BOS_candidate:down", "LH_candidate", "LL_candidate"},
    },
    "absorption_reversal_candidate": {
        "long": {"absorption_candidate:buy", "responsive_buyer_candidate", "trapped_seller_candidate", "fractal_low_candidate", "HL_candidate", "long_watch_candidate", "long_condition_satisfied_candidate"},
        "short": {"absorption_candidate:sell", "responsive_seller_candidate", "trapped_buyer_candidate", "fractal_high_candidate", "LH_candidate", "short_watch_candidate", "short_condition_satisfied_candidate"},
    },
    "sweep_reclaim_candidate": {
        "long": {"equal_low_candidate", "fractal_low_candidate", "responsive_buyer_candidate", "trapped_seller_candidate", "long_watch_candidate"},
        "short": {"equal_high_candidate", "fractal_high_candidate", "responsive_seller_candidate", "trapped_buyer_candidate", "short_watch_candidate"},
    },
    "breakout_continuation_candidate": {
        "long": {"BOS_candidate:up", "MSB_candidate:up", "initiative_buyer_candidate", "momentum_candidate:buy", "momentum_candidate:up", "long_condition_satisfied_candidate"},
        "short": {"BOS_candidate:down", "MSB_candidate:down", "initiative_seller_candidate", "momentum_candidate:sell", "momentum_candidate:down", "short_condition_satisfied_candidate"},
    },
    "pullback_mitigation_candidate": {
        "long": {"order_block_candidate:buy", "imbalance_candidate:buy", "responsive_buyer_candidate", "initiative_buyer_candidate", "long_watch_candidate"},
        "short": {"order_block_candidate:sell", "imbalance_candidate:sell", "responsive_seller_candidate", "initiative_seller_candidate", "short_watch_candidate"},
    },
    "trap_reversal_candidate": {
        "long": {"trapped_seller_candidate", "responsive_buyer_candidate", "absorption_candidate:buy", "CHoCH_candidate:up", "HL_candidate", "long_condition_satisfied_candidate"},
        "short": {"trapped_buyer_candidate", "responsive_seller_candidate", "absorption_candidate:sell", "CHoCH_candidate:down", "LH_candidate", "short_condition_satisfied_candidate"},
    },
}


class SetupEngine:
    def __init__(self) -> None:
        registry_errors = validate_setup_contracts()
        self.registry_validation_passed = not registry_errors
        if registry_errors:
            raise RuntimeError("Setup registry validation failed: " + "; ".join(registry_errors))
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.buckets: dict[BucketKey, dict[str, Any]] = {}
        self.profile_refs: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        self.written_keys = load_written_keys()
        self.setup_index: dict[tuple[str, str], dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        self.input_rows_processed = {name: 0 for name in INPUT_FILES if name != "data_quality"}
        self.setup_candidates_written = 0
        self.last_window_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_heartbeat = time.monotonic()
        self.output_handle = CANDIDATES_FILE.open("a", encoding="utf-8")
        self.restore_setup_index()
        self.refresh_missing_inputs()
        self.write_health()

    def close(self) -> None:
        self.write_health()
        self.output_handle.close()

    def restore_setup_index(self) -> None:
        for row in read_jsonl(CANDIDATES_FILE):
            symbol = row.get("symbol"); timeframe = row.get("timeframe"); side = row.get("side")
            setup_name = row.get("setup_name")
            if symbol and timeframe and side in ("long", "short") and setup_name != "premium_alignment_candidate":
                self.setup_index[(str(symbol), str(side))][str(timeframe)].add(str(setup_name))

    def refresh_missing_inputs(self) -> None:
        for name, path in INPUT_FILES.items():
            label = relative_label(path)
            if path.exists(): self.missing_inputs.discard(label)
            else: self.missing_inputs.add(label)

    def process_row(self, source: str, row: dict[str, Any], evaluate: bool = True) -> None:
        if source in self.input_rows_processed:
            self.input_rows_processed[source] += 1
        normalized = normalize_input(source, row)
        if normalized is None:
            if source != "data_quality":
                self.warnings.add(f"invalid_input:{source}")
            return
        if source == "calibration_profiles":
            self.index_profiles(normalized)
            return
        key = (normalized["symbol"], normalized["timeframe"], normalized["window_start_ts"])
        if normalized["timeframe"] not in ALLOWED_TIMEFRAMES:
            self.warnings.add(f"unsupported_timeframe:{normalized['timeframe']}")
            return
        bucket = self.buckets.setdefault(key, new_bucket(normalized))
        merge_bucket(bucket, normalized)
        self.last_window_ts = max(self.last_window_ts, normalized["window_start_ts"])
        if evaluate and source not in ("context_dna", "smart_money_dna", "historical_outcome_observations", "data_quality"):
            self.evaluate_bucket(key)

    def index_profiles(self, normalized: dict[str, Any]) -> None:
        for profile in normalized.get("profiles", []):
            symbol = profile.get("symbol"); timeframe = profile.get("timeframe")
            if symbol and timeframe:
                append_unique(self.profile_refs[(str(symbol), str(timeframe))], profile)

    def evaluate_all(self) -> None:
        for key in sorted(self.buckets, key=lambda value: (value[2], value[1], value[0])):
            self.evaluate_bucket(key)

    def evaluate_bucket(self, key: BucketKey) -> None:
        bucket = self.buckets[key]
        tokens = bucket["tokens"]
        for setup_name, side_map in SIDE_COMPONENTS.items():
            for side, required_tokens in side_map.items():
                matches = sorted(tokens & required_tokens)
                if not matches:
                    continue
                self.emit_candidate(setup_name, side, bucket, matches)

    def emit_candidate(self, setup_name: str, side: str, bucket: dict[str, Any], matches: list[str]) -> None:
        contract = get_setup_contract(setup_name)
        if contract is None:
            self.warnings.add(f"setup_contract_not_found:{setup_name}")
            return
        key: DuplicateKey = (setup_name, bucket["symbol"], bucket["timeframe"], bucket["window_start_ts"], side)
        if key in self.written_keys:
            return
        blocking = bucket["sell_events"] if side == "long" else bucket["buy_events"]
        supporting = [item for item in bucket["events"] if event_tokens(item) & set(matches)]
        matched_event_types = {token.split(":", 1)[0] for token in matches}
        historical_refs = [
            ref for ref in bucket["historical_refs"]
            if ref.get("event_type") in matched_event_types
        ]
        historical_refs.extend(
            ref for ref in self.profile_refs.get((bucket["symbol"], bucket["timeframe"]), [])
            if ref.get("event_type") in matched_event_types
        )
        historical_refs = aggregate_historical_refs(historical_refs)
        payload = {
            "layer": "Layer-8", "engine": "SetupEngine", "record_type": "setup_candidate",
            "setup_id": make_setup_id(key), "setup_name": setup_name, "setup_family": contract["setup_family"],
            "symbol": bucket["symbol"], "timeframe": bucket["timeframe"],
            "window_start_ts": bucket["window_start_ts"], "window_end_ts": bucket["window_end_ts"],
            "side": side, "setup_status": "candidate_not_trade_signal",
            "supporting_evidence": supporting, "blocking_evidence": list(blocking),
            "observer_refs": list(bucket["observer_refs"]), "structure_refs": list(bucket["structure_refs"]),
            "context_refs": list(bucket["context_refs"]), "historical_outcome_refs": unique_values(historical_refs),
            "calibration_status": "uncalibrated", "scores": dict(NULL_SCORES),
            "execution_readiness": {
                "ready_for_execution_plan": False,
                "reason": "blocking_evidence_present" if blocking else "setup_uncalibrated",
            },
            "risk_readiness": {"ready_for_position_sizing": False, "reason": "no_execution_plan"},
            "validation": {"contract_found": True, "invariants_passed": True, "errors": []},
        }
        self.output_handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.output_handle.flush()
        self.written_keys.add(key)
        self.setup_candidates_written += 1
        self.setup_index[(bucket["symbol"], side)][bucket["timeframe"]].add(setup_name)
        if setup_name != "premium_alignment_candidate":
            self.evaluate_alignment(bucket, side)

    def evaluate_alignment(self, bucket: dict[str, Any], side: str) -> None:
        timeframe_map = self.setup_index[(bucket["symbol"], side)]
        distinct_timeframes = sorted(timeframe for timeframe, names in timeframe_map.items() if names)
        if len(distinct_timeframes) < 2:
            return
        matches = [f"setup_candidate:{timeframe}" for timeframe in distinct_timeframes]
        self.emit_candidate("premium_alignment_candidate", side, bucket, matches)

    def tick(self) -> None:
        if time.monotonic() - self.last_heartbeat >= HEARTBEAT_SECONDS:
            self.write_health(); self.heartbeat(); self.last_heartbeat = time.monotonic()

    def write_health(self) -> None:
        self.refresh_missing_inputs()
        payload = {
            "status": "alive", "input_rows_processed": dict(self.input_rows_processed),
            "setup_candidates_written": self.setup_candidates_written,
            "last_window_ts": self.last_window_ts, "missing_inputs": sorted(self.missing_inputs),
            "warnings": sorted(self.warnings), "registry_validation_passed": self.registry_validation_passed,
        }
        HEALTH_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def heartbeat(self) -> None:
        print("Setup Engine alive", flush=True)
        print(f"observer_events processed={self.input_rows_processed['observer_events']}", flush=True)
        print(f"evidence_packets processed={self.input_rows_processed['evidence_packets']}", flush=True)
        print(f"structure_events processed={self.input_rows_processed['structure_events']}", flush=True)
        print(f"setup_candidates_written={self.setup_candidates_written}", flush=True)
        print(f"last_window_ts={self.last_window_ts}", flush=True)


def new_bucket(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": row["symbol"], "timeframe": row["timeframe"], "window_start_ts": row["window_start_ts"],
        "window_end_ts": row.get("window_end_ts"), "tokens": set(), "events": [],
        "buy_events": [], "sell_events": [], "observer_refs": [], "structure_refs": [],
        "context_refs": [], "historical_refs": [],
    }


def merge_bucket(bucket: dict[str, Any], row: dict[str, Any]) -> None:
    if row.get("window_end_ts") is not None: bucket["window_end_ts"] = row["window_end_ts"]
    for event in row.get("events", []):
        append_unique(bucket["events"], event)
        bucket["tokens"].update(event_tokens(event))
        if event.get("side") == "buy": append_unique(bucket["buy_events"], event)
        elif event.get("side") == "sell": append_unique(bucket["sell_events"], event)
    for field in ("observer_refs", "structure_refs", "context_refs", "historical_refs"):
        for value in row.get(field, []): append_unique(bucket[field], value)


def normalize_input(source: str, row: dict[str, Any]) -> dict[str, Any] | None:
    if source == "calibration_profiles":
        groups = row.get("groups", []) if isinstance(row, dict) else []
        return {"profiles": [profile_ref(group) for group in groups if isinstance(group, dict)]}
    timestamp_field = "source_window_ts" if source == "context_dna" else "window_start_ts"
    if source == "historical_outcome_observations": timestamp_field = "event_window_start_ts"
    symbol = row.get("symbol"); timeframe = row.get("timeframe"); timestamp = safe_int(row.get(timestamp_field))
    if not symbol or not timeframe or timestamp is None: return None
    end_field = "source_window_end_ts" if source == "context_dna" else "event_window_end_ts" if source == "historical_outcome_observations" else "window_end_ts"
    base = {"symbol": str(symbol), "timeframe": str(timeframe), "window_start_ts": timestamp,
            "window_end_ts": safe_int(row.get(end_field)),
            "events": [], "observer_refs": [], "structure_refs": [], "context_refs": [], "historical_refs": []}
    if source == "evidence_packets":
        summary = row.get("evidence_summary") if isinstance(row.get("evidence_summary"), dict) else {}
        source_events = row.get("evidence_events") if isinstance(row.get("evidence_events"), list) else []
        if source_events:
            for item in source_events:
                if isinstance(item, dict): base["events"].append(normalized_event("evidence", item, timestamp))
        else:
            for side, field in (("buy", "buy_side_events"), ("sell", "sell_side_events"), ("neutral", "neutral_events"), ("unknown", "unknown_side_events")):
                for value in summary.get(field, []): base["events"].append({"source": "evidence", "event_type": value, "side": side, "direction": "unknown", "event_id": None, "window_start_ts": timestamp})
        return base
    if source in ("observer_events", "observer_states"):
        side = {"long": "buy", "short": "sell"}.get(str(row.get("watch_side")), "neutral" if row.get("watch_side") == "neutral" else "unknown")
        event_type = row.get("event_type") or row.get("last_event_type")
        event = {"source": "observer", "event_type": event_type, "side": side, "direction": "unknown", "event_id": row.get("event_id"), "window_start_ts": timestamp}
        if event_type: base["events"].append(event)
        for item in row.get("supporting_events", []):
            if isinstance(item, dict): base["events"].append(normalized_event("observer_support", item, timestamp))
        for item in row.get("opposing_events", []):
            if isinstance(item, dict): base["events"].append(normalized_event("observer_opposition", item, timestamp))
        base["observer_refs"].append({"event_id": row.get("event_id"), "event_type": event_type, "watch_side": row.get("watch_side"), "watch_status": row.get("watch_status"), "window_start_ts": timestamp})
        return base
    if source in ("structure_events", "detector_events"):
        event = normalized_event("smart_money" if source == "structure_events" else "detector", row, timestamp)
        base["events"].append(event)
        if source == "structure_events": base["structure_refs"].append({"event_id": row.get("event_id"), "event_type": row.get("event_type"), "window_start_ts": timestamp})
        return base
    if source == "context_dna":
        base["context_refs"].append({"window_start_ts": timestamp, "context": row.get("context", {})})
        return base
    if source == "smart_money_dna":
        swing = row.get("swing_state") if isinstance(row.get("swing_state"), dict) else {}
        bias = swing.get("structure_bias")
        if bias: base["events"].append({"source": "smart_money_snapshot", "event_type": "structure_bias", "side": "buy" if bias == "up" else "sell" if bias == "down" else "neutral", "direction": bias, "event_id": None, "window_start_ts": timestamp})
        return base
    if source == "historical_outcome_observations":
        base["historical_refs"].append({"observation_id": row.get("observation_id"), "pattern_signature": row.get("pattern_signature"), "pattern_key": row.get("pattern_key"), "event_type": row.get("event_type"), "window_start_ts": timestamp})
        return base
    return base


def normalized_event(source: str, row: dict[str, Any], timestamp: int) -> dict[str, Any]:
    return {"source": source, "event_type": row.get("event_type"), "side": row.get("side", "unknown"),
            "direction": row.get("direction", "unknown"), "event_id": row.get("event_id") or row.get("detector_event_id"),
            "window_start_ts": timestamp}


def event_tokens(event: dict[str, Any]) -> set[str]:
    event_type = event.get("event_type")
    if not event_type: return set()
    tokens = {str(event_type)}
    side = event.get("side"); direction = event.get("direction")
    if side in ("buy", "sell"): tokens.add(f"{event_type}:{side}")
    if direction in ("up", "down"): tokens.add(f"{event_type}:{direction}")
    return tokens


def profile_ref(group: dict[str, Any]) -> dict[str, Any]:
    return {field: group.get(field) for field in ("symbol", "timeframe", "pattern_signature", "pattern_key", "event_type", "side", "direction", "sample_count", "sample_status", "horizons")}


def aggregate_historical_refs(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, set[str]] = defaultdict(set)
    for value in values:
        event_type = str(value.get("event_type") or "unknown")
        signature = value.get("pattern_signature")
        if signature:
            grouped[event_type].add(str(signature))
    return [
        {"event_type": event_type, "pattern_signatures": sorted(signatures)}
        for event_type, signatures in sorted(grouped.items())
    ]


def make_setup_id(key: DuplicateKey) -> str:
    return "setup_" + hashlib.sha256("|".join(str(value) for value in key).encode()).hexdigest()[:24]


def load_written_keys() -> set[DuplicateKey]:
    keys: set[DuplicateKey] = set()
    for row in read_jsonl(CANDIDATES_FILE):
        timestamp = safe_int(row.get("window_start_ts"))
        if timestamp is not None:
            keys.add((str(row.get("setup_name")), str(row.get("symbol")), str(row.get("timeframe")), timestamp, str(row.get("side"))))
    return keys


def safe_int(value: Any) -> int | None:
    try: return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError): return None


def append_unique(values: list[Any], value: Any) -> None:
    if value not in values: values.append(value)


def unique_values(values: list[Any]) -> list[Any]:
    result = []
    for value in values: append_unique(result, value)
    return result


def relative_label(path: Path) -> str:
    return str(path.relative_to(ROOT_DIR)).replace("\\", "/")


def read_jsonl(path: Path):
    if not path.exists(): return
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try: row = json.loads(line)
            except json.JSONDecodeError: continue
            if isinstance(row, dict): yield row


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists(): return None
    try: row = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError): return None
    return row if isinstance(row, dict) else None


def run() -> None:
    engine = SetupEngine()
    handles: dict[str, Any] = {name: None for name, path in INPUT_FILES.items() if path.suffix == ".jsonl"}
    try:
        # Index static profile summaries and all JSONL backlog before matching.
        profile = load_json(INPUT_FILES["calibration_profiles"])
        if profile is not None: engine.process_row("calibration_profiles", profile, evaluate=False)
        for source, path in INPUT_FILES.items():
            if path.suffix != ".jsonl" or not path.exists(): continue
            handles[source] = path.open("r", encoding="utf-8", errors="replace")
            for line in handles[source]:
                try: row = json.loads(line)
                except json.JSONDecodeError: engine.warnings.add(f"invalid_json:{source}"); continue
                if isinstance(row, dict): engine.process_row(source, row, evaluate=False)
        engine.evaluate_all(); engine.write_health()
        profile_mtime = INPUT_FILES["calibration_profiles"].stat().st_mtime if INPUT_FILES["calibration_profiles"].exists() else None
        while True:
            activity = 0
            profile_path = INPUT_FILES["calibration_profiles"]
            current_mtime = profile_path.stat().st_mtime if profile_path.exists() else None
            if current_mtime is not None and current_mtime != profile_mtime:
                profile = load_json(profile_path)
                if profile is not None:
                    engine.process_row("calibration_profiles", profile, evaluate=False)
                profile_mtime = current_mtime; activity += 1
            for source, path in INPUT_FILES.items():
                if path.suffix != ".jsonl": continue
                if handles[source] is None:
                    if not path.exists(): continue
                    handles[source] = path.open("r", encoding="utf-8", errors="replace")
                while True:
                    line = handles[source].readline()
                    if not line: break
                    try: row = json.loads(line)
                    except json.JSONDecodeError: engine.warnings.add(f"invalid_json:{source}"); continue
                    if isinstance(row, dict): engine.process_row(source, row, evaluate=True); activity += 1
            engine.tick()
            if activity == 0: time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in handles.values():
            if handle is not None: handle.close()
        engine.close()


if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: print("Stopped.", flush=True)
