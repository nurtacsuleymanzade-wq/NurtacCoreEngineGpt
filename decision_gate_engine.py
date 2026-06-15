"""Layer-11 non-executing Decision Gate candidate engine."""

import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from decision_gate_contracts import DECISION_GATE_CONTRACTS, get_decision_gate_contract


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
INPUT_FILES = {
    "probability_candidates": DATA_DIR / "probability_candidates.jsonl",
    "setup_candidates": DATA_DIR / "setup_candidates.jsonl",
    "execution_plan_candidates": DATA_DIR / "execution_plan_candidates.jsonl",
    "evidence_packets": DATA_DIR / "evidence_packets.jsonl",
    "volume_profile_dna": DATA_DIR / "volume_profile_dna.jsonl",
    "volume_profile_events": DATA_DIR / "volume_profile_events.jsonl",
    "structure_events": DATA_DIR / "structure_events.jsonl",
    "context_dna": DATA_DIR / "context_dna.jsonl",
    "historical_outcome_observations": DATA_DIR / "historical_outcome_observations.jsonl",
}
CALIBRATION_FILE = DATA_DIR / "calibration_profiles.json"
PRIMARY_INPUTS = {"probability_candidates", "setup_candidates", "execution_plan_candidates"}
EVENTS_FILE = DATA_DIR / "decision_gate_events.jsonl"
HEALTH_FILE = DATA_DIR / "decision_gate_health.json"
ERRORS_FILE = DATA_DIR / "decision_gate_errors.jsonl"
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0


class DecisionGateEngine:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.registry_validation_passed = validate_registry()
        self.buckets: dict[tuple[str, str, int], dict[str, list[dict[str, Any]]]] = defaultdict(new_bucket)
        self.written_keys = load_written_keys()
        self.input_rows_processed = {name: 0 for name in INPUT_FILES}
        self.input_rows_processed["calibration_profiles"] = 0
        self.decision_events_written = 0
        self.last_window_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_heartbeat = time.monotonic()
        self.calibration_mtime: float | None = None
        self.output_handle = EVENTS_FILE.open("a", encoding="utf-8")
        self.error_handle = ERRORS_FILE.open("a", encoding="utf-8")
        self.refresh_missing_inputs()
        self.reload_calibration_profiles()
        self.write_health()

    def refresh_missing_inputs(self) -> None:
        paths = {**INPUT_FILES, "calibration_profiles": CALIBRATION_FILE}
        for name, path in paths.items():
            label = relative_label(path)
            if path.exists():
                self.missing_inputs.discard(label)
                self.warnings.discard(f"optional_input_missing:{label}")
            elif name in PRIMARY_INPUTS:
                self.missing_inputs.add(label)
            else:
                self.warnings.add(f"optional_input_missing:{label}")

    def reload_calibration_profiles(self) -> None:
        if not CALIBRATION_FILE.exists():
            return
        try:
            payload = json.loads(CALIBRATION_FILE.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not isinstance(payload.get("profiles", []), list):
                raise ValueError("profiles_not_list")
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            self.write_error("calibration_profiles", f"profile_parse_error:{exc}")
            return
        self.input_rows_processed["calibration_profiles"] += 1
        self.calibration_mtime = CALIBRATION_FILE.stat().st_mtime

    def process_line(self, source: str, line: str) -> None:
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            self.write_error(source, f"json_parse_error:{exc}")
            return
        if not isinstance(row, dict):
            self.write_error(source, "row_not_object")
            return
        self.input_rows_processed[source] += 1
        normalized = normalize(source, row)
        if normalized is None:
            self.write_error(source, "window_start_ts_missing_or_invalid")
            return
        self.last_window_ts = max(self.last_window_ts, normalized["window_start_ts"])
        if source == "historical_outcome_observations":
            return
        key = bucket_key(normalized)
        self.buckets[key][source].append(normalized)
        self.evaluate_bucket(key)

    def evaluate_bucket(self, key: tuple[str, str, int]) -> None:
        bucket = self.buckets[key]
        setups = bucket["setup_candidates"]
        probabilities = bucket["probability_candidates"]
        plans = bucket["execution_plan_candidates"]
        evidence = bucket["evidence_packets"]
        data_quality_ok, quality_reasons = data_quality_status(evidence)

        sides = {item["side"] for item in setups + probabilities + plans}
        if not sides:
            return
        for side in sides:
            side_setups = matching_side(setups, side)
            side_probabilities = matching_side(probabilities, side)
            side_plans = matching_side(plans, side)
            if not (side_setups or side_probabilities or side_plans):
                continue
            setup = side_setups[0] if side_setups else None
            probability = side_probabilities[0] if side_probabilities else None
            plan = side_plans[0] if side_plans else None
            sample_status = (
                probability.get("outcome_profile", {}).get("sample_status")
                if probability else None
            )
            insufficient = sample_status == "insufficient_data"
            contradiction, contradiction_reasons = contradiction_status(setup, probability, evidence)
            risk_review = bool(
                plan and plan.get("risk_readiness", {}).get("ready_for_risk_engine") is False
            )
            invalid_candidate = any(
                item.get("validation", {}).get("invariants_passed") is False
                for item in (setup, probability, plan) if item
            )
            context = {
                "symbol": key[0], "timeframe": key[1], "window_start_ts": key[2],
                "window_end_ts": latest_end(setup, probability, plan), "side": side,
                "setup": setup, "probability": probability, "plan": plan,
                "evidence": evidence, "bucket": bucket,
                "sample_status": sample_status, "insufficient": insufficient,
                "contradiction": contradiction, "contradiction_reasons": contradiction_reasons,
                "data_quality_ok": data_quality_ok, "quality_reasons": quality_reasons,
                "risk_review": risk_review,
            }

            if setup and probability and not plan:
                self.emit("execution_plan_required_candidate", context, ["execution_plan_candidate_missing"])
            if setup and not probability:
                self.emit("wait_for_confirmation_candidate", context, ["probability_candidate_missing"])
            if insufficient:
                self.emit("insufficient_data_reject_candidate", context, ["sample_status_insufficient_data"])
            if contradiction:
                self.emit("contradiction_reject_candidate", context, contradiction_reasons)
            if not data_quality_ok:
                self.emit("data_quality_reject_candidate", context, quality_reasons)
            if risk_review:
                self.emit("risk_reward_review_candidate", context, ["risk_engine_not_ready"])
            if invalid_candidate:
                self.emit("reject_trade_candidate", context, ["candidate_validation_failed"])

            can_allow = bool(
                setup and probability and plan and not insufficient and not contradiction
                and data_quality_ok and not invalid_candidate
            )
            if can_allow:
                self.emit("allow_paper_trade_candidate", context, ["paper_trade_dependencies_available"])
            elif probability and not setup and not plan:
                self.emit("manual_review_candidate", context, ["probability_without_setup_or_execution_plan"])
            elif plan and not setup:
                self.emit("manual_review_candidate", context, ["execution_plan_without_setup"])

    def emit(self, decision_name: str, context: dict[str, Any], reasons: list[str]) -> None:
        contract = get_decision_gate_contract(decision_name)
        if contract is None:
            self.warnings.add(f"contract_missing:{decision_name}")
            return
        key = duplicate_key(decision_name, context)
        if key in self.written_keys:
            return
        decision = decision_value(decision_name)
        allow_paper = decision_name == "allow_paper_trade_candidate"
        setup = context["setup"]
        probability = context["probability"]
        plan = context["plan"]
        payload = {
            "layer": "Layer-11", "engine": "DecisionGate", "record_type": "decision_gate_event",
            "decision_id": make_decision_id(key), "decision_name": decision_name,
            "decision_family": contract["decision_family"], "symbol": context["symbol"],
            "timeframe": context["timeframe"], "window_start_ts": context["window_start_ts"],
            "window_end_ts": context["window_end_ts"], "side": context["side"],
            "decision": decision, "reason": {"codes": reasons},
            "probability_refs": references(probability, "probability_id"),
            "setup_refs": references(setup, "setup_id"),
            "execution_plan_refs": references(plan, "plan_id"),
            "evidence_refs": collect_refs(context["evidence"], "event_id"),
            "structure_refs": collect_refs(context["bucket"]["structure_events"], "event_id"),
            "volume_profile_refs": collect_refs(context["bucket"]["volume_profile_events"], "event_id"),
            "context_refs": collect_refs(context["bucket"]["context_dna"], "context_id"),
            "calibration_refs": list_value(probability.get("calibration_refs")) if probability else [],
            "gate_checks": {
                "probability_available": probability is not None,
                "setup_available": setup is not None,
                "execution_plan_available": plan is not None,
                "historical_sample_available": bool(probability and not context["insufficient"]),
                "data_quality_ok": context["data_quality_ok"],
                "contradiction_detected": context["contradiction"],
                "risk_reward_review_required": context["risk_review"],
            },
            "scores": {"confidence": None, "strength_score": None, "decision_score": None, "threshold": None},
            "order_readiness": {"ready_for_order": False, "reason": "decision_gate_does_not_execute"},
            "paper_trade_readiness": {
                "ready_for_paper_trade": allow_paper,
                "reason": "paper_trade_candidate_only" if allow_paper else "decision_does_not_allow_paper_trade",
            },
            "validation": {"contract_found": True, "invariants_passed": True, "errors": []},
        }
        self.output_handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.output_handle.flush()
        self.written_keys.add(key)
        self.decision_events_written += 1

    def write_error(self, source: str, detail: str) -> None:
        payload = {"engine": "DecisionGate", "source": source, "detail": detail}
        self.error_handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.error_handle.flush()
        self.warnings.add(f"error:{source}")

    def tick(self) -> None:
        if CALIBRATION_FILE.exists() and CALIBRATION_FILE.stat().st_mtime != self.calibration_mtime:
            self.reload_calibration_profiles()
        if time.monotonic() - self.last_heartbeat < HEARTBEAT_SECONDS:
            return
        self.refresh_missing_inputs()
        self.write_health()
        print("Decision Gate alive", flush=True)
        print(f"probability_candidates processed={self.input_rows_processed['probability_candidates']}", flush=True)
        print(f"setup_candidates processed={self.input_rows_processed['setup_candidates']}", flush=True)
        print(f"execution_plan_candidates processed={self.input_rows_processed['execution_plan_candidates']}", flush=True)
        print(f"decision_events_written={self.decision_events_written}", flush=True)
        print(f"last_window_ts={self.last_window_ts}", flush=True)
        self.last_heartbeat = time.monotonic()

    def write_health(self) -> None:
        payload = {
            "status": "alive", "input_rows_processed": self.input_rows_processed,
            "decision_events_written": self.decision_events_written,
            "last_window_ts": self.last_window_ts,
            "missing_inputs": sorted(self.missing_inputs), "warnings": sorted(self.warnings),
            "registry_validation_passed": self.registry_validation_passed,
        }
        HEALTH_FILE.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def close(self) -> None:
        self.write_health()
        self.output_handle.close()
        self.error_handle.close()


def validate_registry() -> bool:
    names = {item.get("decision_name") for item in DECISION_GATE_CONTRACTS if isinstance(item, dict)}
    return len(DECISION_GATE_CONTRACTS) == 9 and len(names) == 9


def new_bucket() -> dict[str, list[dict[str, Any]]]:
    return {name: [] for name in INPUT_FILES}


def normalize(source: str, row: dict[str, Any]) -> dict[str, Any] | None:
    ts = safe_int(row.get("window_start_ts", row.get("source_window_ts", row.get("event_ts"))))
    if ts is None:
        return None
    base = {
        "source": source, "symbol": str(row.get("symbol") or "BTCUSDT"),
        "timeframe": str(row.get("timeframe") or "unknown"), "window_start_ts": ts,
        "window_end_ts": safe_int(row.get("window_end_ts")), "side": canonical_side(row.get("side")),
        "validation": dict_value(row.get("validation")), "raw": {},
    }
    if source == "probability_candidates":
        base.update({
            "probability_id": str(row.get("probability_id") or ""),
            "probability_name": str(row.get("probability_name") or ""),
            "probability": dict_value(row.get("probability")),
            "outcome_profile": {
                "sample_status": dict_value(row.get("outcome_profile")).get("sample_status")
            },
            "contradiction_context": {
                "has_contradiction": dict_value(row.get("contradiction_context")).get("has_contradiction") is True,
                "contradicting_evidence": list_value(dict_value(row.get("contradiction_context")).get("contradicting_evidence")),
            },
            "calibration_refs": list_value(row.get("calibration_refs")),
        })
    elif source == "setup_candidates":
        base.update({
            "setup_id": str(row.get("setup_id") or ""), "setup_name": str(row.get("setup_name") or ""),
            "setup_family": str(row.get("setup_family") or ""),
            "setup_status": str(row.get("setup_status") or "candidate_not_trade_signal"),
            "supporting_evidence": list_value(row.get("supporting_evidence")),
            "blocking_evidence": list_value(row.get("blocking_evidence")),
        })
    elif source == "execution_plan_candidates":
        base.update({
            "plan_id": str(row.get("plan_id") or ""), "plan_name": str(row.get("plan_name") or ""),
            "entry": dict_value(row.get("entry")), "stop_loss": dict_value(row.get("stop_loss")),
            "take_profit": dict_value(row.get("take_profit")), "invalidation": dict_value(row.get("invalidation")),
            "order_readiness": dict_value(row.get("order_readiness")),
            "risk_readiness": dict_value(row.get("risk_readiness")),
        })
    elif source == "evidence_packets":
        summary = dict_value(row.get("evidence_summary"))
        base.update({
            "event_id": str(row.get("event_id") or f"evidence:{base['timeframe']}:{ts}"),
            "event_types": list_value(row.get("event_types")) or list_value(summary.get("event_types")),
            "buy_side_events": list_value(row.get("buy_side_events")) or list_value(summary.get("buy_side_events")),
            "sell_side_events": list_value(row.get("sell_side_events")) or list_value(summary.get("sell_side_events")),
            "neutral_events": list_value(row.get("neutral_events")) or list_value(summary.get("neutral_events")),
            "unknown_side_events": list_value(row.get("unknown_side_events")) or list_value(summary.get("unknown_side_events")),
            "data_quality": dict_value(row.get("data_quality")),
        })
    else:
        base.update({
            "event_id": str(row.get("event_id") or row.get("observation_id") or ""),
            "context_id": str(row.get("context_id") or ""),
            "data_quality": dict_value(row.get("data_quality")),
        })
    return base


def contradiction_status(
    setup: dict[str, Any] | None,
    probability: dict[str, Any] | None,
    evidence: list[dict[str, Any]],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if probability and probability.get("contradiction_context", {}).get("has_contradiction") is True:
        reasons.append("probability_contradiction")
    if setup and setup.get("blocking_evidence"):
        reasons.append("setup_blocking_evidence")
    if any(item.get("buy_side_events") and item.get("sell_side_events") for item in evidence):
        reasons.append("opposing_evidence_in_bucket")
    return bool(reasons), reasons


def data_quality_status(evidence: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    for item in evidence:
        quality = item.get("data_quality", {})
        for key, value in quality.items():
            key_text = str(key).lower()
            value_text = str(value).lower()
            if key_text in {"valid", "input_valid", "is_valid"} and value is False:
                reasons.append(f"{key_text}=false")
            elif any(marker in key_text or marker in value_text for marker in ("invalid", "gap", "missing")):
                if value not in (False, None, "", [], {}):
                    reasons.append(f"{key_text}:{value_text}")
    return not reasons, sorted(set(reasons))


def decision_value(name: str) -> str:
    return {
        "allow_paper_trade_candidate": "allow_paper_trade",
        "reject_trade_candidate": "reject",
        "wait_for_confirmation_candidate": "wait",
        "insufficient_data_reject_candidate": "wait",
        "contradiction_reject_candidate": "manual_review",
        "data_quality_reject_candidate": "reject",
        "risk_reward_review_candidate": "manual_review",
        "manual_review_candidate": "manual_review",
        "execution_plan_required_candidate": "execution_plan_required",
    }[name]


def matching_side(rows: list[dict[str, Any]], side: str) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("side") in {side, "neutral", "unknown"}]


def references(row: dict[str, Any] | None, field: str) -> list[str]:
    value = row.get(field) if row else None
    return [str(value)] if value else []


def collect_refs(rows: list[dict[str, Any]], field: str) -> list[str]:
    return [str(row[field]) for row in rows if row.get(field)]


def latest_end(*rows: dict[str, Any] | None) -> int | None:
    values = [row.get("window_end_ts") for row in rows if row and row.get("window_end_ts") is not None]
    return max(values) if values else None


def duplicate_key(name: str, context: dict[str, Any]) -> tuple[str, str, str, int, str]:
    return (name, context["symbol"], context["timeframe"], context["window_start_ts"], context["side"])


def make_decision_id(key: tuple[Any, ...]) -> str:
    return hashlib.sha256("".join(map(str, key)).encode()).hexdigest()


def load_written_keys() -> set[tuple[str, str, str, int, str]]:
    result: set[tuple[str, str, str, int, str]] = set()
    if not EVENTS_FILE.exists():
        return result
    with EVENTS_FILE.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = safe_int(row.get("window_start_ts")) if isinstance(row, dict) else None
            if ts is not None:
                result.add((
                    str(row.get("decision_name")), str(row.get("symbol")), str(row.get("timeframe")),
                    ts, canonical_side(row.get("side")),
                ))
    return result


def bucket_key(row: dict[str, Any]) -> tuple[str, str, int]:
    return (row["symbol"], row["timeframe"], row["window_start_ts"])


def canonical_side(value: Any) -> str:
    side = str(value or "unknown").lower()
    if side in {"buy", "long"}:
        return "long"
    if side in {"sell", "short"}:
        return "short"
    return side if side in {"neutral", "unknown"} else "unknown"


def safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def list_value(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def relative_label(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT_DIR)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def run() -> None:
    engine = DecisionGateEngine()
    handles: dict[str, Any] = {name: None for name in INPUT_FILES}
    try:
        while True:
            activity = 0
            for source, path in INPUT_FILES.items():
                if handles[source] is None:
                    if not path.exists():
                        continue
                    handles[source] = path.open("r", encoding="utf-8", errors="replace")
                while True:
                    line = handles[source].readline()
                    if not line:
                        break
                    engine.process_line(source, line)
                    activity += 1
                    engine.tick()
            engine.tick()
            if activity == 0:
                time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in handles.values():
            if handle:
                handle.close()
        engine.close()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
