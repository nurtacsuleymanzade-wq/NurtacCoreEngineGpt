"""Layer-7 uncalibrated watch-state observation engine.

This engine classifies continuing or opposing candidate evidence. It does not
produce setups, trade decisions, forecasts, scores, or numeric thresholds.
"""

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from observer_contracts import get_observer_contract, validate_observer_contracts


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
INPUT_FILES = {
    "evidence_packets": DATA_DIR / "evidence_packets.jsonl",
    "structure_events": DATA_DIR / "structure_events.jsonl",
    "detector_events": DATA_DIR / "detector_events.jsonl",
    "context_dna": DATA_DIR / "context_dna.jsonl",
    "smart_money_dna": DATA_DIR / "smart_money_dna.jsonl",
}
STATES_FILE = DATA_DIR / "observer_states.jsonl"
EVENTS_FILE = DATA_DIR / "observer_events.jsonl"
HEALTH_FILE = DATA_DIR / "observer_health.json"
POLL_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 10.0
VALID_SIDES = {"buy", "sell", "neutral", "unknown"}
OPEN_STATUSES = {"watching", "waiting_for_trigger", "condition_satisfied"}
NULL_SCORES = {
    "confidence": None,
    "strength_score": None,
    "observer_score": None,
    "threshold": None,
}

EventKey = tuple[str, str, str, int, str]
StateKey = tuple[str, str]


@dataclass
class WatchState:
    symbol: str
    timeframe: str
    current_watch_side: str = "unknown"
    current_watch_status: str = "watching"
    supporting_events: list[dict[str, Any]] = field(default_factory=list)
    opposing_events: list[dict[str, Any]] = field(default_factory=list)
    structure_refs: list[dict[str, Any]] = field(default_factory=list)
    detector_refs: list[dict[str, Any]] = field(default_factory=list)
    evidence_refs: list[dict[str, Any]] = field(default_factory=list)
    context_refs: list[dict[str, Any]] = field(default_factory=list)
    last_window_ts: int = 0
    last_window_end_ts: int | None = None
    last_event_type: str | None = None


class ObserverEngine:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        registry_errors = validate_observer_contracts()
        self.registry_validation_passed = not registry_errors
        if registry_errors:
            raise RuntimeError("Observer registry validation failed: " + "; ".join(registry_errors))

        self.written_event_keys = load_event_keys(EVENTS_FILE)
        self.written_state_keys = load_state_keys(STATES_FILE)
        self.state_by_key = load_runtime_states(STATES_FILE)
        self.restart_cutoffs = {
            key: state.last_window_ts for key, state in self.state_by_key.items()
        }
        self.context_by_key: dict[tuple[str, str, int], dict[str, Any]] = {}
        self.smart_snapshot_by_key: dict[tuple[str, str, int], dict[str, Any]] = {}
        self.input_rows_processed = {name: 0 for name in INPUT_FILES}
        self.observer_states_written = 0
        self.observer_events_written = 0
        self.last_window_ts = max(
            (state.last_window_ts for state in self.state_by_key.values()),
            default=0,
        )
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_heartbeat = time.monotonic()
        self.states_handle = STATES_FILE.open("a", encoding="utf-8")
        self.events_handle = EVENTS_FILE.open("a", encoding="utf-8")
        self.refresh_missing_inputs()
        self.write_health()

    def close(self) -> None:
        self.write_health()
        self.states_handle.close()
        self.events_handle.close()

    def refresh_missing_inputs(self) -> None:
        for path in INPUT_FILES.values():
            label = relative_label(path)
            if path.exists():
                self.missing_inputs.discard(label)
            else:
                self.missing_inputs.add(label)

    def process_row(self, source: str, row: dict[str, Any]) -> None:
        self.input_rows_processed[source] += 1
        normalized = normalize_input(source, row)
        if normalized is None:
            self.warnings.add(f"invalid_input:{source}")
            return
        timestamp = normalized["window_start_ts"]
        self.last_window_ts = max(self.last_window_ts, timestamp)
        index_key = (normalized["symbol"], normalized["timeframe"], timestamp)
        if source == "context_dna":
            self.context_by_key[index_key] = normalized
            return
        if source == "smart_money_dna":
            self.smart_snapshot_by_key[index_key] = normalized
            return

        key = (normalized["symbol"], normalized["timeframe"])
        state = self.state_by_key.get(key)
        restart_cutoff = self.restart_cutoffs.get(key)
        if restart_cutoff is not None and timestamp <= restart_cutoff:
            return
        if state is not None and timestamp < state.last_window_ts:
            return
        if state is None:
            state = WatchState(symbol=key[0], timeframe=key[1])
            self.state_by_key[key] = state
        self.apply_observation(state, normalized)

    def apply_observation(self, state: WatchState, observation: dict[str, Any]) -> None:
        has_buy = bool(observation["buy_events"])
        has_sell = bool(observation["sell_events"])
        has_non_directional = bool(observation["neutral_events"] or observation["unknown_events"])
        state.last_window_ts = observation["window_start_ts"]
        state.last_window_end_ts = observation.get("window_end_ts")
        self.attach_reference(state, observation)

        if (has_buy and has_sell) or (not has_buy and not has_sell and has_non_directional):
            state.current_watch_side = "neutral"
            state.current_watch_status = "watching"
            state.supporting_events = observation["all_events"]
            state.opposing_events = []
            self.emit(state, observation, "neutral_watch_candidate", "neutral", "watching")
            return
        if not has_buy and not has_sell:
            return

        observed_side = "long" if has_buy else "short"
        support = observation["buy_events"] if has_buy else observation["sell_events"]
        opposite = observation["sell_events"] if has_buy else observation["buy_events"]
        watch_is_open = state.current_watch_status in OPEN_STATUSES

        if watch_is_open and state.current_watch_side in {"long", "short"}:
            if state.current_watch_side == observed_side:
                state.supporting_events = list(support)
                state.opposing_events = list(opposite)
                event_type = f"{observed_side}_condition_satisfied_candidate"
                state.current_watch_status = "condition_satisfied"
                self.emit(state, observation, event_type, observed_side, "condition_satisfied")
                return
            state.opposing_events = list(support)
            prior_side = state.current_watch_side
            state.current_watch_status = "invalidated"
            self.emit(
                state,
                observation,
                "invalidation_candidate",
                prior_side,
                "invalidated",
                reason={"opposing_watch_side": observed_side},
            )
            return

        state.current_watch_side = observed_side
        state.current_watch_status = "watching"
        state.supporting_events = list(support)
        state.opposing_events = list(opposite)
        self.emit(state, observation, f"{observed_side}_watch_candidate", observed_side, "watching")
        state.current_watch_status = "waiting_for_trigger"
        self.emit(
            state,
            observation,
            "wait_for_trigger_candidate",
            observed_side,
            "waiting_for_trigger",
            reason={"condition_satisfied": False},
        )

    def attach_reference(self, state: WatchState, observation: dict[str, Any]) -> None:
        state.structure_refs = []
        state.detector_refs = []
        state.evidence_refs = []
        state.context_refs = []
        ref = observation["source_ref"]
        target = {
            "detector": state.detector_refs,
            "smart_money": state.structure_refs,
            "evidence": state.evidence_refs,
        }.get(observation["source"])
        if target is not None:
            append_unique(target, ref)
        lookup = (state.symbol, state.timeframe, observation["window_start_ts"])
        context = self.context_by_key.get(lookup)
        if context:
            append_unique(state.context_refs, context["source_ref"])

    def emit(
        self,
        state: WatchState,
        observation: dict[str, Any],
        event_type: str,
        watch_side: str,
        watch_status: str,
        reason: dict[str, Any] | None = None,
    ) -> None:
        timestamp = observation["window_start_ts"]
        event_key: EventKey = (event_type, state.symbol, state.timeframe, timestamp, watch_side)
        contract = get_observer_contract(event_type)
        errors = [] if contract else ["observer_contract_not_found"]
        if event_key not in self.written_event_keys:
            event = {
                "layer": "Layer-7",
                "engine": "ObserverEngine",
                "record_type": "observer_event",
                "event_id": make_event_id(event_key),
                "symbol": state.symbol,
                "timeframe": state.timeframe,
                "window_start_ts": timestamp,
                "window_end_ts": observation.get("window_end_ts"),
                "event_type": event_type,
                "watch_side": watch_side,
                "watch_status": watch_status,
                "calibration_status": "uncalibrated",
                "confidence": None,
                "strength_score": None,
                "thresholds": None,
                "reason": reason or {"observation_source": observation["source"]},
                "source_refs": {observation["source"]: observation["source_ref"]},
                "supporting_events": list(state.supporting_events),
                "opposing_events": list(state.opposing_events),
                "validation": {
                    "contract_found": contract is not None,
                    "invariants_passed": not errors,
                    "errors": errors,
                },
            }
            self.events_handle.write(json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n")
            self.events_handle.flush()
            self.written_event_keys.add(event_key)
            self.observer_events_written += 1
        state.last_event_type = event_type
        self.write_state(state, observation)

    def write_state(self, state: WatchState, observation: dict[str, Any]) -> None:
        key = (
            state.symbol,
            state.timeframe,
            state.last_window_ts,
            state.current_watch_side,
            state.current_watch_status,
            state.last_event_type,
        )
        if key in self.written_state_keys:
            return
        data_quality = observation.get("data_quality")
        if not isinstance(data_quality, dict) or not data_quality:
            data_quality = {"quality_state": "unknown", "warning": "source_data_quality_missing"}
        payload = {
            "layer": "Layer-7",
            "engine": "ObserverEngine",
            "record_type": "observer_state",
            "symbol": state.symbol,
            "timeframe": state.timeframe,
            "window_start_ts": state.last_window_ts,
            "window_end_ts": state.last_window_end_ts,
            "watch_side": state.current_watch_side,
            "watch_status": state.current_watch_status,
            "supporting_events": list(state.supporting_events),
            "opposing_events": list(state.opposing_events),
            "structure_refs": list(state.structure_refs),
            "detector_refs": list(state.detector_refs),
            "evidence_refs": list(state.evidence_refs),
            "context_refs": list(state.context_refs),
            "calibration_status": "uncalibrated",
            "scores": dict(NULL_SCORES),
            "decision_readiness": {
                "ready_for_setup": False,
                "reason": "observer_uncalibrated",
            },
            "data_quality": data_quality,
            "validation": {"input_valid": True, "errors": []},
            "last_event_type": state.last_event_type,
        }
        self.states_handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.states_handle.flush()
        self.written_state_keys.add(key)
        self.observer_states_written += 1

    def tick(self) -> None:
        if time.monotonic() - self.last_heartbeat >= HEARTBEAT_SECONDS:
            self.heartbeat()
            self.last_heartbeat = time.monotonic()

    def write_health(self) -> None:
        self.refresh_missing_inputs()
        payload = {
            "status": "alive",
            "input_rows_processed": dict(self.input_rows_processed),
            "observer_states_written": self.observer_states_written,
            "observer_events_written": self.observer_events_written,
            "open_watch_states": sum(
                state.current_watch_status in OPEN_STATUSES for state in self.state_by_key.values()
            ),
            "last_window_ts": self.last_window_ts,
            "missing_inputs": sorted(self.missing_inputs),
            "warnings": sorted(self.warnings),
            "registry_validation_passed": self.registry_validation_passed,
        }
        HEALTH_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def heartbeat(self) -> None:
        self.write_health()
        print("Observer Engine alive", flush=True)
        for source in INPUT_FILES:
            print(f"{source} processed={self.input_rows_processed[source]}", flush=True)
        print(f"observer_states_written={self.observer_states_written}", flush=True)
        print(f"observer_events_written={self.observer_events_written}", flush=True)
        print(
            "open_watch_states="
            f"{sum(state.current_watch_status in OPEN_STATUSES for state in self.state_by_key.values())}",
            flush=True,
        )
        print(f"last_window_ts={self.last_window_ts}", flush=True)


def normalize_input(source: str, row: dict[str, Any]) -> dict[str, Any] | None:
    symbol = row.get("symbol")
    timeframe = row.get("timeframe")
    timestamp_field = "source_window_ts" if source == "context_dna" else "window_start_ts"
    window_end_field = "source_window_end_ts" if source == "context_dna" else "window_end_ts"
    timestamp = safe_int(row.get(timestamp_field))
    if not symbol or not timeframe or timestamp is None:
        return None
    base = {
        "source": source_name(source),
        "symbol": str(symbol),
        "timeframe": str(timeframe),
        "window_start_ts": timestamp,
        "window_end_ts": safe_int(row.get(window_end_field)),
        "buy_events": [],
        "sell_events": [],
        "neutral_events": [],
        "unknown_events": [],
        "all_events": [],
        "data_quality": row.get("data_quality"),
    }
    if source == "context_dna":
        base["source_ref"] = {
            "source": "context",
            "window_start_ts": timestamp,
        }
        return base
    if source == "smart_money_dna":
        base["source_ref"] = {
            "source": "smart_money_snapshot",
            "window_start_ts": timestamp,
        }
        return base
    if source == "evidence_packets":
        summary = row.get("evidence_summary") if isinstance(row.get("evidence_summary"), dict) else {}
        for side, field_name in (
            ("buy", "buy_side_events"),
            ("sell", "sell_side_events"),
            ("neutral", "neutral_events"),
            ("unknown", "unknown_side_events"),
        ):
            values = summary.get(field_name, [])
            if not isinstance(values, list):
                values = []
            target = base[f"{side}_events"]
            for value in values:
                target.append(event_reference("evidence", value, side, timestamp))
        base["all_events"] = base["buy_events"] + base["sell_events"] + base["neutral_events"] + base["unknown_events"]
        base["source_ref"] = {
            "source": "evidence",
            "packet_version": row.get("packet_version"),
            "window_start_ts": timestamp,
            "event_types": summary.get("event_types", []),
        }
        return base

    side = str(row.get("side", "unknown"))
    if side not in VALID_SIDES:
        side = "unknown"
    event_id = row.get("event_id") or row.get("detector_event_id")
    event = {
        "source": base["source"],
        "event_type": row.get("event_type"),
        "event_id": event_id,
        "side": side,
        "direction": row.get("direction", "unknown"),
        "window_start_ts": timestamp,
    }
    base[f"{side}_events"].append(event)
    base["all_events"] = [event]
    base["source_ref"] = {
        "source": base["source"],
        "event_id": event_id,
        "event_type": row.get("event_type"),
        "window_start_ts": timestamp,
    }
    return base


def event_reference(source: str, value: Any, side: str, timestamp: int) -> dict[str, Any]:
    if isinstance(value, dict):
        return {
            "source": source,
            "event_type": value.get("event_type"),
            "event_id": value.get("event_id") or value.get("detector_event_id"),
            "side": value.get("side", side),
            "window_start_ts": timestamp,
        }
    return {
        "source": source,
        "event_type": value,
        "event_id": None,
        "side": side,
        "window_start_ts": timestamp,
    }


def source_name(source: str) -> str:
    return {
        "detector_events": "detector",
        "structure_events": "smart_money",
        "evidence_packets": "evidence",
        "context_dna": "context",
        "smart_money_dna": "smart_money_snapshot",
    }[source]


def make_event_id(key: EventKey) -> str:
    raw = "|".join(str(value) for value in key)
    return "observer_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def append_unique(values: list[dict[str, Any]], value: dict[str, Any]) -> None:
    if value not in values:
        values.append(value)


def relative_label(path: Path) -> str:
    return str(path.relative_to(ROOT_DIR)).replace("\\", "/")


def read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def load_event_keys(path: Path) -> set[EventKey]:
    keys: set[EventKey] = set()
    for row in read_jsonl(path):
        timestamp = safe_int(row.get("window_start_ts"))
        if timestamp is None:
            continue
        keys.add((
            str(row.get("event_type")),
            str(row.get("symbol")),
            str(row.get("timeframe")),
            timestamp,
            str(row.get("watch_side")),
        ))
    return keys


def load_state_keys(path: Path) -> set[tuple[Any, ...]]:
    keys: set[tuple[Any, ...]] = set()
    for row in read_jsonl(path):
        timestamp = safe_int(row.get("window_start_ts"))
        if timestamp is None:
            continue
        keys.add((
            str(row.get("symbol")),
            str(row.get("timeframe")),
            timestamp,
            str(row.get("watch_side")),
            str(row.get("watch_status")),
            row.get("last_event_type"),
        ))
    return keys


def load_runtime_states(path: Path) -> dict[StateKey, WatchState]:
    states: dict[StateKey, WatchState] = {}
    for row in read_jsonl(path):
        symbol = row.get("symbol")
        timeframe = row.get("timeframe")
        timestamp = safe_int(row.get("window_start_ts"))
        if not symbol or not timeframe or timestamp is None:
            continue
        key = (str(symbol), str(timeframe))
        previous = states.get(key)
        if previous is not None and previous.last_window_ts > timestamp:
            continue
        states[key] = WatchState(
            symbol=key[0],
            timeframe=key[1],
            current_watch_side=str(row.get("watch_side", "unknown")),
            current_watch_status=str(row.get("watch_status", "watching")),
            supporting_events=list(row.get("supporting_events", [])),
            opposing_events=list(row.get("opposing_events", [])),
            structure_refs=list(row.get("structure_refs", [])),
            detector_refs=list(row.get("detector_refs", [])),
            evidence_refs=list(row.get("evidence_refs", [])),
            context_refs=list(row.get("context_refs", [])),
            last_window_ts=timestamp,
            last_window_end_ts=safe_int(row.get("window_end_ts")),
            last_event_type=row.get("last_event_type"),
        )
    return states


def run() -> None:
    engine = ObserverEngine()
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
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        engine.warnings.add(f"invalid_json:{source}")
                        continue
                    if isinstance(row, dict):
                        engine.process_row(source, row)
                        activity += 1
            engine.tick()
            if activity == 0:
                time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for handle in handles.values():
            if handle is not None:
                handle.close()
        engine.close()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
