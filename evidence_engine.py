"""Layer-5 evidence packet aggregation engine.

The engine groups Layer-4 candidate events. It does not produce decisions,
signals, setups, forecasts, scores, or calibrated thresholds.
"""

import json
import time
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"

INBOX_FILE = DATA_DIR / "evidence_inbox.jsonl"
DETECTOR_EVENTS_FILE = DATA_DIR / "detector_events.jsonl"
MEASUREMENTS_FILE = DATA_DIR / "detector_measurements.jsonl"
CONTEXT_FILE = DATA_DIR / "context_dna.jsonl"
DATA_QUALITY_FILE = DATA_DIR / "data_quality.jsonl"

PACKETS_FILE = DATA_DIR / "evidence_packets.jsonl"
HEALTH_FILE = DATA_DIR / "evidence_health.json"

POLL_INTERVAL_SECONDS = 0.5
FLUSH_INTERVAL_SECONDS = 5.0
HEARTBEAT_SECONDS = 10.0
VALID_TIMEFRAMES = {"1S", "3S", "5S", "15S", "1M"}

PacketKey = tuple[str, str, int]


class EvidenceEngine:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.packet_versions, self.seen_event_ids = load_packet_state(PACKETS_FILE)
        self.open_packets: dict[PacketKey, dict[str, Any]] = {}
        self.detector_index = load_index(DETECTOR_EVENTS_FILE, "detector_event_id")
        self.measurement_index = load_window_index(MEASUREMENTS_FILE, "window_start_ts")
        self.context_index = load_window_index(CONTEXT_FILE, "source_window_ts")
        self.quality_index = load_quality_index(DATA_QUALITY_FILE)
        self.input_rows_processed = 0
        self.evidence_packets_written = 0
        self.last_window_ts = 0
        self.missing_inputs: set[str] = set()
        self.warnings: set[str] = set()
        self.last_flush = time.monotonic()
        self.last_heartbeat = time.monotonic()
        self.output_handle = PACKETS_FILE.open("a", encoding="utf-8")
        self.refresh_missing_inputs()
        self.write_health()

    def close(self) -> None:
        self.flush_all()
        self.write_health()
        self.output_handle.close()

    def refresh_missing_inputs(self) -> None:
        for path in (
            INBOX_FILE,
            DETECTOR_EVENTS_FILE,
            MEASUREMENTS_FILE,
            CONTEXT_FILE,
            DATA_QUALITY_FILE,
        ):
            label = relative_label(path)
            if path.exists():
                self.missing_inputs.discard(label)
            else:
                self.missing_inputs.add(label)

    def process_row(self, row: dict[str, Any]) -> None:
        self.input_rows_processed += 1
        parsed, errors = normalize_inbox_event(row, self.detector_index)
        if parsed is None:
            self.warnings.update(errors)
            return
        detector_event_id = parsed.get("detector_event_id")
        if detector_event_id and detector_event_id in self.seen_event_ids:
            return

        key = (parsed["symbol"], parsed["timeframe"], parsed["window_start_ts"])
        self.flush_previous_windows(key)
        packet = self.open_packets.get(key)
        if packet is None:
            packet = self.new_packet(key, parsed.get("window_end_ts"))
            self.open_packets[key] = packet

        packet["evidence_events"].append(parsed)
        input_errors = [error for error in errors if error != "non_null_score_ignored"]
        packet["validation"]["errors"].extend(input_errors)
        if "non_null_score_ignored" in errors:
            append_unique(packet["warnings"], "non_null_score_ignored")
        if input_errors:
            packet["validation"]["input_valid"] = False
        self.warnings.update(errors)
        if detector_event_id:
            self.seen_event_ids.add(str(detector_event_id))
        self.add_summary_event(packet, parsed)
        self.add_references(packet, parsed)
        self.merge_data_quality(packet, parsed)
        self.last_window_ts = max(self.last_window_ts, parsed["window_start_ts"])

    def new_packet(self, key: PacketKey, window_end_ts: int | None) -> dict[str, Any]:
        symbol, timeframe, window_start_ts = key
        return {
            "layer": "Layer-5",
            "engine": "EvidenceEngine",
            "record_type": "evidence_packet",
            "symbol": symbol,
            "timeframe": timeframe,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "packet_version": self.packet_versions.get(key, 0) + 1,
            "calibration_status": "uncalibrated",
            "evidence_summary": {
                "total_events": 0,
                "event_types": [],
                "buy_side_events": [],
                "sell_side_events": [],
                "neutral_events": [],
                "unknown_side_events": [],
            },
            "evidence_events": [],
            "measurement_refs": [],
            "detector_event_refs": [],
            "context_refs": [],
            "data_quality": {},
            "warnings": [],
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
                "input_valid": True,
                "events_grouped": True,
                "errors": [],
            },
        }

    def add_summary_event(self, packet: dict[str, Any], event: dict[str, Any]) -> None:
        summary = packet["evidence_summary"]
        summary["total_events"] += 1
        event_type = event["event_type"]
        if event_type not in summary["event_types"]:
            summary["event_types"].append(event_type)
        side_field = {
            "buy": "buy_side_events",
            "sell": "sell_side_events",
            "neutral": "neutral_events",
        }.get(event.get("side"), "unknown_side_events")
        summary[side_field].append(event_type)

    def add_references(self, packet: dict[str, Any], event: dict[str, Any]) -> None:
        append_unique(packet["detector_event_refs"], event.get("detector_event_id"))
        append_unique(packet["measurement_refs"], event.get("measurement_ref"))
        context_ref = event.get("context_refs")
        if context_ref:
            append_unique(packet["context_refs"], context_ref)

        key = (event["symbol"], event["timeframe"], event["window_start_ts"])
        measurement = self.measurement_index.get(key)
        if measurement is not None:
            append_unique(packet["measurement_refs"], measurement.get("measurement_id"))
            if measurement.get("context_refs"):
                append_unique(packet["context_refs"], measurement["context_refs"])
        context = self.context_index.get(key)
        if context is not None:
            append_unique(
                packet["context_refs"],
                {
                    "timeframe": context.get("timeframe"),
                    "source_window_ts": context.get("source_window_ts"),
                    "source_window_end_ts": context.get("source_window_end_ts"),
                },
            )

    def merge_data_quality(self, packet: dict[str, Any], event: dict[str, Any]) -> None:
        key = (event["symbol"], event["timeframe"], event["window_start_ts"])
        sources = [
            event.get("data_quality"),
            self.measurement_index.get(key, {}).get("data_quality"),
            self.context_index.get(key, {}).get("data_quality"),
            self.quality_index.get((event["timeframe"], event["window_start_ts"])),
        ]
        quality = packet["data_quality"]
        found = False
        for source in sources:
            if isinstance(source, dict) and source:
                quality.update(source)
                found = True
        if not found:
            quality.update(
                {
                    "quality_state": "unknown",
                    "warning": "source_data_quality_missing",
                }
            )

    def flush_previous_windows(self, current_key: PacketKey) -> None:
        symbol, timeframe, window_start_ts = current_key
        keys = [
            key
            for key in self.open_packets
            if key[0] == symbol and key[1] == timeframe and key[2] != window_start_ts
        ]
        for key in keys:
            self.flush_packet(key)

    def flush_packet(self, key: PacketKey) -> None:
        packet = self.open_packets.pop(key, None)
        if packet is None or not packet["evidence_events"]:
            return
        if not packet["data_quality"]:
            packet["data_quality"] = {
                "quality_state": "unknown",
                "warning": "source_data_quality_missing",
            }
        packet["validation"]["errors"] = unique_values(packet["validation"]["errors"])
        self.output_handle.write(json.dumps(packet, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.output_handle.flush()
        self.packet_versions[key] = packet["packet_version"]
        self.evidence_packets_written += 1

    def flush_all(self) -> None:
        for key in list(self.open_packets):
            self.flush_packet(key)
        self.last_flush = time.monotonic()

    def tick(self) -> None:
        now = time.monotonic()
        if now - self.last_flush >= FLUSH_INTERVAL_SECONDS:
            self.flush_all()
        if now - self.last_heartbeat >= HEARTBEAT_SECONDS:
            self.heartbeat()
            self.last_heartbeat = now

    def write_health(self) -> None:
        self.refresh_missing_inputs()
        payload = {
            "status": "alive",
            "input_rows_processed": self.input_rows_processed,
            "evidence_packets_written": self.evidence_packets_written,
            "open_packets": len(self.open_packets),
            "last_window_ts": self.last_window_ts,
            "missing_inputs": sorted(self.missing_inputs),
            "warnings": sorted(self.warnings),
        }
        HEALTH_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def heartbeat(self) -> None:
        self.write_health()
        print("Evidence Engine alive", flush=True)
        print(f"input_rows_processed={self.input_rows_processed}", flush=True)
        print(f"evidence_packets_written={self.evidence_packets_written}", flush=True)
        print(f"open_packets={len(self.open_packets)}", flush=True)
        print(f"last_window_ts={self.last_window_ts}", flush=True)


def normalize_inbox_event(
    row: dict[str, Any], detector_index: dict[str, dict[str, Any]]
) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    symbol = row.get("symbol")
    timeframe = row.get("timeframe")
    window_start_ts = safe_int(row.get("window_start_ts"))
    event_type = row.get("event_type")
    if not symbol:
        errors.append("missing_symbol")
    if timeframe not in VALID_TIMEFRAMES:
        errors.append("invalid_or_missing_timeframe")
    if window_start_ts is None:
        errors.append("missing_window_start_ts")
    if not event_type:
        errors.append("missing_event_type")
    if not symbol or timeframe not in VALID_TIMEFRAMES or window_start_ts is None or not event_type:
        return None, errors

    detector_event_id = row.get("detector_event_id")
    detector_event = detector_index.get(str(detector_event_id), {})
    confidence = row.get("confidence")
    strength_score = row.get("strength_score")
    warning = None
    if confidence is not None or strength_score is not None:
        warning = "non_null_score_ignored"
        errors.append(warning)

    side = row.get("side", detector_event.get("side", "unknown"))
    if side not in ("buy", "sell", "neutral"):
        side = "unknown"
    event = {
        "event_type": str(event_type),
        "detector_event_id": detector_event_id,
        "contract_name": row.get("contract_name", detector_event.get("contract_name")),
        "contract_version": row.get("contract_version", detector_event.get("contract_version")),
        "symbol": str(symbol),
        "timeframe": timeframe,
        "window_start_ts": window_start_ts,
        "window_end_ts": safe_int(row.get("window_end_ts")),
        "calibration_status": row.get("calibration_status", "unknown"),
        "side": side,
        "direction": row.get("direction", detector_event.get("direction", "unknown")),
        "validation_passed": row.get("validation_passed", detector_event.get("validation_passed")),
        "measurement_ref": row.get("measurement_ref", detector_event.get("measurement_ref")),
        "context_refs": row.get("context_refs", detector_event.get("context_refs")),
        "data_quality": row.get("data_quality", detector_event.get("data_quality")),
    }
    if warning is not None:
        event["warning"] = warning
    return event, errors


def load_packet_state(path: Path) -> tuple[dict[PacketKey, int], set[str]]:
    versions: dict[PacketKey, int] = {}
    event_ids: set[str] = set()
    for row in read_jsonl(path):
        key = packet_key(row)
        version = safe_int(row.get("packet_version"))
        if key is not None and version is not None:
            versions[key] = max(versions.get(key, 0), version)
        for event_id in row.get("detector_event_refs", []):
            if event_id:
                event_ids.add(str(event_id))
    return versions, event_ids


def load_index(path: Path, id_field: str) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in read_jsonl(path):
        record_id = row.get(id_field)
        if record_id:
            index[str(record_id)] = row
    return index


def load_window_index(path: Path, timestamp_field: str) -> dict[PacketKey, dict[str, Any]]:
    index: dict[PacketKey, dict[str, Any]] = {}
    for row in read_jsonl(path):
        symbol = row.get("symbol")
        timeframe = row.get("timeframe")
        timestamp = safe_int(row.get(timestamp_field))
        if symbol and timeframe and timestamp is not None:
            index[(str(symbol), str(timeframe), timestamp)] = row
    return index


def load_quality_index(path: Path) -> dict[tuple[str, int], dict[str, Any]]:
    index: dict[tuple[str, int], dict[str, Any]] = {}
    for row in read_jsonl(path):
        timeframe = row.get("timeframe")
        timestamp = safe_int(row.get("bucket_ts"))
        if timeframe and timestamp is not None:
            index[(str(timeframe), timestamp)] = row
    return index


def read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def packet_key(row: dict[str, Any]) -> PacketKey | None:
    symbol = row.get("symbol")
    timeframe = row.get("timeframe")
    window_start_ts = safe_int(row.get("window_start_ts"))
    if not symbol or not timeframe or window_start_ts is None:
        return None
    return str(symbol), str(timeframe), window_start_ts


def append_unique(values: list[Any], value: Any) -> None:
    if value is not None and value not in values:
        values.append(value)


def unique_values(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def relative_label(path: Path) -> str:
    return str(path.relative_to(ROOT_DIR)).replace("\\", "/")


def run() -> None:
    engine = EvidenceEngine()
    handle = None
    try:
        while True:
            if handle is None:
                if not INBOX_FILE.exists():
                    engine.refresh_missing_inputs()
                    engine.tick()
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue
                handle = INBOX_FILE.open("r", encoding="utf-8")
                engine.refresh_missing_inputs()

            line = handle.readline()
            if not line:
                engine.tick()
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            stripped = line.strip()
            if not stripped:
                engine.tick()
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError:
                engine.input_rows_processed += 1
                engine.warnings.add("invalid_json_input_row")
                engine.tick()
                continue
            if isinstance(row, dict):
                engine.process_row(row)
            else:
                engine.input_rows_processed += 1
                engine.warnings.add("non_object_input_row")
            engine.tick()
    finally:
        if handle is not None:
            handle.close()
        engine.close()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
