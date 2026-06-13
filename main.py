import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import websockets


SYMBOL = "BTCUSDT"
TRADE_STREAM_URL = "wss://fstream.binance.com/ws/btcusdt@trade"
DEPTH_STREAM_URL = "wss://fstream.binance.com/ws/btcusdt@depth@100ms"

DATA_DIR = Path("data")
CANDLE_FILE = DATA_DIR / "one_second_candle_dna.jsonl"
FOOTPRINT_FILE = DATA_DIR / "one_second_footprint_dna.jsonl"
DEPTH_MUTATION_FILE = DATA_DIR / "one_second_depth_mutation_dna.jsonl"
COMBINED_FILE = DATA_DIR / "one_second_combined_dna.jsonl"

FULL_PRINT = False
EPSILON = 1e-9
ALLOWED_LATENESS_MS = 250


@dataclass
class TradeEvent:
    time: int
    price: float
    quantity: float
    side: str


@dataclass
class PriceLevelAgg:
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    trade_count: int = 0


@dataclass
class DepthLevelAgg:
    price: float
    last_qty: float = 0.0
    update_count: int = 0
    last_update_time: int = 0


@dataclass
class WindowState:
    window_start_ts: int
    valid_trades: list[dict[str, Any]] = field(default_factory=list)
    valid_depth_events: list[dict[str, Any]] = field(default_factory=list)


class JsonlWriter:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.files = {
            "candle": CANDLE_FILE.open("a", encoding="utf-8"),
            "footprint": FOOTPRINT_FILE.open("a", encoding="utf-8"),
            "depth": DEPTH_MUTATION_FILE.open("a", encoding="utf-8"),
            "combined": COMBINED_FILE.open("a", encoding="utf-8"),
        }

    def write_all(
        self,
        candle_dna: dict[str, Any],
        footprint_dna: dict[str, Any],
        depth_mutation_dna: dict[str, Any],
        combined_dna: dict[str, Any],
    ) -> None:
        payloads = {
            "candle": candle_dna,
            "footprint": footprint_dna,
            "depth": depth_mutation_dna,
            "combined": combined_dna,
        }
        for key, payload in payloads.items():
            handle = self.files[key]
            handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
            handle.flush()

    def close(self) -> None:
        for handle in self.files.values():
            handle.close()


class WindowManager:
    def __init__(self, writer: JsonlWriter) -> None:
        self.writer = writer
        self.windows: dict[int, WindowState] = {}
        self.finalized_windows: set[int] = set()
        self.failed_windows: list[int] = []
        self.has_seen_trade = False
        self.has_seen_depth = False
        self.last_close_price: float | None = None
        self.next_window_start: int | None = None
        self.watermark_ts: int | None = None

    def add_trade(self, payload: dict[str, Any]) -> None:
        trade = self._parse_trade(payload)
        if trade is None:
            return

        event_time = int(trade["time"])
        window_start_ts = self._window_start(event_time)
        if self._is_late(event_time, window_start_ts):
            return

        self._observe_window(window_start_ts)
        self._advance_watermark(event_time)
        self.has_seen_trade = True
        window = self._get_window(window_start_ts)
        window.valid_trades.append(trade)

        self._finalize_ready_windows()

    def _parse_trade(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        try:
            event_time = int(payload["T"])
            price = float(payload["p"])
            qty = float(payload["q"])
            buyer_is_maker = payload["m"]
        except KeyError as exc:
            self._log_invalid_trade(f"missing_field_{exc.args[0]}", payload)
            return None
        except (TypeError, ValueError) as exc:
            self._log_invalid_trade(f"parse_error_{exc}", payload)
            return None

        side = "sell" if buyer_is_maker is True else "buy"
        if buyer_is_maker not in (True, False):
            self._log_invalid_trade("invalid_buyer_is_maker", payload)
            return None
        if event_time <= 0:
            self._log_invalid_trade("event_time_not_positive", payload)
            return None
        if price <= 0:
            self._log_invalid_trade("price_not_positive", payload)
            return None
        if qty <= 0:
            self._log_invalid_trade("quantity_not_positive", payload)
            return None
        if side not in ("buy", "sell"):
            self._log_invalid_trade("invalid_side", payload)
            return None

        return {
            "price": price,
            "qty": qty,
            "time": event_time,
            "side": side,
        }

    @staticmethod
    def _log_invalid_trade(reason: str, payload: dict[str, Any]) -> None:
        print(f"Invalid trade skipped: reason={reason} payload={payload}", flush=True)

    def add_depth(self, payload: dict[str, Any]) -> None:
        depth_event = self._parse_depth_event(payload)
        if depth_event is None:
            return

        event_time = int(depth_event["time"])
        window_start_ts = self._window_start(event_time)
        if self._is_late(event_time, window_start_ts):
            return

        self._observe_window(window_start_ts)
        self._advance_watermark(event_time)
        self.has_seen_depth = True
        window = self._get_window(window_start_ts)
        window.valid_depth_events.append(depth_event)

        self._finalize_ready_windows()

    def _parse_depth_event(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        try:
            event_time = int(payload["E"])
        except KeyError:
            print(f"Invalid depth skipped: reason=missing_field_E payload={payload}", flush=True)
            return None
        except (TypeError, ValueError) as exc:
            print(f"Invalid depth skipped: reason=parse_error_{exc} payload={payload}", flush=True)
            return None

        if event_time <= 0:
            print(f"Invalid depth skipped: reason=event_time_not_positive payload={payload}", flush=True)
            return None

        return {
            "time": event_time,
            "bids": self._parse_depth_updates(payload.get("b", []), payload, "bid"),
            "asks": self._parse_depth_updates(payload.get("a", []), payload, "ask"),
        }

    @staticmethod
    def _parse_depth_updates(
        updates: list[list[str]],
        payload: dict[str, Any],
        side: str,
    ) -> list[dict[str, float]]:
        parsed = []
        for update in updates:
            try:
                price_raw, qty_raw = update
                parsed.append({"price": float(price_raw), "qty": float(qty_raw)})
            except (TypeError, ValueError) as exc:
                print(
                    f"Invalid depth update skipped: side={side} reason=parse_error_{exc} payload={payload}",
                    flush=True,
                )
        return parsed

    def _observe_window(self, window_start_ts: int) -> None:
        if self.next_window_start is None or window_start_ts < self.next_window_start:
            self.next_window_start = window_start_ts

    def _get_window(self, window_start_ts: int) -> WindowState:
        return self.windows.setdefault(window_start_ts, WindowState(window_start_ts))

    def _is_late(self, event_time: int, window_start_ts: int) -> bool:
        if window_start_ts in self.finalized_windows:
            print(
                "Late event ignored for finalized window: "
                f"event_time={event_time} window_start={window_start_ts}",
                flush=True,
            )
            return True
        return False

    def _advance_watermark(self, event_time: int) -> None:
        if self.watermark_ts is None or event_time > self.watermark_ts:
            self.watermark_ts = event_time

    def _finalize_ready_windows(self) -> None:
        if not (self.has_seen_trade and self.has_seen_depth):
            print("Warm-up: waiting for first trade and depth event...", flush=True)
            return

        if self.next_window_start is None or self.watermark_ts is None:
            return

        while self.watermark_ts >= self.next_window_start + 1000 + ALLOWED_LATENESS_MS:
            window_start_ts = self.next_window_start
            if window_start_ts not in self.finalized_windows:
                self._finalize_window(window_start_ts)
            self.next_window_start += 1000

    def _finalize_window(self, window_start_ts: int) -> None:
        window = self.windows.pop(window_start_ts, WindowState(window_start_ts))
        candle_dna = self._build_candle_dna(window)
        footprint_dna = self._build_footprint_dna(window)
        depth_mutation_dna = self._build_depth_mutation_dna(window)
        combined_dna = {
            "symbol": SYMBOL,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_start_ts + 1000,
            "candle_dna": candle_dna,
            "footprint_dna": footprint_dna,
            "depth_mutation_dna": depth_mutation_dna,
        }

        errors = self._validate(candle_dna, footprint_dna, depth_mutation_dna, combined_dna)
        if errors:
            self.failed_windows.append(window_start_ts)
            self.finalized_windows.add(window_start_ts)
            print(
                f"Validation failed for window {window_start_ts}: {'; '.join(errors)}\n"
                f"valid_trades_count={len(window.valid_trades)}\n"
                f"first_trades={window.valid_trades[:5]}\n"
                f"last_trades={window.valid_trades[-5:]}\n"
                f"valid_depth_events_count={len(window.valid_depth_events)}\n"
                f"first_depth_times={[event['time'] for event in window.valid_depth_events[:5]]}\n"
                f"last_depth_times={[event['time'] for event in window.valid_depth_events[-5:]]}",
                flush=True,
            )
            return

        self.writer.write_all(candle_dna, footprint_dna, depth_mutation_dna, combined_dna)
        self.finalized_windows.add(window_start_ts)
        if candle_dna["has_trade"]:
            self.last_close_price = candle_dna["close"]["price"]
        self._print_output(candle_dna, footprint_dna, depth_mutation_dna, combined_dna)

    def _build_candle_dna(self, window: WindowState) -> dict[str, Any]:
        window_start_ts = window.window_start_ts
        window_end_ts = window_start_ts + 1000
        valid_trades = sorted(window.valid_trades, key=lambda trade: trade["time"])
        if not valid_trades:
            return {
                "symbol": SYMBOL,
                "window_start_ts": window_start_ts,
                "window_end_ts": window_end_ts,
                "has_trade": False,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "trade_count": 0,
                "buy_volume": 0.0,
                "sell_volume": 0.0,
                "total_volume": 0.0,
                "delta": 0.0,
                "delta_state": "neutral",
                "last_trade_price": None,
                "carry_forward": self.last_close_price is not None,
                "carry_forward_price": self.last_close_price,
            }

        open_trade = valid_trades[0]
        close_trade = valid_trades[-1]
        high_trade = valid_trades[0]
        low_trade = valid_trades[0]
        buy_volume = 0.0
        sell_volume = 0.0
        for trade in valid_trades:
            if trade["price"] > high_trade["price"]:
                high_trade = trade
            if trade["price"] < low_trade["price"]:
                low_trade = trade
            if trade["side"] == "buy":
                buy_volume += trade["qty"]
            else:
                sell_volume += trade["qty"]

        total_volume = buy_volume + sell_volume
        delta = buy_volume - sell_volume

        return {
            "symbol": SYMBOL,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "has_trade": True,
            "open": self._trade_point(open_trade),
            "high": self._trade_point(high_trade),
            "low": self._trade_point(low_trade),
            "close": self._trade_point(close_trade),
            "trade_count": len(valid_trades),
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "total_volume": total_volume,
            "delta": delta,
            "delta_state": self._delta_state(delta),
            "last_trade_price": close_trade["price"],
            "carry_forward": False,
            "carry_forward_price": None,
        }

    def _build_footprint_dna(self, window: WindowState) -> dict[str, Any]:
        footprint: dict[float, PriceLevelAgg] = {}
        for trade in window.valid_trades:
            level = footprint.setdefault(trade["price"], PriceLevelAgg())
            if trade["side"] == "buy":
                level.buy_volume += trade["qty"]
            else:
                level.sell_volume += trade["qty"]
            level.trade_count += 1

        price_levels = []
        for price in sorted(footprint.keys(), reverse=True):
            level = footprint[price]
            total_volume = level.buy_volume + level.sell_volume
            delta = level.buy_volume - level.sell_volume
            price_levels.append(
                {
                    "price": price,
                    "buy_volume": level.buy_volume,
                    "sell_volume": level.sell_volume,
                    "total_volume": total_volume,
                    "delta": delta,
                    "delta_state": self._delta_state(delta),
                    "trade_count": level.trade_count,
                }
            )
        return {
            "symbol": SYMBOL,
            "window_start_ts": window.window_start_ts,
            "window_end_ts": window.window_start_ts + 1000,
            "has_trade": bool(window.valid_trades),
            "price_levels": price_levels,
        }

    def _build_depth_mutation_dna(self, window: WindowState) -> dict[str, Any]:
        bid_update_count = 0
        ask_update_count = 0
        bid_total_reported_qty = 0.0
        ask_total_reported_qty = 0.0
        bid_price_levels: dict[float, DepthLevelAgg] = {}
        ask_price_levels: dict[float, DepthLevelAgg] = {}
        last_bid_update = None
        last_ask_update = None

        for event in sorted(window.valid_depth_events, key=lambda item: item["time"]):
            event_time = event["time"]
            bids = event["bids"]
            asks = event["asks"]
            bid_update_count += len(bids)
            ask_update_count += len(asks)

            for update in bids:
                price = update["price"]
                qty = update["qty"]
                bid_total_reported_qty += qty
                level = bid_price_levels.setdefault(price, DepthLevelAgg(price=price))
                level.last_qty = qty
                level.update_count += 1
                level.last_update_time = event_time
                last_bid_update = {"price": price, "qty": qty, "time": event_time}

            for update in asks:
                price = update["price"]
                qty = update["qty"]
                ask_total_reported_qty += qty
                level = ask_price_levels.setdefault(price, DepthLevelAgg(price=price))
                level.last_qty = qty
                level.update_count += 1
                level.last_update_time = event_time
                last_ask_update = {"price": price, "qty": qty, "time": event_time}

        balance = bid_update_count - ask_update_count
        update_total = bid_update_count + ask_update_count
        if bid_update_count > ask_update_count:
            dominant_side = "bid"
        elif ask_update_count > bid_update_count:
            dominant_side = "ask"
        else:
            dominant_side = "neutral"

        return {
            "symbol": SYMBOL,
            "window_start_ts": window.window_start_ts,
            "window_end_ts": window.window_start_ts + 1000,
            "has_depth": bool(window.valid_depth_events),
            "bid_update_count": bid_update_count,
            "ask_update_count": ask_update_count,
            "bid_total_reported_qty": bid_total_reported_qty,
            "ask_total_reported_qty": ask_total_reported_qty,
            "bid_price_levels": self._depth_levels(bid_price_levels),
            "ask_price_levels": self._depth_levels(ask_price_levels),
            "last_bid_update": last_bid_update,
            "last_ask_update": last_ask_update,
            "dominant_side": dominant_side,
            "balance": balance,
            "imbalance": balance / update_total if update_total > 0 else 0.0,
            "ratio": (
                bid_update_count / ask_update_count
                if ask_update_count > 0
                else None
            ),
        }

    def _validate(
        self,
        candle: dict[str, Any],
        footprint: dict[str, Any],
        depth: dict[str, Any],
        combined: dict[str, Any],
    ) -> list[str]:
        errors = []
        if not self._same_float(candle["total_volume"], candle["buy_volume"] + candle["sell_volume"]):
            errors.append("candle total_volume mismatch")
        if not self._same_float(candle["delta"], candle["buy_volume"] - candle["sell_volume"]):
            errors.append("candle delta mismatch")
        trade_points = ("open", "high", "low", "close")
        if candle["has_trade"] and any(candle[key] is None for key in trade_points):
            errors.append("candle trade points missing")
        if candle["has_trade"] and all(candle[key] is not None for key in trade_points):
            if candle["high"]["price"] < candle["low"]["price"]:
                errors.append("candle high.price < low.price")
            for key in ("open", "high", "low", "close"):
                if candle[key]["price"] <= 0:
                    errors.append(f"candle {key}.price must be positive")
                if not self._time_in_window(
                    candle[key]["time"],
                    candle["window_start_ts"],
                    candle["window_end_ts"],
                ):
                    errors.append(f"candle {key}.time outside window")
        if not candle["has_trade"] and any(candle[key] is not None for key in trade_points):
            errors.append("empty candle trade points present")

        for key in ("last_bid_update", "last_ask_update"):
            update = depth[key]
            if update is not None and not self._time_in_window(
                update["time"],
                depth["window_start_ts"],
                depth["window_end_ts"],
            ):
                errors.append(f"depth {key}.time outside window")

        footprint_buy = sum(level["buy_volume"] for level in footprint["price_levels"])
        footprint_sell = sum(level["sell_volume"] for level in footprint["price_levels"])
        if not self._same_float(footprint_buy, candle["buy_volume"]):
            errors.append("footprint buy_volume mismatch")
        if not self._same_float(footprint_sell, candle["sell_volume"]):
            errors.append("footprint sell_volume mismatch")

        nested = [combined["candle_dna"], combined["footprint_dna"], combined["depth_mutation_dna"]]
        starts = {combined["window_start_ts"], *(item["window_start_ts"] for item in nested)}
        ends = {combined["window_end_ts"], *(item["window_end_ts"] for item in nested)}
        if len(starts) != 1:
            errors.append("combined window_start_ts mismatch")
        if len(ends) != 1:
            errors.append("combined window_end_ts mismatch")
        if depth["window_start_ts"] != combined["window_start_ts"]:
            errors.append("depth window_start_ts mismatch")
        return errors

    def _print_output(
        self,
        candle: dict[str, Any],
        footprint: dict[str, Any],
        depth: dict[str, Any],
        combined: dict[str, Any],
    ) -> None:
        if FULL_PRINT:
            print(json.dumps(candle, indent=2, ensure_ascii=False), flush=True)
            print(json.dumps(footprint, indent=2, ensure_ascii=False), flush=True)
            print(json.dumps(depth, indent=2, ensure_ascii=False), flush=True)
            print(json.dumps(combined, indent=2, ensure_ascii=False), flush=True)
            return

        price = candle["close"]["price"] if candle["has_trade"] else candle["carry_forward_price"]
        print(
            "[1S DNA] "
            f"ts={candle['window_start_ts']} "
            f"price={self._format_price(price)} "
            f"trades={candle['trade_count']} "
            f"delta={self._format_float(candle['delta'], 6)} "
            f"levels={len(footprint['price_levels'])} "
            f"bid_updates={depth['bid_update_count']} "
            f"ask_updates={depth['ask_update_count']}",
            flush=True,
        )

    @staticmethod
    def _window_start(event_time: int) -> int:
        return (event_time // 1000) * 1000

    @staticmethod
    def _trade_point(trade: dict[str, Any]) -> dict[str, Any]:
        return {"price": trade["price"], "time": trade["time"], "side": trade["side"]}

    @staticmethod
    def _depth_levels(levels: dict[float, DepthLevelAgg]) -> list[dict[str, Any]]:
        return [
            {
                "price": level.price,
                "last_qty": level.last_qty,
                "update_count": level.update_count,
                "last_update_time": level.last_update_time,
            }
            for price, level in sorted(levels.items(), reverse=True)
        ]

    @staticmethod
    def _delta_state(delta: float) -> str:
        if delta > 0:
            return "positive"
        if delta < 0:
            return "negative"
        return "neutral"

    @staticmethod
    def _same_float(left: float, right: float) -> bool:
        return abs(left - right) <= EPSILON

    @staticmethod
    def _time_in_window(event_time: int, window_start_ts: int, window_end_ts: int) -> bool:
        # window_end_ts is an exclusive boundary: start <= event_time < end.
        return window_start_ts <= event_time < window_end_ts

    @staticmethod
    def _format_price(value: float | None) -> str:
        if value is None:
            return "null"
        return f"{value:.2f}"

    @staticmethod
    def _format_float(value: float | None, places: int) -> str:
        if value is None:
            return "null"
        return f"{value:.{places}f}".rstrip("0").rstrip(".")


async def stream_reader(name: str, url: str, queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    backoff = 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as websocket:
                print(f"{name} stream connected", flush=True)
                backoff = 1
                async for message in websocket:
                    await queue.put((name, json.loads(message)))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"{name} stream disconnected: {exc}. Reconnecting in {backoff}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def event_consumer(queue: asyncio.Queue[tuple[str, dict[str, Any]]], manager: WindowManager) -> None:
    while True:
        stream_name, payload = await queue.get()
        try:
            if stream_name == "trade":
                manager.add_trade(payload)
            elif stream_name == "depth":
                manager.add_depth(payload)
        except Exception as exc:
            print(f"Event processing error on {stream_name}: {exc}", flush=True)
        finally:
            queue.task_done()


async def run() -> None:
    writer = JsonlWriter()
    manager = WindowManager(writer)
    queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
    tasks = [
        asyncio.create_task(stream_reader("trade", TRADE_STREAM_URL, queue)),
        asyncio.create_task(stream_reader("depth", DEPTH_STREAM_URL, queue)),
        asyncio.create_task(event_consumer(queue, manager)),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        writer.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
