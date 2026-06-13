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
    trades: list[TradeEvent] = field(default_factory=list)
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    footprint: dict[float, PriceLevelAgg] = field(default_factory=dict)
    bid_update_count: int = 0
    ask_update_count: int = 0
    bid_total_reported_qty: float = 0.0
    ask_total_reported_qty: float = 0.0
    bid_price_levels: dict[float, DepthLevelAgg] = field(default_factory=dict)
    ask_price_levels: dict[float, DepthLevelAgg] = field(default_factory=dict)
    last_bid_update: dict[str, Any] | None = None
    last_ask_update: dict[str, Any] | None = None
    has_depth: bool = False


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

    def add_trade(self, payload: dict[str, Any]) -> None:
        event_time = int(payload["T"])
        window_start_ts = self._window_start(event_time)
        if self._is_late(window_start_ts):
            return

        self._observe_window(window_start_ts)
        self.has_seen_trade = True
        trade = TradeEvent(
            time=event_time,
            price=float(payload["p"]),
            quantity=float(payload["q"]),
            side="sell" if bool(payload["m"]) else "buy",
        )
        window = self._get_window(window_start_ts)
        window.trades.append(trade)
        if trade.side == "buy":
            window.buy_volume += trade.quantity
        else:
            window.sell_volume += trade.quantity

        level = window.footprint.setdefault(trade.price, PriceLevelAgg())
        if trade.side == "buy":
            level.buy_volume += trade.quantity
        else:
            level.sell_volume += trade.quantity
        level.trade_count += 1

        self._finalize_ready_windows(window_start_ts)

    def add_depth(self, payload: dict[str, Any]) -> None:
        event_time = int(payload["E"])
        window_start_ts = self._window_start(event_time)
        if self._is_late(window_start_ts):
            return

        self._observe_window(window_start_ts)
        self.has_seen_depth = True
        window = self._get_window(window_start_ts)
        window.has_depth = True

        bids = payload.get("b", [])
        asks = payload.get("a", [])
        self._add_depth_updates(window, bids, "bid", event_time)
        self._add_depth_updates(window, asks, "ask", event_time)

        self._finalize_ready_windows(window_start_ts)

    def _add_depth_updates(
        self,
        window: WindowState,
        updates: list[list[str]],
        side: str,
        event_time: int,
    ) -> None:
        if side == "bid":
            window.bid_update_count += len(updates)
            level_map = window.bid_price_levels
        else:
            window.ask_update_count += len(updates)
            level_map = window.ask_price_levels

        last_update = None
        for price_raw, qty_raw in updates:
            price = float(price_raw)
            qty = float(qty_raw)
            if side == "bid":
                window.bid_total_reported_qty += qty
            else:
                window.ask_total_reported_qty += qty

            level = level_map.setdefault(price, DepthLevelAgg(price=price))
            level.last_qty = qty
            level.update_count += 1
            level.last_update_time = event_time
            last_update = {"price": price, "qty": qty, "time": event_time}

        if last_update is not None:
            if side == "bid":
                window.last_bid_update = last_update
            else:
                window.last_ask_update = last_update

    def _observe_window(self, window_start_ts: int) -> None:
        if self.next_window_start is None or window_start_ts < self.next_window_start:
            self.next_window_start = window_start_ts

    def _get_window(self, window_start_ts: int) -> WindowState:
        return self.windows.setdefault(window_start_ts, WindowState(window_start_ts))

    def _is_late(self, window_start_ts: int) -> bool:
        if window_start_ts in self.finalized_windows:
            print(f"Late event ignored for finalized window: {window_start_ts}", flush=True)
            return True
        return False

    def _finalize_ready_windows(self, current_window_start_ts: int) -> None:
        if not (self.has_seen_trade and self.has_seen_depth):
            print("Warm-up: waiting for first trade and depth event...", flush=True)
            return

        if self.next_window_start is None:
            return

        while self.next_window_start < current_window_start_ts:
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
            "window_end_ts": window_start_ts + 999,
            "candle_dna": candle_dna,
            "footprint_dna": footprint_dna,
            "depth_mutation_dna": depth_mutation_dna,
        }

        errors = self._validate(candle_dna, footprint_dna, depth_mutation_dna, combined_dna)
        if errors:
            self.failed_windows.append(window_start_ts)
            self.finalized_windows.add(window_start_ts)
            print(
                f"Validation failed for window {window_start_ts}: {'; '.join(errors)}",
                flush=True,
            )
            return

        self.writer.write_all(candle_dna, footprint_dna, depth_mutation_dna, combined_dna)
        self.finalized_windows.add(window_start_ts)
        if candle_dna["has_trade"]:
            self.last_close_price = candle_dna["close"]["price"]
        self._print_output(candle_dna, footprint_dna, depth_mutation_dna, combined_dna)

    def _build_candle_dna(self, window: WindowState) -> dict[str, Any]:
        trades = window.trades
        window_start_ts = window.window_start_ts
        window_end_ts = window_start_ts + 999
        if not trades:
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
                "carry_forward_price": self.last_close_price,
            }

        open_trade = trades[0]
        high_trade = trades[0]
        low_trade = trades[0]
        for trade in trades[1:]:
            if trade.price > high_trade.price:
                high_trade = trade
            if trade.price < low_trade.price:
                low_trade = trade
        close_trade = trades[-1]
        total_volume = window.buy_volume + window.sell_volume
        delta = window.buy_volume - window.sell_volume

        return {
            "symbol": SYMBOL,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "has_trade": True,
            "open": self._trade_point(open_trade),
            "high": self._trade_point(high_trade),
            "low": self._trade_point(low_trade),
            "close": self._trade_point(close_trade),
            "trade_count": len(trades),
            "buy_volume": window.buy_volume,
            "sell_volume": window.sell_volume,
            "total_volume": total_volume,
            "delta": delta,
            "delta_state": self._delta_state(delta),
            "last_trade_price": close_trade.price,
            "carry_forward_price": None,
        }

    def _build_footprint_dna(self, window: WindowState) -> dict[str, Any]:
        price_levels = []
        for price in sorted(window.footprint.keys(), reverse=True):
            level = window.footprint[price]
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
            "window_end_ts": window.window_start_ts + 999,
            "has_trade": bool(window.trades),
            "price_levels": price_levels,
        }

    def _build_depth_mutation_dna(self, window: WindowState) -> dict[str, Any]:
        balance = window.bid_update_count - window.ask_update_count
        update_total = window.bid_update_count + window.ask_update_count
        if window.bid_update_count > window.ask_update_count:
            dominant_side = "bid"
        elif window.ask_update_count > window.bid_update_count:
            dominant_side = "ask"
        else:
            dominant_side = "neutral"

        return {
            "symbol": SYMBOL,
            "window_start_ts": window.window_start_ts,
            "window_end_ts": window.window_start_ts + 999,
            "has_depth": window.has_depth,
            "bid_update_count": window.bid_update_count,
            "ask_update_count": window.ask_update_count,
            "bid_total_reported_qty": window.bid_total_reported_qty,
            "ask_total_reported_qty": window.ask_total_reported_qty,
            "bid_price_levels": self._depth_levels(window.bid_price_levels),
            "ask_price_levels": self._depth_levels(window.ask_price_levels),
            "last_bid_update": window.last_bid_update,
            "last_ask_update": window.last_ask_update,
            "dominant_side": dominant_side,
            "balance": balance,
            "imbalance": balance / update_total if update_total > 0 else 0.0,
            "ratio": (
                window.bid_update_count / window.ask_update_count
                if window.ask_update_count > 0
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
        if candle["has_trade"] and candle["high"]["price"] < candle["low"]["price"]:
            errors.append("candle high.price < low.price")
        if candle["has_trade"] and any(candle[key] is None for key in ("open", "high", "low", "close")):
            errors.append("candle trade points missing")
        if not candle["has_trade"] and any(candle[key] is not None for key in ("open", "high", "low", "close")):
            errors.append("empty candle trade points present")

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
            f"price={price} "
            f"trades={candle['trade_count']} "
            f"delta={candle['delta']} "
            f"levels={len(footprint['price_levels'])} "
            f"bid_updates={depth['bid_update_count']} "
            f"ask_updates={depth['ask_update_count']}",
            flush=True,
        )

    @staticmethod
    def _window_start(event_time: int) -> int:
        return (event_time // 1000) * 1000

    @staticmethod
    def _trade_point(trade: TradeEvent) -> dict[str, Any]:
        return {"price": trade.price, "time": trade.time, "side": trade.side}

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
