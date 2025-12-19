from __future__ import annotations

import sys
from pathlib import Path
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional

# Add project root to path for shared models
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from models.tick import Tick

@dataclass
class RollingStats:
    window: Deque[Tick]
    sum_price: float
    count: int = 0
    open_price: Optional[float] = None
    sum_pv: float = 0.0
    sum_vol: float = 0.0

class InMemoryAggregator:
    """
    Keeps a rolling time-based window of ticks per symbol in memory,
    and maintains running sums so we can compute metrics quickly.
    """

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds
        self.by_symbol: Dict[str, RollingStats] = {}
        self.latest: Dict[str, Tick] = {}

    def _get(self, symbol: str) -> RollingStats:
        if symbol not in self.by_symbol:
            self.by_symbol[symbol] = RollingStats(window=deque())
        return self.by_symbol[symbol]

    def add_tick(self, symbol: str, tick: Tick) -> None:
        s = self._get(symbol)

        # First price seen for this symbol becomes "open"
        if s.open_price is None:
            s.open_price = tick.price

        # Add new tick at the end of the deque
        s.window.append(tick)
        s.sum_price += tick.price
        s.count += 1

        if tick.volume is not None:
            s.sum_pv += tick.price * tick.volume
            s.sum_vol += tick.volume

        # Update latest tick
        self.latest[symbol] = tick

        # Evict old ticks outside the rolling window
        cutoff = tick.ts - self.window_seconds
        while s.window and s.window[0].ts < cutoff:
            old = s.window.popleft()
            s.sum_price -= old.price
            s.count -= 1
            if old.volume is not None:
                s.sum_pv -= old.price * old.volume
                s.sum_vol -= old.volume

    def snapshot_symbol(self, symbol: str) -> dict:
        s = self._get(symbol)
        latest = self.latest.get(symbol)

        sma = (s.sum_price / s.count) if s.count > 0 else None

        pct_change = None
        if latest and s.open_price:
            pct_change = (latest.price - s.open_price) / s.open_price * 100.0

        vwap = (s.sum_pv / s.sum_vol) if s.sum_vol > 0 else None

        return {
            "symbol": symbol,
            "latest_price": latest.price if latest else None,
            "latest_ts": latest.ts if latest else None,
            "sma_window": sma,
            "pct_change_from_open": pct_change,
            "vwap_window": vwap,
            "window_points": s.count,
        }

    def snapshot_all(self) -> list[dict]:
        return [
            self.snapshot_symbol(sym)
            for sym in sorted(self.by_symbol.keys())
        ]
