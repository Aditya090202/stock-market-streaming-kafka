from dataclasses import dataclass
from typing import Optional


@dataclass
class Tick:
    """
    A single price tick representing a trade or quote event.
    Used for aggregation and analytics across services.
    """
    ts: float  # Unix timestamp
    price: float
    volume: Optional[float] = None

