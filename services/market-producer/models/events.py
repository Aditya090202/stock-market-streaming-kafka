from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, timezone
from enum import Enum
from typing import Literal, Optional
from uuid import uuid4

"""
example = Source.alpaca or Source.finnhub
example 
"""
# this class inherriting from str and Enum class means you get the best of both worlds
# the Enum restricts to only be specific strings, and str class allows you to seriliaze and have it behave as a string
class Source(str, Enum):
    alpaca = "alpaca"
    finnhub = "finnhub"

class EventType(str, Enum):
    trade = "trade"
    quote = "quote"
    bar = "bar"

# this is the base model for every type of message
# the other messages I will create below, will inherit all these attributes from it
class EventMessage(BaseModel):
    # controls the what happens when you provide fields that aren't defined in this model
    model_config = ConfigDict(extra="forbid")
    
    # a unique ID attribute, that calls a function every time a new instance is created
    id: str = Field(default_factory=lambda: uuid4().hex)
    schema_version: int = 1
    
    # uses the two classes created above
    event_type: EventType
    source: Source

    # the ticker symbol of the stock that was subscribed to
    symbol: str = Field(min_length=1, max_length=40)

    # a timestamp of when this message was sent
    event_ts: datetime
    # a timestamp for when this message was ingested into the kafka event stream
    ingested_ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class TradePayload(BaseModel):

    model_config = ConfigDict(extra="forbid")
    price: float = Field(gt=0)
    size: float = Field(gt=0)
    exchange: Optional[str] = None

class QuotePayload(BaseModel):

    model_config = ConfigDict(extra="forbid")
    bid_price: float = Field(ge=0)
    ask_price: float = Field(ge=0)
    bid_size: Optional[float] = Field(default=None, ge=0)
    ask_size: Optional[float] = Field(default=None, ge=0)

class BarPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    
    timeframe: Literal["1Min", "5Min", "15Min", "1H", "1D"] = "1Min"
    open: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    close: float = Field(ge=0)
    volume: float = Field(default=0, ge=0)

class TradeEvent(EventMessage):
    event_type: Literal[EventType.trade] = EventType.trade
    payload: TradePayload

class QuoteEvent(EventMessage):
    event_type: Literal[EventType.quote] = EventType.quote
    payload: QuotePayload

class BarEvent(EventMessage):
    event_type: Literal[EventType.bar] = EventType.bar
    payload: BarPayload