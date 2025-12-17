import asyncio
import websockets
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from confluent_kafka import Producer
import sys
from pathlib import Path
# Add parent directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))
from models.events import (
    TradeEvent,
    QuoteEvent, 
    BarEvent,
    Source,
    EventType,
    TradePayload,
    QuotePayload,
    BarPayload
)

# Load environment variables from .env file
load_dotenv()

# create a producer and have it send messages to all three topics
alpaca_producer = Producer({'bootstrap.servers': 'localhost:9092'})


"""
## FUNCTIONS
"""

def get_kafka_topic(event):
    """
    Determine the Kafka topic name based on the event type.
    
    Args:
        event: EventMessage instance (TradeEvent, QuoteEvent, or BarEvent)
    
    Returns:
        str: Kafka topic name ("market.trades", "market.quotes", or "market.bars")
    """
    if event.event_type == EventType.trade:
        return "market.trades"
    elif event.event_type == EventType.quote:
        return "market.quotes"
    elif event.event_type == EventType.bar:
        return "market.bars"
    else:
        raise ValueError(f"Unknown event type: {event.event_type}")

def parse_alpaca_timestamp(timestamp_value):
    """
    Convert Alpaca timestamp to datetime object.
    Handles:
    - ISO 8601/RFC3339 strings (e.g., "2025-12-17T02:43:34.949784589Z")
    - Numeric timestamps (nanoseconds or seconds)
    - Numeric timestamps as strings
    
    Args:
        timestamp_value: Timestamp as string or int/float
    
    Returns:
        datetime object or None if parsing fails
    """
    try:
        if isinstance(timestamp_value, str):
            # Check if it's an ISO 8601 format (contains 'T' or '-')
            if 'T' in timestamp_value or '-' in timestamp_value:
                # Parse ISO 8601 format, removing 'Z' and handling nanoseconds
                timestamp_str = timestamp_value.replace('Z', '+00:00')
                # Python's fromisoformat can't handle nanoseconds (9 digits), only microseconds (6 digits)
                # Truncate to microseconds if needed
                if '.' in timestamp_str:
                    parts = timestamp_str.split('.')
                    fractional_with_tz = parts[1]
                    # Extract timezone if present
                    tz_part = ''
                    for i, char in enumerate(fractional_with_tz):
                        if char in ['+', '-']:
                            tz_part = fractional_with_tz[i:]
                            fractional_with_tz = fractional_with_tz[:i]
                            break
                    # Truncate to 6 digits (microseconds)
                    fractional = fractional_with_tz[:6].ljust(6, '0')
                    timestamp_str = f"{parts[0]}.{fractional}{tz_part}"
                return datetime.fromisoformat(timestamp_str)
            else:
                # Try to parse as numeric timestamp string
                # Use float() to preserve fractional precision
                timestamp_value = float(timestamp_value)
        
        # Handle numeric timestamps based on magnitude
        # Current epoch values (Dec 2025):
        #   Seconds:      ~1.7e9  (1,700,000,000)
        #   Milliseconds: ~1.7e12 (1,700,000,000,000)
        #   Nanoseconds:  ~1.7e18 (1,700,000,000,000,000,000)
        if timestamp_value > 1e15:
            # Nanoseconds (> 1e15)
            return datetime.fromtimestamp(timestamp_value / 1e9, tz=timezone.utc)
        elif timestamp_value > 1e12:
            # Milliseconds (> 1e12 but < 1e15)
            return datetime.fromtimestamp(timestamp_value / 1e3, tz=timezone.utc)
        else:
            # Seconds (< 1e12)
            return datetime.fromtimestamp(timestamp_value, tz=timezone.utc)
    except (ValueError, TypeError, OSError) as e:
        print(f"Error parsing timestamp: {timestamp_value} - {e}")
        return None

"""
This function will take in a "str" message => a structured "str" message in JSON
It will determine if the message received from the Alpaca API is a trade, quote, or a bar
Returns None if the message is not a trade/quote/bar event (e.g., subscription confirmations)
"""
def filter_event_messages(event_msg):
    # Check if data array is empty or if first element doesn't have required fields
    if not event_msg or not isinstance(event_msg, list) or len(event_msg) == 0:
        return None
    
    msg = event_msg[0]
    msg_type = msg.get("T")
    
    # Skip non-data messages (subscription confirmations, errors, etc.)
    if msg_type not in ["t", "q", "d", "b"]:
        return None
    
    # Check if required fields exist
    if "t" not in msg or "S" not in msg:
        return None
    
    # Convert Alpaca timestamp to datetime
    event_ts = parse_alpaca_timestamp(msg["t"])
    if event_ts is None:
        return None
    
    symbol = msg["S"]
    
    # Create the event message payload
    if msg_type == "t":
        payload = TradePayload(
            price=msg["p"],
            size=msg["s"]
        )
        event = TradeEvent(
            event_type=EventType.trade,
            source=Source.alpaca,
            symbol=symbol,
            event_ts=event_ts,
            payload=payload
        )
        return event

    elif msg_type == "q":
        # Use .get() for optional fields to avoid KeyError
        bid_price = msg.get("bp", 0)
        ask_price = msg.get("ap", 0)
        
        # Validate required price fields
        if bid_price == 0 and ask_price == 0:
            return None
            
        payload = QuotePayload(
            bid_price=bid_price,
            bid_size=msg.get("bs"),
            ask_price=ask_price,
            ask_size=msg.get("as")
        )
        event = QuoteEvent(
            event_type=EventType.quote,
            source=Source.alpaca,
            symbol=symbol,
            event_ts=event_ts,
            payload=payload
        )
        return event
    else:  # msg_type in ["d", "b"] - daily or minute bars
        # Use .get() with defaults to avoid KeyError on missing fields
        open_price = msg.get("o")
        high_price = msg.get("h")
        low_price = msg.get("l")
        close_price = msg.get("c")
        
        # Validate required OHLC fields
        if None in [open_price, high_price, low_price, close_price]:
            return None
            
        payload = BarPayload(
            timeframe="1D" if msg_type == "d" else "1Min",
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=msg.get("v", 0)
        )
        event = BarEvent(
            event_type=EventType.bar,
            source=Source.alpaca,
            symbol=symbol,
            event_ts=event_ts,
            payload=payload
        )
        return event


def print_connection_info(api_key):
    """Print connection and debugging information"""
    print(f"Connecting to Alpaca Crypto stream...")
    print(f"API Key present: {bool(api_key)}")
    print(f"Current time: {datetime.now()}")

def print_message(item):
    """Pretty print received messages"""
    msg_type = item.get('T', 'unknown')
    if msg_type == 't':  # Trade
        print(f"💰 TRADE: {item.get('S')} - Price: ${item.get('p')} Size: {item.get('s')}")
    elif msg_type == 'q':  # Quote
        print(f"📊 QUOTE: {item.get('S')} - Bid: ${item.get('bp')} Ask: ${item.get('ap')}")
    else:
        print(f"📨 {msg_type}: {item}")

async def connect_to_alpaca():
    uri_stocks = "wss://stream.data.alpaca.markets/v2/iex"
    uri_crpyto = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"
    # Get credentials from environment variables
    api_key = os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("APCA_API_SECRET_KEY")

    
    async with websockets.connect(uri_crpyto) as websocket:
        # Authenticate with Alpaca
        auth_message = {
            "action": "auth",
            "key": api_key,
            "secret": api_secret
        }
        await websocket.send(json.dumps(auth_message))
        
        # Wait for auth response
        auth_response = await websocket.recv()
        auth_data = json.loads(auth_response)
        print(f"Auth response: {auth_data}")
        
        # Check if auth was successful
        if auth_data[0].get("T") == "success":
            print("Authentication successful!")
        else:
            print("Authentication failed!")
            return
        
        # Subscribe to crypto trades and quotes
        subscribe_message_crypto = {
            "action": "subscribe",
            "trades": ["BTC/USD", "ETH/USD", "SOL/USD"],
            "quotes": ["BTC/USD", "ETH/USD", "SOL/USD"],
            "bars": ["XRP/USD"]
        }

        subscribe_message_stocks = {
            "action": "subscribe",
            "trades": ["AAPL", "MSFT", "NVDA"],
            "quotes": ["ORCL"],
            "bars": ["TSLA"]
        }

        await websocket.send(json.dumps(subscribe_message_crypto))
        #print(f"Sent subscription for: BTC/USD, ETH/USD, SOL/USD")

        subscribe_response = await websocket.recv()
        sub_data = json.loads(subscribe_response)
        print(f"Subscribe response: {sub_data}")
        
        #print("\n🔄 Waiting for crypto data (24/7)...\n")

        message_count = 0
        try:
            async for message in websocket:
                data = json.loads(message)
                """
                ### TODO ###
                Create a function that can filter out quotes, trades and bars
                Have it return a simple JSON format with relevant attributes
                """
                print(data)
                event = filter_event_messages(data)
                if event:  # Only print if we got a valid event
                    # Get the appropriate Kafka topic for this event type
                    topic = get_kafka_topic(event)
                    # Send the event to Kafka
                    alpaca_producer.produce(
                        topic=topic,
                        key=event.symbol,
                        value=event.model_dump_json()
                    )
                    print(f"Sent to {topic}: {event.model_dump_json()}")
                    
                    message_count += 1
                    # Flush every 10 messages to ensure delivery
                    if message_count % 10 == 0:
                        alpaca_producer.flush()
        finally:
            # Always flush remaining messages before exiting
            print("\nFlushing remaining messages to Kafka...")
            alpaca_producer.flush()
            print("All messages delivered!")
            
asyncio.run(connect_to_alpaca())


