import asyncio
import websockets
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from confluent_kafka import Producer

# Load environment variables from .env file
load_dotenv()

# create a producer and have it send messages to all three topics
alpaca_producer = Producer({'bootstrap.servers': 'localhost:9092'})



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
    uri = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"
    
    # Get credentials from environment variables
    api_key = os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("APCA_API_SECRET_KEY")

    
    async with websockets.connect(uri) as websocket:
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
        subscribe_message = {
            "action": "subscribe",
            "trades": ["BTC/USD", "ETH/USD", "SOL/USD"],
            "quotes": ["BTC/USD", "ETH/USD", "SOL/USD"],
            "bars": ["XRP/USD"]
        }
        await websocket.send(json.dumps(subscribe_message))
        print(f"Sent subscription for: BTC/USD, ETH/USD, SOL/USD")

        subscribe_response = await websocket.recv()
        sub_data = json.loads(subscribe_response)
        print(f"Subscribe response: {sub_data}")
        
        print("\n🔄 Waiting for crypto data (24/7)...\n")

        async for message in websocket:
            data = json.loads(message)
            """
            ### TODO ###
            Create a function that can filter out quotes, trades and bars
            Have it return a simple JSON format with relevant attributes
            """
            print(data)
            
asyncio.run(connect_to_alpaca())