import asyncio
import websockets
import json
import finnhub
import os
from dotenv import load_dotenv
    
#Load environmment variables from .env file
load_dotenv()

#grab api key for finnhub
api_key = os.getenv("FINNHUB_API_KEY")

#setup finnhub client
finnhub_client = finnhub.Client(api_key=api_key)

async def connect_to_finnhub():

    uri = f"wss://ws.finnhub.io?token={api_key}"
    async with websockets.connect(uri) as websocket:

        #create the subscriber message and check if correct data is received
        subscribe_message = {
            "type": "subscribe",
            "symbol": "AAPL"
        }
        #send subscriber message
        await websocket.send(json.dumps(subscribe_message))

        subscribe_response = await websocket.recv()
        print(f"Received subscribe message response: {subscribe_response}")

        while True:
            message = await websocket.recv()
            data = json.loads(message)
            print(f"Received: {data}")
        
asyncio.run(connect_to_finnhub())
