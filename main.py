import asyncio
import websockets
import json
import pandas as pd
import time
import os
from datetime import datetime, timezone, timedelta

class OrderBookDataCollector:
    def __init__(self, url, symbol):
        self.url = url
        self.symbol = symbol.upper()
        self.order_book = None
        self.last_update = None
        self.running = True
        self.buffer = []
        self.last_flush = time.time()
        self.current_date = self.get_current_date()
        self.last_rollover = time.time()

    def get_current_date(self):
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def get_filepath(self):
        date_str = self.get_current_date()
        dir_path = os.path.join('data', self.symbol)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, f'{date_str}.csv.gz')

    def buffer_snapshot(self, data):
        timestamp = data.get('E')
        update_id = data.get('u')
        bids = data.get('b', [])
        asks = data.get('a', [])
        # Top 10 bids
        for i, (price, qty) in enumerate(bids[:10], 1):
            self.buffer.append({
                'timestamp': timestamp,
                'price': price,
                'quantity': qty,
                'side': 'bid',
                'level': i,
                'update_id': update_id
            })
        # Top 10 asks
        for i, (price, qty) in enumerate(asks[:10], 1):
            self.buffer.append({
                'timestamp': timestamp,
                'price': price,
                'quantity': qty,
                'side': 'ask',
                'level': i,
                'update_id': update_id
            })

    def flush_buffer(self):
        if not self.buffer:
            return
        df = pd.DataFrame(self.buffer)
        filepath = self.get_filepath()
        write_header = not os.path.exists(filepath)
        df.to_csv(filepath, mode='a', header=write_header, index=False, compression='gzip')
        print(f'Flushed {len(self.buffer)} rows to {filepath}')
        self.buffer = []
        self.last_flush = time.time()

    def rollover_file(self):
        self.flush_buffer()
        self.current_date = self.get_current_date()
        print(f'Rollover: New file for date {self.current_date}')
        self.last_rollover = time.time()

    async def receive_data(self):
        print("Connecting to Binance WebSocket...")
        async with websockets.connect(self.url) as websocket:
            print("Connected!")
            while self.running:
                message = await websocket.recv()
                data = json.loads(message)
                self.order_book = data
                self.last_update = time.strftime('%Y-%m-%d %H:%M:%S')
                self.buffer_snapshot(data)
                # Rollover at midnight UTC
                new_date = self.get_current_date()
                if new_date != self.current_date:
                    self.rollover_file()
                # Flush every 60 seconds
                if time.time() - self.last_flush > 60:
                    self.flush_buffer()

    async def print_data(self):
        while self.running:
            if self.order_book:
                bids = self.order_book.get("b")
                asks = self.order_book.get("a")
                if bids is not None and asks is not None:
                    bids_df = pd.DataFrame(bids, columns=["Price", "Quantity"]).astype(float)
                    asks_df = pd.DataFrame(asks, columns=["Price", "Quantity"]).astype(float)
                    # print("\n--- Order Book Snapshot ---")
                    print(f"Last update: {self.last_update}")
                    # print("Top 10 Bids:")
                    # print(bids_df.head(10).to_string(index=False))
                    # print("Top 10 Asks:")
                    # print(asks_df.head(10).to_string(index=False))
            await asyncio.sleep(1)

    async def run(self):
        await asyncio.gather(
            self.receive_data(),
            self.print_data()
        )

if __name__ == "__main__":
    symbol = "BTCUSDT"
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth"
    collector = OrderBookDataCollector(url, symbol)
    asyncio.run(collector.run())