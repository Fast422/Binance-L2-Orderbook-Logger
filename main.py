import asyncio
import websockets
import json
import pandas as pd
import time
import os
from datetime import datetime, timezone, timedelta


class OrderBookDataCollector:
    """
    Collects order book data from Binance WebSocket, buffers it, and periodically writes to compressed CSV files.
    Handles file rollover at midnight UTC and supports live printing of order book snapshots.
    """
    def __init__(self, url, symbol):
        """
        Initialize the collector with WebSocket URL and trading symbol.
        """
        self.url = url
        self.symbol = symbol.upper()
        self.order_book = None  # Latest order book snapshot
        self.last_update = None  # Timestamp of last update (string)
        self.running = True  # Control flag for async loops
        self.buffer = []  # Buffer for order book rows
        self.last_flush = time.time()  # Last time buffer was flushed
        self.current_date = self.get_current_date()  # Current date string (YYYY-MM-DD)
        self.last_rollover = time.time()  # Last time file rollover occurred


    def get_current_date(self):
        """
        Returns the current date in UTC as a string (YYYY-MM-DD).
        """
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')


    def get_filepath(self):
        """
        Returns the file path for the current day's compressed CSV file.
        Creates the directory if it does not exist.
        """
        date_str = self.get_current_date()
        dir_path = os.path.join('data', self.symbol)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, f'{date_str}.csv.gz')


    def buffer_snapshot(self, data):
        """
        Extracts top 10 bids and asks from the order book update and appends them to the buffer.
        Each row includes timestamp, price, quantity, side, level, and update_id.
        """
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
        """
        Writes the buffered order book rows to the current day's compressed CSV file.
        Appends to the file and writes the header if the file is new.
        Clears the buffer after writing.
        """
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
        """
        Flushes the buffer and updates the current date for file rollover (e.g., at midnight UTC).
        """
        self.flush_buffer()
        self.current_date = self.get_current_date()
        print(f'Rollover: New file for date {self.current_date}')
        self.last_rollover = time.time()

    async def receive_data(self):
        """
        Connects to the Binance WebSocket and receives order book updates.
        Buffers each update, handles file rollover at midnight, and flushes buffer every 60 seconds.
        """
        print("Connecting to Binance WebSocket...")
        print("Connected!")
        while self.running:
            try:
                async with websockets.connect(self.url) as websocket:
                    while self.running:
                        try:
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
                        except asyncio.CancelledError:
                            print("receive_data cancelled.")
                            raise
                        except Exception as e:
                            print(f"Error in receive_data inner loop: {e}")
                            break
            except asyncio.CancelledError:
                print("receive_data cancelled (outer).")
                raise
            except (websockets.ConnectionClosed, websockets.InvalidStatusCode, OSError) as e:
                print(f"WebSocket connection lost: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Unexpected error in receive_data: {e}")
                await asyncio.sleep(5)


    async def print_data(self):
        """
        Periodically prints the latest order book snapshot (every second).
        Only prints the last update time for brevity.
        """
        while self.running:
            if self.order_book:
                bids = self.order_book.get("b")
                asks = self.order_book.get("a")
                if bids is not None and asks is not None:
                    bids_df = pd.DataFrame(bids, columns=["Price", "Quantity"]).astype(float)
                    asks_df = pd.DataFrame(asks, columns=["Price", "Quantity"]).astype(float)
                    # Uncomment below for more detailed output
                    # print("\n--- Order Book Snapshot ---")
                    print(f"Last update: {self.last_update}")
                    # print("Top 10 Bids:")
                    # print(bids_df.head(10).to_string(index=False))
                    # print("Top 10 Asks:")
                    # print(asks_df.head(10).to_string(index=False))
            await asyncio.sleep(1)


    async def run(self):
        """
        Runs the data receiver and printer concurrently.
        """
        await asyncio.gather(
            self.receive_data(),
            self.print_data()
        )


# Entry point for running the collector as a script
if __name__ == "__main__":
    symbol = "BTCUSDT"
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@depth20@100ms"
    collector = OrderBookDataCollector(url, symbol)
    asyncio.run(collector.run())