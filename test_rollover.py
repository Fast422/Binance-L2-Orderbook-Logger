import os
import gzip
import pandas as pd
import shutil
import time
from datetime import datetime, timezone, timedelta
import pytest
from main import OrderBookDataCollector

def make_fake_data(ts, update_id):
    # Simulate a Binance depth update message with all real keys
    return {
        'e': 'depthUpdate',
        'E': ts,
        's': 'TESTCOIN',
        'U': update_id,  # first update ID in event
        'u': update_id + 5,  # final update ID in event (arbitrary)
        'b': [[f'{119000 + i:.8f}', f'{1 + i:.8f}'] for i in range(10)],
        'a': [[f'{120000 + i:.8f}', f'{2 + i:.8f}'] for i in range(10)]
    }

def test_rollover_creates_new_files(tmp_path):
    symbol = "TESTCOIN"
    orig_dir = os.getcwd()
    os.chdir(tmp_path)
    try:
        collector = OrderBookDataCollector(url="", symbol=symbol)
        # Patch get_current_date to simulate date change by minute
        base_date = datetime(2025, 8, 11, tzinfo=timezone.utc)
        # We'll use a mutable [minute] so the lambda can see the current value
        minute_holder = {'minute': 0}
        def fake_get_current_date():
            return (base_date + timedelta(days=minute_holder['minute'])).strftime('%Y-%m-%d')
        collector.get_current_date = fake_get_current_date
        # Simulate 2 minutes of data, 1 update per second
        start = int(time.time() * 1000)
        for minute in range(2):
            minute_holder['minute'] = minute
            for sec in range(60):
                ts = start + (minute * 60 + sec) * 1000
                data = make_fake_data(ts, update_id=minute*60+sec)
                collector.buffer_snapshot(data)
            collector.rollover_file()  # Simulate rollover at each minute
        # Check that two files exist with different dates
        files = list((tmp_path / 'data' / symbol).glob('*.csv.gz'))
        assert len(files) >= 2, f"Expected at least 2 files, found {len(files)}: {files}"
        # Check that each file has data
        for f in files:
            with gzip.open(f, 'rt') as fin:
                df = pd.read_csv(fin)
                assert not df.empty, f"File {f} is empty!"
    finally:
        os.chdir(orig_dir)
