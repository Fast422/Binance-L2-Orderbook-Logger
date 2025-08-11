import os
import gzip
import pandas as pd
import shutil
import time
from datetime import datetime, timezone
import pytest
from main import OrderBookDataCollector

def make_fake_data(ts, update_id):
    # Simulate a Binance depth update message
    return {
        'E': ts,
        'u': update_id,
        'b': [[str(10000 + i), str(1 + i)] for i in range(10)],
        'a': [[str(11000 + i), str(2 + i)] for i in range(10)]
    }

def test_rollover_creates_new_files(tmp_path):
    symbol = "TESTCOIN"
    # Patch data dir to tmp_path
    orig_dir = os.getcwd()
    os.chdir(tmp_path)
    try:
        collector = OrderBookDataCollector(url="", symbol=symbol, test_mode=True)
        # Simulate 2+ minutes of data, 1 update per second
        start = int(time.time() * 1000)
        for minute in range(2):
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
