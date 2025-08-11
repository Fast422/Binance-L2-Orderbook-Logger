import os
import gzip
import pandas as pd
import pytest
import time
from datetime import datetime, timezone, timedelta
from main import OrderBookDataCollector

def make_fake_data(ts, update_id, bids=None, asks=None):
    return {
        'e': 'depthUpdate',
        'E': ts,
        's': 'BTCUSDT',
        'U': update_id,
        'u': update_id + 5,
        'b': bids if bids is not None else [[f'{119000 + i:.8f}', f'{1 + i:.8f}'] for i in range(10)],
        'a': asks if asks is not None else [[f'{120000 + i:.8f}', f'{2 + i:.8f}'] for i in range(10)]
    }

def test_rollover_creates_new_files(tmp_path):
    symbol = "TESTCOIN"
    orig_dir = os.getcwd()
    os.chdir(tmp_path)
    try:
        collector = OrderBookDataCollector(url="", symbol=symbol)
        base_date = datetime(2025, 8, 11, tzinfo=timezone.utc)
        minute_holder = {'minute': 0}
        def fake_get_current_date():
            return (base_date + timedelta(days=minute_holder['minute'])).strftime('%Y-%m-%d')
        collector.get_current_date = fake_get_current_date
        start = int(time.time() * 1000)
        for minute in range(2):
            minute_holder['minute'] = minute
            for sec in range(60):
                ts = start + (minute * 60 + sec) * 1000
                data = make_fake_data(ts, update_id=minute*60+sec)
                collector.buffer_snapshot(data)
            collector.rollover_file()
        files = list((tmp_path / 'data' / symbol).glob('*.csv.gz'))
        assert len(files) >= 2, f"Expected at least 2 files, found {len(files)}: {files}"
        for f in files:
            with gzip.open(f, 'rt') as fin:
                df = pd.read_csv(fin)
                assert not df.empty, f"File {f} is empty!"
    finally:
        os.chdir(orig_dir)

def test_buffer_snapshot_and_flush(tmp_path):
    symbol = "TESTCOIN"
    os.chdir(tmp_path)
    collector = OrderBookDataCollector(url="", symbol=symbol)
    ts = 1234567890
    data = make_fake_data(ts, 1)
    collector.buffer_snapshot(data)
    assert len(collector.buffer) == 20
    collector.flush_buffer()
    files = list((tmp_path / 'data' / symbol).glob('*.csv.gz'))
    assert len(files) == 1
    with gzip.open(files[0], 'rt') as fin:
        df = pd.read_csv(fin)
        assert not df.empty
        assert set(['timestamp','price','quantity','side','level','update_id']).issubset(df.columns)

def test_empty_bids_asks(tmp_path):
    symbol = "TESTCOIN"
    os.chdir(tmp_path)
    collector = OrderBookDataCollector(url="", symbol=symbol)
    ts = 1234567890
    data = make_fake_data(ts, 1, bids=[], asks=[])
    collector.buffer_snapshot(data)
    assert len(collector.buffer) == 0
    collector.flush_buffer()
    files = list((tmp_path / 'data' / symbol).glob('*.csv.gz'))
    assert len(files) == 0

def test_missing_keys(tmp_path):
    symbol = "TESTCOIN"
    os.chdir(tmp_path)
    collector = OrderBookDataCollector(url="", symbol=symbol)
    ts = 1234567890
    data = {'E': ts, 'u': 1}
    collector.buffer_snapshot(data)
    assert len(collector.buffer) == 0
    collector.flush_buffer()
    files = list((tmp_path / 'data' / symbol).glob('*.csv.gz'))
    assert len(files) == 0

def test_multiple_flushes(tmp_path):
    symbol = "TESTCOIN"
    os.chdir(tmp_path)
    collector = OrderBookDataCollector(url="", symbol=symbol)
    ts = 1234567890
    data1 = make_fake_data(ts, 1)
    collector.buffer_snapshot(data1)
    collector.flush_buffer()
    data2 = make_fake_data(ts+1, 2)
    collector.buffer_snapshot(data2)
    collector.flush_buffer()
    files = list((tmp_path / 'data' / symbol).glob('*.csv.gz'))
    assert len(files) == 1
    with gzip.open(files[0], 'rt') as fin:
        df = pd.read_csv(fin)
        assert len(df) == 40
