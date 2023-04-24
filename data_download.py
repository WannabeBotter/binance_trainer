import os
import polars as pl
import numpy as np
import datetime
import time
from pathlib import Path
import re
import requests
import threading

import tarfile
import zipfile
from io import BytesIO
from joblib import Parallel, delayed
from retrying import retry
import argparse
from urllib.parse import urlencode
import hmac
import hashlib
import gc

from target_symbols import target_symbols
from joblib_util import tqdm_joblib

from pygui_util import pygui_log
import dearpygui.dearpygui as dpg

# BinanceのAPIエンドポイント
S_URL_V1 = "https://api.binance.com/sapi/v1"

# APIキーとシークレットキー
api_key = os.environ.get("BINANCE_APIKEY")
secret_key = os.environ.get("BINANCE_APISECRET")
print(secret_key)

# ロードしたデータを格納するデータフレーム
df_events = None

# 署名用関数 Binanceのサンプルからコピーしたもの
# https://github.com/binance/binance-public-data/tree/master/Futures_Order_Book_Download
def sign(params={}):
    global secret_key

    data = params.copy()
    ts = str(int(1000 * time.time()))
    data.update({"timestamp": ts})
    h = urlencode(data)
    h = h.replace("%40", "@")
    b = bytearray()
    b.extend(secret_key.encode())
    signature = hmac.new(b, msg=h.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()
    sig = {"signature": signature}
    return data, sig

# Binance側が生成したオーダーブックの.tar.gzをダウンロードしてPolarsデータフレームを作り、parquetとして保存する関数
@retry(stop_max_attempt_number = 5, wait_fixed = 1000)
def download_orderbook_targz(symbol: str = None, startdate: datetime.datetime = None) -> None:
    assert symbol is not None
    assert startdate is not None

    # データ取得範囲の開始時間と終了時間
    _starttime = int(startdate.timestamp()) * 1000
    _endtime = int((startdate + datetime.timedelta(days=1)).timestamp()) * 1000
    _datatype = "T_DEPTH"
    _timestamp = int(time.time() * 1000)

    # 圧縮されたオーダーブックファイルのダウンロードリンク作成をリクエストする
    _params = {
        "symbol": symbol,
        "startTime": _starttime,
        "endTime": _endtime,
        "dataType": _datatype,
        "timestamp": _timestamp,
    }
    _sign = sign(_params)
    _url = f"{S_URL_V1}/futuresHistDataId?{urlencode(_sign[0])}&{urlencode(_sign[1])}"

    pygui_log(f"HTTP POST : {_url}")
    _r = requests.post(_url, headers = {"X-MBX-APIKEY": api_key}, verify = True, timeout = 60)
    
    if _r.status_code != requests.codes.ok:
        pygui_log(f"HTTP POST status : {_r.status_code}.")
        raise Exception
    
    _downloadid = _r.json()["id"]

    # ダウンロードリンクが渡されるまで何度もリトライする
    while True:
        _timestamp = str(int(1000 * time.time()))  # current timestamp which serves as an input for the params variable
        _params = {"downloadId": _downloadid, "timestamp": _timestamp}
        _sign = sign(_params)
        _url = f"{S_URL_V1}/downloadLink?{urlencode(_sign[0])}&{urlencode(_sign[1])}"

        _r = requests.get(_url, headers={"X-MBX-APIKEY": api_key}, timeout=30, verify=True)

        if _r.status_code != requests.codes.ok:
            pygui_log(f"HTTP GET status : {_r.status_code}.")
            pygui_log(_r.json())
            raise Exception

        
        if "expirationTime" not in _r.json():
            pygui_log(f"Download link isn't ready. Retry after 10 seconds.")
            time.sleep(10)
            continue
        else:
            pygui_log(f"Received a download link. {_r.json()['link']}")
            print(f"Received a download link. {_r.json()['link']}")
            break
    
    _url = _r.json()["link"]
    _r = requests.get(_url)
    if _r.status_code != requests.codes.ok:
        pygui_log(f"From response.get({_url}), received HTTP status {_r.status_code}.")
        raise Exception

    pygui_log("Download completed.")

    # Handle tar.gz file
    _fileobj = BytesIO(_r.content)
    try:
        # Process downloaded tar.gz file
        with tarfile.open(fileobj = _fileobj, mode = "r:gz") as _tarfile:
            _filenames = _tarfile.getnames()
   
            # Process .tar.gz files in downloaded tar.gz file
            for _filename in _filenames:
                _stem = Path(_filename).stem
                _orderbook_fileobj = _tarfile.extractfile(_filename)
                with tarfile.open(fileobj = _orderbook_fileobj, mode = "r:gz") as _orderbook_tarfile:
                    _orderbook_filenames = _orderbook_tarfile.getnames()

                    for _orderbook_filename in _orderbook_filenames:
                        _stem = Path(_orderbook_filename).stem
                        _orderbook_df = pl.read_csv(BytesIO(_orderbook_tarfile.extractfile(_orderbook_filename).read()),
                            skip_rows = 1,
                            new_columns = ["symbol", "timestamp", "first_update_id", "last_update_id", "side", "update_type", "price", "qty", "pu"],
                            dtypes = {"symbol": pl.Utf8, "timestamp": pl.Int64, "first_update_id": pl.Int64, "last_update_id": pl.Int64, "side": pl.Utf8, "price": pl.Float64, "qty": pl.Float64, "pu": pl.Int64})
                        
                        if not os.path.exists(f"./data/"):
                            os.makedirs(f"./data/")
                        _orderbook_df.write_parquet(f"./data/{_stem}.parquet")
                        pygui_log(f"Saved ./data/{_stem}.parquet")

                        del(_orderbook_df)
                        gc.collect()
    except Exception as e:
        pygui_log(e)
        raise e

    return

# 指定されたファイル名をもとに、.zipをダウンロードしてデータフレームを作り、pkl.gzとして保存する関数
@retry(stop_max_attempt_number = 5, wait_fixed = 1000)
def download_trades_zip(target_symbol: str = None, target_date: datetime.datetime = None) -> None:
    assert target_symbol is not None
    assert target_date is not None
    
    target_file_name = f"{target_symbol}-trades-{target_date.strftime('%Y-%m-%d')}.zip"

    _stem = Path(target_file_name).stem

    if "USD_" in target_symbol:
        _url = f'https://data.binance.vision/data/futures/cm/daily/trades/{target_symbol}/{target_file_name}'
    else:
        _url = f'https://data.binance.vision/data/futures/um/daily/trades/{target_symbol}/{target_file_name}'

    pygui_log(f"HTTP GET : {_url}")
    _r = requests.get(_url)
    pygui_log(f"HTTP GET status : {_r.status_code}.")
    
    if _r.status_code != requests.codes.ok:
        print(f"From response.get({_url}), received HTTP status {_r.status_code}.")
        time.sleep(1)
        raise Exception
    
    _csvzip = zipfile.ZipFile(BytesIO(_r.content))
    if _csvzip.testzip() != None:
        print(f'Corrupt zip file from {_url}. Retry.')
        raise Exception
    _csvraw = _csvzip.read(f'{_stem}.csv')
    
    if chr(_csvraw[0]) == 'i':
        # ヘッダーラインがあるので削除しないといけない
        _header = 1
    else:
        _header = 0
    
    try:
        _df = pl.read_csv(BytesIO(_csvraw),
                          skip_rows = _header,
                          new_columns = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"],
                          dtypes = {"id": pl.Int64, "price": pl.Float64, "qty": pl.Float64, "quote_qty": pl.Float64, "time": pl.Int64, "is_buyer_maker": pl.Boolean})
        
        # カラム名などをOrderbookのデータ形式に揃える
        _df = _df.rename({"time": "timestamp", "id": "first_update_id"})
        _df = _df.with_columns(pl.col("first_update_id").alias("last_update_id"),
                               pl.when(pl.col("is_buyer_maker") == True).then("a").otherwise("b").alias("side"),
                               (pl.col("first_update_id") - 1).alias("pu"),
                               pl.lit(target_symbol).alias("symbol"),
                               pl.lit("trade").alias("update_type"))
        _df = _df.select(["symbol", "timestamp", "first_update_id", "last_update_id", "side", "update_type", "price", "qty", "pu"])
    except Exception as e:
        print(f"polars.read_csv({_url}) returned Exception {e}.")
        raise e
    
    # Parquetファイルを書き込む
    if not os.path.exists(f"./data/"):
        os.makedirs(f"./data/")
    _df.write_parquet(f"./data/{target_symbol}_TRADES_{target_date.strftime('%Y-%m-%d')}.parquet")
    return

def download_completion(thread_trades, thread_orderbook) -> None:
    thread_trades.join()
    thread_orderbook.join()
    dpg.configure_item("button_download", enabled=True)

def download_trades_orderbook(target_symbol: str = None, target_date: datetime.datetime = None) -> None:
    assert target_symbol is not None
    assert target_date is not None
    
    pygui_log(f"Starting a download thread for {target_symbol} orderbook data on {target_date}.")
    _thread_trades = threading.Thread(target = download_trades_zip, args = (target_symbol, target_date))
    _thread_orderbook = threading.Thread(target = download_orderbook_targz, args = (target_symbol, target_date))
    _thread_waiting = threading.Thread(target = download_completion, args = (_thread_trades, _thread_orderbook))
    
    _thread_trades.start()
    _thread_orderbook.start()
    _thread_waiting.start()

# データフレームをロードする関数
def load_dataframes(target_symbol: str = None, target_date: datetime.datetime = None) -> None:
    assert target_symbol is not None
    assert target_date is not None

    global df_events
    
    _df_orderbook_update = pl.read_parquet(f"data/{target_symbol}_T_DEPTH_{target_date.strftime('%Y-%m-%d')}_depth_update.parquet").with_columns(pl.col("qty").cast(pl.Float64).alias("qty"))
    _df_orderbook_snap = pl.read_parquet(f"data/{target_symbol}_T_DEPTH_{target_date.strftime('%Y-%m-%d')}_depth_snap.parquet").with_columns(pl.col("qty").cast(pl.Float64).alias("qty"))
    _df_trades = pl.read_parquet(f"data/{target_symbol}_TRADES_{target_date.strftime('%Y-%m-%d')}.parquet").with_columns(pl.col("qty").cast(pl.Float64).alias("qty"))
    
    df_events = pl.concat([_df_orderbook_update, _df_orderbook_snap, _df_trades], how="vertical").sort(["timestamp", "update_type"])

    dpg.configure_item("button_load", enabled=True)
    dpg.configure_item("window_load", show=False)

# データフレームをロードするスレッドを起動する関数
def load_dataframes_thread(symbol: str = None, target_date: datetime.datetime = None) -> None:
    dpg.configure_item("button_load", enabled=False)
    _thread = threading.Thread(target = load_dataframes, args = (symbol, target_date))
    _thread.start()
