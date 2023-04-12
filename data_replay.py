import polars as pl
import numpy as np
import time
import datetime
import threading
from pygui_util import pygui_log, pygui_label_id_map
import dearpygui.dearpygui as dpg

import random

@profile
def run_replay() -> None:
    global df_orderbook_update, df_orderbook_snapshot, df_trades, df_orderbook

    _idx = 0

    while True:
        if False:
            _idx_start = _idx
            _timestamp = df_orderbook_update[_idx_start, "timestamp"]
            while _idx < df_orderbook_update.shape[0] and df_orderbook_update[_idx, "timestamp"] == _timestamp:
                _idx = _idx + 1
            
            _df = df_orderbook_update[_idx_start:_idx, ["side", "timestamp", "price", "qty"]]

            _df_joined_orderbook = df_orderbook.join(_df, on=["side", "price"], how="outer").sort(["timestamp"], descending=False)
            _df_joined_orderbook = _df_joined_orderbook.unique(subset=["side", "price"], keep="last")
            df_orderbook = _df_joined_orderbook.select([
                pl.col("side"),
                pl.col("price"),
                pl.coalesce(["qty_right", "qty"]).alias("qty")]).filter(pl.col("qty") > 0)

            _df_orderbook_bid = df_orderbook.filter(pl.col("side") == "b").sort("price").tail(50)
            _df_orderbook_ask = df_orderbook.filter(pl.col("side") == "a").sort("price").head(50).with_columns((-pl.col("qty")).alias("qty"))

            if _df_orderbook_bid.shape[0] == 0 or _df_orderbook_ask.shape[0] == 0:
                continue

            _best_bid = _df_orderbook_bid[-1, "price"]
            _best_ask = _df_orderbook_ask[0, "price"]
            df_orderbook = pl.concat([_df_orderbook_bid, _df_orderbook_ask], how="vertical")
            _df_orderbook_display = df_orderbook.with_columns((pl.col("price") - (_best_bid + _best_ask) / 2).alias("price_diff"))
            
            _qty_list = _df_orderbook_display["qty"].to_list()
            _price_list = _df_orderbook_display["price_diff"].to_list()
            _series_data = [_qty_list, _price_list]
            dpg.set_value("series_ask", _series_data)
            dpg.set_axis_limits("ask_plot_xaxis", -10, 10)
            dpg.set_axis_limits("ask_plot_yaxis", -25, 25)
            #dpg.fit_axis_data("ask_plot_xaxis")
            #dpg.fit_axis_data("ask_plot_yaxis")
            time.sleep(0.01)
            if _idx >= df_orderbook_update.shape[0]:
                break
        else:
            while True:
                _series_data = [[random.uniform(-10, 10)], [0]]
                dpg.set_value("series_ask", _series_data)
                dpg.set_axis_limits("ask_plot_xaxis", -10, 10)
                dpg.set_axis_limits("ask_plot_yaxis", -25, 25)
                time.sleep(0.01)
        
def load_dataframes(symbol: str = None, target_date: datetime.datetime = None) -> None:
    assert symbol is not None
    assert target_date is not None

    global df_orderbook_update, df_orderbook_snapshot, df_trades, df_orderbook
    df_orderbook_update = pl.read_parquet(f"data/{symbol}_T_DEPTH_{target_date.strftime('%Y-%m-%d')}_depth_update.parquet").sort("timestamp")
    df_orderbook_snap = pl.read_parquet(f"data/{symbol}_T_DEPTH_{target_date.strftime('%Y-%m-%d')}_depth_snap.parquet").sort("timestamp")
    df_trades = pl.read_parquet(f"data/{symbol}-trades-{target_date.strftime('%Y-%m-%d')}.parquet").sort("time")
    
    df_orderbook = pl.DataFrame(
        {
            "price": pl.Series("price", dtype=pl.Float64),
            "side": pl.Series("side", dtype=pl.Utf8),
            "qty": pl.Series("qty", dtype=pl.Float64),
        })
    
    run_replay()

    dpg.configure_item("button_load", enabled=True)

def load_trades_orderbook(symbol: str = None, target_date: datetime.datetime = None) -> None:
    _thread = threading.Thread(target = load_dataframes, args = (symbol, target_date))
    _thread.start()
