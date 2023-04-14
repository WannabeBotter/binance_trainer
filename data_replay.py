import polars as pl
import numpy as np
import time
import datetime
import threading
import pygui_util
import dearpygui.dearpygui as dpg
import data_download

import random

def run_replay() -> None:
    df_orderbook_update = data_download.df_orderbook_update
    df_trades = data_download.df_trades
    
    _df_emptybook = pl.DataFrame(
    {
        "price": pl.Series("price", dtype=pl.Float64),
        "side": pl.Series("side", dtype=pl.Utf8),
        "qty": pl.Series("qty", dtype=pl.Float64),
    })
    _df_orderbook = _df_emptybook.clone()    
    _idx = 0

    while True:
        # timestampとupdate_typeが同じ行を取得する
        _idx_start = _idx
        _timestamp = df_orderbook_update[_idx_start, "timestamp"]
        _update_type = df_orderbook_update[_idx_start, "update_type"]
        while _idx < df_orderbook_update.shape[0] and df_orderbook_update[_idx, "timestamp"] == _timestamp and df_orderbook_update[_idx, "update_type"] == _update_type:
            _idx = _idx + 1
        
        _df = df_orderbook_update[_idx_start:_idx, ["side", "timestamp", "price", "qty"]]

        # スナップショットの場合は板情報をリセットする
        if _update_type == "snap":
            print("Applying snapshot")
            _df_orderbook = _df_emptybook.clone()

        # BidとAskの板情報をそれぞれ計算して価格順にソートしてから結合する
        _df_joined_orderbook = _df_orderbook.join(_df, on=["side", "price"], how="outer").sort(["timestamp"], descending=False)
        _df_joined_orderbook = _df_joined_orderbook.unique(subset=["side", "price"], keep="last")
        _df_orderbook = _df_joined_orderbook.select([
            pl.col("side"),
            pl.col("price"),
            pl.coalesce(["qty_right", "qty"]).alias("qty")]).filter(pl.col("qty") > 0)

        _df_orderbook_bid = _df_orderbook.filter(pl.col("side") == "b").sort("price")
        _df_orderbook_ask = _df_orderbook.filter(pl.col("side") == "a").sort("price")
        
        # BidとAskの板情報が空の場合はスキップする
        if _df_orderbook_bid.shape[0] == 0 or _df_orderbook_ask.shape[0] == 0:
            continue

        _best_bid = _df_orderbook_bid[-1, "price"]
        _best_ask = _df_orderbook_ask[0, "price"]
        _mid_price = (_best_bid + _best_ask) / 2
        df_orderbook = pl.concat([_df_orderbook_bid, _df_orderbook_ask], how="vertical")

        # 次の行と0.1秒単位の時間の繰り上がりがない場合は表示をせず次の行へ進む (timestampはミリ秒単位)
        if _idx < df_orderbook_update.shape[0] - 1:
            if _timestamp // 100 == df_orderbook_update[_idx, "timestamp"] // 100:
                continue
        
        # テクスチャの更新 (Ask側)
        _bin_edges = np.arange(start=_mid_price, stop=_mid_price + 60, step=1)
        _df_edges = pl.DataFrame({"break_point": _bin_edges, "qty_sum": 0})
        _cut_result = _df_orderbook_ask["price"].cut(_bin_edges, category_label="price_bin", maintain_order=True)
        _df_orderbook_ask = _df_orderbook_ask.join(_cut_result, on="price", how="left")
        _df_orderbook_ask = _df_orderbook_ask.filter(~pl.col("price_bin").cast(pl.Utf8).str.contains("inf"))
        _ask_result = _df_orderbook_ask.groupby("break_point").agg([pl.sum("qty").alias("qty_sum")]).join(_df_edges, on="break_point", how="outer").select([
            pl.col("break_point"),
            pl.coalesce(["qty_sum", "qty_sum_right"]).alias("qty_sum")])
        _ask_result = _ask_result.with_columns(pl.when(pl.col("qty_sum") > 50).then(1.0).otherwise(pl.col("qty_sum") / 50.0).alias("qty_sum")).sort("break_point", descending=True)

        # テクスチャの更新 (Bid側)
        _bin_edges = np.arange(start=_mid_price - 60, stop=_mid_price, step=1)
        _df_edges = pl.DataFrame({"break_point": _bin_edges, "qty_sum": 0})
        _cut_result = _df_orderbook_bid["price"].cut(_bin_edges, category_label="price_bin", maintain_order=True)
        _df_orderbook_bid = _df_orderbook_bid.join(_cut_result, on="price", how="left")
        _df_orderbook_bid = _df_orderbook_bid.filter(~pl.col("price_bin").cast(pl.Utf8).str.contains("inf"))
        _bid_result = _df_orderbook_bid.groupby("break_point").agg([pl.sum("qty").alias("qty_sum")]).join(_df_edges, on="break_point", how="outer").select([
            pl.col("break_point"),
            pl.coalesce(["qty_sum", "qty_sum_right"]).alias("qty_sum")])
        _bid_result = _bid_result.with_columns(pl.when(pl.col("qty_sum") > 50).then(1.0).otherwise(pl.col("qty_sum") / 50.0).alias("qty_sum")).sort("break_point", descending=True)

        _orderbook_heatmap_texture = pygui_util.orderbook_heatmap_texture
        _orderbook_heatmap_texture = np.roll(_orderbook_heatmap_texture, shift=-1, axis=1)
        _orderbook_heatmap_texture[0:60,-1, 0] = _ask_result["qty_sum"].to_numpy()
        _orderbook_heatmap_texture[0:60,-1, 3] = 1.0
        _orderbook_heatmap_texture[60:120,-1, 2] = _bid_result["qty_sum"].to_numpy()
        _orderbook_heatmap_texture[60:120,-1, 3] = 1.0
        dpg.set_value("orderbook_heatmap_texture", _orderbook_heatmap_texture)
        pygui_util.orderbook_heatmap_texture = _orderbook_heatmap_texture
        
        # バーグラフの表示のアップデート
        _qty_list = df_orderbook["qty"].to_list()
        _price_list = df_orderbook["price"].to_list()
        _series_data = [_qty_list, _price_list]
        dpg.set_value("series_ask", _series_data)
        dpg.set_axis_limits("ask_plot_yaxis", _mid_price - 50, _mid_price + 50)

        _timestamp_sec = _timestamp / 1000.0
        _date = datetime.datetime.fromtimestamp(_timestamp_sec)
        dpg.set_value("text_current_time", _date.strftime("%Y-%m-%d %H:%M:%S.%f"))

        time.sleep(0.0000001)
        if _idx >= df_orderbook_update.shape[0]:
            break

# リプレイをするスレッドを起動する関数
def run_replay_thread() -> None:
    _thread = threading.Thread(target = run_replay)
    _thread.start()
