import polars as pl
import numpy as np
import time
import datetime
import threading
import dearpygui.dearpygui as dpg
from pygui_util import pygui_log
import data_download

# Orderbookのヒートマップ表示用のテクスチャ (価格の上下に対応するために縦に長い)
tall_orderbook_heatmap_texture = np.zeros((240, 120, 3), dtype=np.float32)

# 最新のmid_price
mid_price = np.inf

# 一つ前のmid_price
prev_mid_price = np.inf

def get_mid_price() -> np.float64:
    global mid_price
    print(f"mid_price: {mid_price}")
    return mid_price

def run_replay() -> None:
    global tall_orderbook_heatmap_texture, mid_price, prev_mid_price

    _df_events = data_download.df_events
    
    _df_emptybook = pl.DataFrame(
    {
        "price": pl.Series("price", dtype=pl.Float64),
        "side": pl.Series("side", dtype=pl.Utf8),
        "qty": pl.Series("qty", dtype=pl.Float64),
    })
    _dict_orderbook_bid = {} # 買い板
    _dict_orderbook_ask = {} # 売り板
    _dict_trades = {} # トレード出来高
    _idx = 0

    while True:
        # _df_eventsの_idx行目と、timestampが同じ行を全て取得する
        _idx_start = _idx
        _timestamp = _df_events[_idx_start, "timestamp"]
        _date = datetime.datetime.fromtimestamp(_timestamp / 1000.0)
        _date_str = _date.strftime('%Y-%m-%d %H:%M:%S.%f')

        _orderbook_reset = False

        while _idx < _df_events.shape[0] and _df_events[_idx, "timestamp"] == _timestamp:
            _update_type = _df_events[_idx, "update_type"]

            # Orderbookのリセット処理
            if _update_type != "snap":
                _orderbook_reset = False
            elif _update_type == "snap" and _orderbook_reset == False:
                pygui_log(f"{_date_str} : Resetting orderbook")
                _dict_orderbook_bid = {}
                _dict_orderbook_ask = {}
                _orderbook_reset = True
            
            if _update_type == "set" or _update_type == "snap":
                if _df_events[_idx, "side"] == "a":
                    _dict_orderbook_ask[_df_events[_idx, "price"]] = _df_events[_idx, "qty"]
                else:
                    _dict_orderbook_bid[_df_events[_idx, "price"]] = _df_events[_idx, "qty"]
            
            # トレード情報についてはBid / Askを区別しない
            if _update_type == "trade":
                if _df_events[_idx, "price"] not in _dict_trades:
                    _dict_trades[_df_events[_idx, "price"]] = _df_events[_idx, "qty"]
                else:
                    _dict_trades[_df_events[_idx, "price"]] += _df_events[_idx, "qty"]

            _idx = _idx + 1
        
        # 次の行と0.1秒単位の時間の繰り上がりがない場合は描画処理をせず次の行へ進む (timestampはミリ秒単位)
        if _idx < _df_events.shape[0] - 1:
            if _timestamp // 100 == _df_events[_idx, "timestamp"] // 100:
                continue

        # 板情報がbid/ask両方分ない場合は、描画処理を行わない
        if len(_dict_orderbook_bid) == 0 or len(_dict_orderbook_ask) == 0:
            continue

        _df_orderbook_bid = pl.DataFrame({"price": list(_dict_orderbook_bid.keys()), "qty": list(_dict_orderbook_bid.values())}).filter(pl.col("qty") > 0).sort("price")
        _df_orderbook_ask = pl.DataFrame({"price": list(_dict_orderbook_ask.keys()), "qty": list(_dict_orderbook_ask.values())}).filter(pl.col("qty") > 0).sort("price")

        # Best bid / Best ask / Mid priceの更新
        _best_bid = _df_orderbook_bid[-1, "price"]
        _best_ask = _df_orderbook_ask[0, "price"]
        mid_price = (_best_bid + _best_ask) / 2
        if prev_mid_price == np.inf:
            prev_mid_price = mid_price
        
        # テクスチャ生成のためのオーダーブックDataFrameの作成
        _df_binned_orderbook = None
        _df_orderbook = pl.concat([_df_orderbook_bid, _df_orderbook_ask], how="vertical")
        
        # テクスチャの更新のために、注文数量を1ドル単位で集計する
        _bin_edges = np.arange(start=mid_price - 60, stop=mid_price + 60, step=1)
        _df_edges = pl.DataFrame({"break_point": _bin_edges, "qty_sum": 0})
        _df_orderbook_cut = _df_orderbook["price"].cut(_bin_edges, category_label="price_bin", maintain_order=True)
        _df_orderbook = _df_orderbook.join(_df_orderbook_cut, on="price", how="left")

        # price_binにinfが含まれる行を削除する
        _df_orderbook = _df_orderbook.filter(~pl.col("price_bin").cast(pl.Utf8).str.contains("inf"))

        # binごとに注文数量を集計する (_df_orderbookに含まれていないbreak_pointについてはqty_sumを0にする)
        _df_binned_orderbook = _df_orderbook.groupby("break_point").agg([pl.sum("qty").alias("qty_sum")]).join(_df_edges, on="break_point", how="outer").select([
            pl.col("break_point"),
            pl.coalesce(["qty_sum", "qty_sum_right"]).alias("qty_sum")])

        # テクスチャ描画のために、一定以上のqty_sumの場合は1.0にする
        _df_binned_orderbook = _df_binned_orderbook.with_columns(pl.when(pl.col("qty_sum") >= 50).then(1.0).otherwise(pl.col("qty_sum") / 50.0).alias("qty_normalized")).sort("break_point", descending=True)

        # テクスチャ生成のためのトレードDataFrameの作成
        _df_binned_trades = None

        if len(_dict_trades) > 0:
            _df_trades = pl.DataFrame({"price": list(_dict_trades.keys()), "qty": list(_dict_trades.values())}).filter(pl.col("qty") > 0).sort("price")
            _df_trades_cut = _df_trades["price"].cut(_bin_edges, category_label="price_bin", maintain_order=True)
            _df_trades = _df_trades.join(_df_trades_cut, on="price", how="left")

            # price_binにinfが含まれる行を削除する
            _df_trades = _df_trades.filter(~pl.col("price_bin").cast(pl.Utf8).str.contains("inf"))

            # binごとに注文数量を集計する (_df_orderbookに含まれていないbreak_pointについてはqty_sumを0にする)
            _df_binned_trades = _df_trades.groupby("break_point").agg([pl.sum("qty").alias("qty_sum")]).join(_df_edges, on="break_point", how="outer").select([
                pl.col("break_point"),
                pl.coalesce(["qty_sum", "qty_sum_right"]).alias("qty_sum")])

            # テクスチャ描画のために、一定以上のqty_sumの場合は1.0にする
            #print(_df_binned_trades.filter(pl.col("qty_sum") > 0))
            _df_binned_trades = _df_binned_trades.with_columns(pl.when(pl.col("qty_sum") >= 2).then(1.0).otherwise(pl.col("qty_sum") / 2.0).alias("qty_normalized")).sort("break_point", descending=True)

        # テクスチャの更新

        # 時間が経過したので、列方向にシフト
        tall_orderbook_heatmap_texture = np.roll(tall_orderbook_heatmap_texture, shift=-1, axis=1)

        # mid_priceの変化1ドルごとに行方向にシフト
        _shift = round(mid_price) - round(prev_mid_price)
        tall_orderbook_heatmap_texture = np.roll(tall_orderbook_heatmap_texture, shift=_shift, axis=0)
        prev_mid_price = mid_price

        # 最新のオーダーブックの内容をテクスチャの右端に追加)
        tall_orderbook_heatmap_texture[:, -1, :] = 0.0
        tall_orderbook_heatmap_texture[60:120, -1, 0] = np.squeeze(_df_binned_orderbook[0:60, "qty_normalized"].to_numpy())
        if _df_binned_trades is not None:
            tall_orderbook_heatmap_texture[60:180, -1, 1] = np.squeeze(_df_binned_trades[0:120, "qty_normalized"].to_numpy())
        tall_orderbook_heatmap_texture[120:180, -1, 2] = np.squeeze(_df_binned_orderbook[60:120, "qty_normalized"].to_numpy())
        dpg.set_value("orderbook_heatmap_texture", tall_orderbook_heatmap_texture[60:180, :, :])
        
        # バーグラフの表示のアップデート
        #_qty_list = _df_orderbook["qty"].to_list()
        #_price_list = _df_orderbook["price"].to_list()
        #_series_data = [_qty_list, _price_list]
        #dpg.set_value("series_ask", _series_data)
        #dpg.set_axis_limits("ask_plot_yaxis", mid_price - 60, mid_price + 60)

        # テキストの表示のアップデート
        dpg.set_value("text_current_time", f"{_date_str}")
        dpg.set_value("text_mid_price", f"Mid price : {mid_price:.2f}")

        # 描画が終わったのでトレード出来高情報をクリア
        _dict_trades = {}

        time.sleep(0.001)

        # リプレイが終了したらループを抜ける
        if _idx >= _df_events.shape[0]:
            break

# リプレイをするスレッドを起動する関数
def run_replay_thread() -> None:
    _thread = threading.Thread(target = run_replay)
    _thread.start()
