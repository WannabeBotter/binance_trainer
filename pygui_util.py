from datetime import datetime
import numpy as np
import dearpygui.dearpygui as dpg

# 定数
TEXTURE_WIDTH = 256
TEXTURE_HEIGHT = TEXTURE_WIDTH
TEXTURE_CHANNELS = 3

# ログウィンドウにログ出力をする関数
def pygui_log(text: str) -> None:
    _log_text = dpg.get_value("text_log")
    _log_text += f"{text}\n"
    dpg.set_value("text_log", _log_text)

    _y_scroll_max = dpg.get_y_scroll_max("window_log")
    dpg.set_y_scroll("window_log", _y_scroll_max)

from data_download import download_trades_orderbook, load_dataframes_thread
from data_replay import run_replay_thread

# Orderbookのヒートマップ表示用のテクスチャ
orderbook_heatmap_texture = np.zeros((TEXTURE_WIDTH, TEXTURE_HEIGHT, TEXTURE_CHANNELS), dtype=np.float32)

# ダウンロードメニューを押したときのコールバック関数
def download_menu_callback(sender, app_data, user_data):
    _show = dpg.get_item_configuration("window_download")["show"]
    dpg.configure_item("window_download", show=True)

# ダウンロードボタンを押した時のコールバック関数
def download_button_callback(sender, app_data, user_data):
    _symbol = dpg.get_value("text_symbol").upper()
    _startdate = dpg.get_value("text_startdate")
    
    _startdatetime = datetime.strptime(_startdate, "%Y-%m-%d")

    dpg.configure_item("button_download", enabled=False)
    download_trades_orderbook(_symbol, _startdatetime)

# ロードメニューを押したときのコールバック関数
def load_menu_callback(sender, app_data, user_data):
    _show = dpg.get_item_configuration("window_load")["show"]
    dpg.configure_item("window_load", show=True)

# ロードボタンを押した時のコールバック関数
def load_button_callback(sender, app_data, user_data):
    _symbol = dpg.get_value("text_symbol2").upper()
    _startdate = dpg.get_value("text_startdate2")
    
    _startdatetime = datetime.strptime(_startdate, "%Y-%m-%d")

    dpg.configure_item("button_load", enabled=False)
    load_dataframes_thread(_symbol, _startdatetime)

# リプレイメニューを押したときのコールバック関数
def replay_menu_callback(sender, app_data, user_data):
    _show = dpg.get_item_configuration("window_replay")["show"]
    dpg.configure_item("window_replay", show=True)
    run_replay_thread()

# デバッグログウィンドウを表示するコールバック関数
def debug_log_menu_callback(sender, app_data, user_data):
    _show = dpg.get_item_configuration("window_debug")["show"]
    dpg.configure_item("window_debug", show=not _show)

def pygui_init() -> None:
    dpg.create_context()
    dpg.create_viewport(title='Binance Trainer', width=800, height=600)

    try:
        with dpg.viewport_menu_bar():
            with dpg.menu(label="Data"):
                dpg.add_menu_item(label="Load", callback=load_menu_callback)
                dpg.add_menu_item(label="Download", callback=download_menu_callback)
            with dpg.menu(label="Replay"):
                dpg.add_menu_item(label="Start", callback=replay_menu_callback)
            with dpg.menu(label="Window"):
                dpg.add_menu_item(label="Debug", callback=debug_log_menu_callback)

        # データダウンロード用ウィンドウ
        with dpg.window(label="Data download", show=False, tag="window_download") as _window:
            dpg.add_text("Target symbol")
            dpg.add_input_text(tag="text_symbol", label="", default_value="BTCUSDT", no_spaces=True, width = 200)
            dpg.add_text("Target date (YYYY-MM-DD)")
            dpg.add_input_text(tag="text_startdate", label="", default_value="2023-04-01", no_spaces=True)
            dpg.add_text("")
            dpg.add_button(tag="button_download", label="Download data", callback=download_button_callback)
        
        # データロード用ウィンドウ
        with dpg.window(label="Data load", width=300, height=200, show=False, tag="window_load") as _window:
            dpg.add_text("Target symbol")
            dpg.add_input_text(tag="text_symbol2", label="", default_value="BTCUSDT", no_spaces=True, width = 200)
            dpg.add_text("Target date (YYYY-MM-DD)")
            dpg.add_input_text(tag="text_startdate2", label="", default_value="2023-04-01", no_spaces=True)
            dpg.add_text("")
            dpg.add_button(tag="button_load", label="Load data", callback=load_button_callback)
        
        # データ再生用ウィンドウ
        with dpg.texture_registry(show=False, tag="texture_registry"):
            dpg.add_raw_texture(width=TEXTURE_WIDTH, height=TEXTURE_HEIGHT, default_value=orderbook_heatmap_texture, format=dpg.mvFormat_Float_rgb, tag="orderbook_heatmap_texture")
        with dpg.window(label="Data replay", width=640, height=480, show=False, tag="window_replay") as _window:
            dpg.add_text("", tag="text_current_time")
            dpg.add_text("", tag="text_mid_price")
            #with dpg.plot(label="Orderbook Plot", tag="plot_ask", width=160, height=160):
            #    dpg.add_plot_legend()
            #    dpg.add_plot_axis(dpg.mvXAxis, label="Volume", tag="ask_plot_xaxis")
            #    dpg.add_plot_axis(dpg.mvYAxis, label="Price", tag="ask_plot_yaxis")
            #    dpg.set_axis_limits("ask_plot_xaxis", -25, 25)
            #    dpg.add_bar_series([], [], parent="ask_plot_yaxis", weight=0.1, horizontal=True, tag="series_ask")
            with dpg.drawlist(width=640, height=480):
                with dpg.draw_layer(tag="layer_image"):
                    dpg.draw_image("orderbook_heatmap_texture", (0, 0), (TEXTURE_WIDTH * 2, TEXTURE_HEIGHT * 2), uv_min=(0, 0), uv_max=(1.0, 1.0))
                with dpg.draw_layer(tag="layer_grid"):
                    dpg.draw_line((0, TEXTURE_WIDTH), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH), color=(255, 255, 255, 64), thickness=1)
                    dpg.draw_line((0, TEXTURE_WIDTH - 40), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH - 40), color=(255, 255, 255, 64), thickness=1)
                    dpg.draw_line((0, TEXTURE_WIDTH + 40), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH + 40), color=(255, 255, 255, 64), thickness=1)
                    dpg.draw_line((0, TEXTURE_WIDTH - 80), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH - 80), color=(255, 255, 255, 64), thickness=1)
                    dpg.draw_line((0, TEXTURE_WIDTH + 80), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH + 80), color=(255, 255, 255, 64), thickness=1)
                    dpg.draw_line((0, TEXTURE_WIDTH - 120), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH - 120), color=(255, 255, 255, 64), thickness=1)
                    dpg.draw_line((0, TEXTURE_WIDTH + 120), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH + 120), color=(255, 255, 255, 64), thickness=1)
                    dpg.draw_line((0, TEXTURE_WIDTH - 160), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH - 160), color=(255, 255, 255, 64), thickness=1)
                    dpg.draw_line((0, TEXTURE_WIDTH + 160), (TEXTURE_WIDTH * 2, TEXTURE_WIDTH + 160), color=(255, 255, 255, 64), thickness=1)
            #dpg.add_image("orderbook_heatmap_texture", width=TEXTURE_WIDTH * 2, height=TEXTURE_HEIGHT * 2, uv_min=(0, 0), uv_max=(1.0, 1.0), border_color=(78, 78, 78), pos=(32, 32 + 16))

        # デバッグ用ウィンドウ
        with dpg.window(label="Debug log", width=600, height=200, show=False, tag="window_debug") as _window:
            dpg.add_child_window(tag="window_log", label="", autosize_y=True, autosize_x=True)
            dpg.add_text("", tag="text_log", label="", parent="window_log")

    except Exception as e:
        print(e)
        raise e
        
    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.set_viewport_vsync(True)
    dpg.start_dearpygui()

def pygui_destroy():
    dpg.destroy_context()

#i = 0
#while dpg.is_dearpygui_running():
#    # insert here any code you would like to run in the render loop
#    # you can manually stop by using stop_dearpygui()
#    i = i + 1
#    dpg.set_value(text_id, f"i={i}, timestamp={orderbook_update_ndarray['timestamp'][i]} side={orderbook_update_ndarray['side'][i]} price={orderbook_update_ndarray['price'][i]} quantity={orderbook_update_ndarray['quantity'][i]}")
#    dpg.render_dearpygui_frame()
#    time.sleep(1/60)
