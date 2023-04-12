from datetime import datetime
import re
import dearpygui.dearpygui as dpg

global pygui_label_id_map
pygui_label_id_map = {}

def pygui_log(text: str) -> None:
    _log_text = dpg.get_value("text_log")
    _log_text += f"{text}\n"
    dpg.set_value("text_log", _log_text)

    _y_scroll_max = dpg.get_y_scroll_max("window_log")
    dpg.set_y_scroll("window_log", _y_scroll_max)

from data_download import download_trades_orderbook
from data_replay import load_trades_orderbook

# ダウンロードメニューを押したときのコールバック関数
def download_menu_callback(sender, app_data, user_data):
    _show = dpg.get_item_configuration("window_download")["show"]
    dpg.configure_item("window_download", show= not _show)

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
    dpg.configure_item("window_load", show= not _show)

# ロードボタンを押した時のコールバック関数
def load_button_callback(sender, app_data, user_data):
    _symbol = dpg.get_value("text_symbol2").upper()
    _startdate = dpg.get_value("text_startdate2")
    
    _startdatetime = datetime.strptime(_startdate, "%Y-%m-%d")

    dpg.configure_item("button_load", enabled=False)
    load_trades_orderbook(_symbol, _startdatetime)

def pygui_init() -> None:
    dpg.create_context()
    dpg.create_viewport(title='Binance Trainer', width=640, height=480)

    try:
        with dpg.viewport_menu_bar():
            with dpg.menu(label="Data"):
                dpg.add_menu_item(label="Load", callback=load_menu_callback)
                dpg.add_menu_item(label="Download", callback=download_menu_callback)

        # データダウンロード用ウィンドウ
        with dpg.window(label="Data download", width=600, height=400, show=False, tag="window_download") as _window:
            dpg.add_text("Target symbol")
            dpg.add_input_text(tag="text_symbol", label="", default_value="BTCUSDT", no_spaces=True, width = 200)
            dpg.add_text("Target date (YYYY-MM-DD)")
            dpg.add_input_text(tag="text_startdate", label="", default_value="2023-04-01", no_spaces=True)
            dpg.add_text("")
            dpg.add_button(tag="button_download", label="Download data", callback=download_button_callback)
            dpg.add_text("Download log")
            dpg.add_child_window(tag="window_log", label="", autosize_y=True, autosize_x=True)
            dpg.add_text("", tag="text_log", label="", parent="window_log")
        
        # データロード用ウィンドウ
        with dpg.window(label="Data load", width=600, height=400, show=False, tag="window_load") as _window:
            dpg.add_text("Target symbol")
            dpg.add_input_text(tag="text_symbol2", label="", default_value="BTCUSDT", no_spaces=True, width = 200)
            dpg.add_text("Target date (YYYY-MM-DD)")
            dpg.add_input_text(tag="text_startdate2", label="", default_value="2023-04-01", no_spaces=True)
            dpg.add_text("")
            dpg.add_button(tag="button_load", label="Load data", callback=load_button_callback)

            with dpg.plot(label="Ask Plot", tag="plot_ask", width=-1, height=-1):
                dpg.add_plot_legend()
                dpg.add_plot_axis(dpg.mvXAxis, label="Volume", tag="ask_plot_xaxis")
                dpg.add_plot_axis(dpg.mvYAxis, label="Price", tag="ask_plot_yaxis")
                dpg.add_bar_series([1, 2, 3], [2, 4, 6], parent="ask_plot_yaxis", horizontal=True, tag="series_ask")

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
