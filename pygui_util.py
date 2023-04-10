import dearpygui.dearpygui as dpg
from datetime import datetime

global pygui_label_id_map
pygui_label_id_map = {}

def pygui_log(text: str) -> None:
    _log_text = dpg.get_value(pygui_label_id_map["text_log"])
    _log_text += f"{text}\n"
    dpg.set_value(pygui_label_id_map["text_log"], _log_text)

    _y_scroll_max = dpg.get_y_scroll_max(pygui_label_id_map["window_log"])
    dpg.set_y_scroll(pygui_label_id_map["window_log"], _y_scroll_max)

from orderbook_download import download_orderbook, download_trades

def orderbook_download_callback(sender, app_data, user_data):
    symbol = dpg.get_value(pygui_label_id_map["text_symbol"])
    startdate = dpg.get_value(pygui_label_id_map["text_startdate"])
    
    startdatetime = datetime.strptime(startdate, "%Y-%m-%d")

    download_orderbook(symbol, startdatetime)
    download_trades(symbol, startdatetime)

def pygui_init() -> None:
    dpg.create_context()
    dpg.create_viewport(title='Binance Trainer', width=600, height=400)

    with dpg.window(label="root_window", no_resize=True, no_move=True, no_collapse=True, no_close=True, no_title_bar=True, width=600, height=400):
        pygui_label_id_map["text_symbol"] = dpg.add_input_text(label="Symbol", default_value="BTCUSDT", no_spaces=True)
        pygui_label_id_map["text_startdate"] = dpg.add_input_text(label="Target date", default_value="2023-04-01", no_spaces=True)
        pygui_label_id_map["button_download"] = dpg.add_button(label="Download orderbook data", callback=orderbook_download_callback)

        pygui_label_id_map["window_log"] = dpg.add_child_window(label="window_log", height=200, autosize_x=True)
        pygui_label_id_map["text_log"] = dpg.add_text("", label="text_log", parent=pygui_label_id_map["window_log"])
        
    dpg.setup_dearpygui()
    dpg.show_viewport()
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
