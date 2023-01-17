import json
import os
import platform
import polars as pl
import numpy as np
import pyqtgraph as pg
from urllib.request import urlopen
from datetime import datetime, timedelta

from PyQt6.QtGui import QPalette
from PyQt6.QtWidgets import *
from PyQt6 import QtWidgets, QtGui
import time
from os.path import expanduser
import requests
import sys

from gui import form

pg.setConfigOptions(background='w', foreground='k', antialias=True)


class SolWatcher(form.Ui_MainWindow):

    def __init__(self):
        """
        Prepare system paths and variables
        """

        self.MainWindow = None

        """ Paths """

        self.sol_launch = datetime(2020, 4, 11)

        self.user_path: str = expanduser("~")
        self.app_path: str = self.user_path
        self.platform = platform.system()
        if self.platform == "Linux":
            self.app_path = self.user_path + "/.local/share/SolWatcher"
            self.df_path = self.app_path + "/dataframe.json"
            self.ex_path = self.app_path + "/exchanges.json"
        elif self.platform == "Windows":
            self.app_path = self.user_path + "\AppData\Local\SolWatcher"
            self.df_path = self.app_path + "\dataframe.json"
            self.ex_path = self.app_path + "\exchanges.json"
        else:
            print("Unknown platform!")
            exit(100)

        if not os.path.exists(self.app_path):
            os.makedirs(self.app_path)

        """ Dataframe """

        self.df = None
        self.oldest_record = None
        self.latest_record = None

        self.exchange_rates = None
        self.exchange_rates_last_update: datetime = datetime(1970, 1, 1)

        self.display_fiat: str = "czk"

        # gui
        self.gui = None

        self.change_24h: float = 0
        self.change_7d: float = 0
        self.change_30d: float = 0
        self.change_1y: float = 0
        self.change_at: float = 0

        self.current_usd_price: float = 0

    def init(self, main_window):
        """ Initialize PyQt6 Window """
        self.MainWindow = main_window
        self.MainWindow.setWindowTitle("SolWatcher")
        self.MainWindow.setWindowIcon(QtGui.QIcon('data/logo_300.png'))

    def init_df(self):
        """ Load existing data or create new DataFrame """

        if os.path.exists(self.df_path):
            print("Loading existing data...")
            self.load_dataframe()
        else:
            print("Creating new DataFrame (downloading all-time data) ...")

            # all-time data (low precision)
            time_end = self.get_current_time() - timedelta(days=30)
            time_start = self.sol_launch
            prices_list, timestamps_list = self.download_hist_data(coin='solana', fiat='usd', time_s=time_start,
                                                                   time_e=time_end)

            # 30 days data (higher precision)
            time_end = self.get_current_time() - timedelta(days=1)
            time_start = time_end - timedelta(days=30)
            prices_30_list, timestamps_30_list = self.download_hist_data(coin='solana', fiat='usd', time_s=time_start,
                                                                         time_e=time_end)
            prices_list, timestamps_list = self.add_new_records(prices_list, timestamps_list,
                                                                prices_30_list, timestamps_30_list)

            # 1 day data (even higher precision)
            time_end = self.get_current_time()
            time_start = time_end - timedelta(days=1)
            prices_24_list, timestamps_24_list = self.download_hist_data(coin='solana', fiat='usd', time_s=time_start,
                                                                         time_e=time_end)
            prices_list, timestamps_list = self.add_new_records(prices_list, timestamps_list,
                                                                prices_24_list, timestamps_24_list)

            # create dataframe
            self.df = pl.DataFrame({'price_usd': prices_list,
                                    'time': timestamps_list})

            # sort DataFrame
            self.df = self.df.sort('time')

            self.save_dataframe()

    def init_exchanges(self):
        """ Load existing exchange data or load from internet """

        if os.path.exists(self.ex_path):
            print("Loading existing exchange rates...")
            self.load_exchange_data()
        else:
            print("Local exchange rates data not found...")
            self.get_exchange_data()
            self.save_exchange_data()

    def init_gui(self):
        """ Initialize gui slots and plot default data """

        self.connect_slots()

        print("\nDescription:")
        print(self.df.describe())
        print("")
        self.print_move_stats(timedelta(days=1), fiat='czk', plot=self.graphicsView_24h)
        print("")
        self.print_move_stats(timedelta(days=7), fiat='czk', plot=self.graphicsView_7d)
        print("")
        self.print_move_stats(timedelta(days=30), fiat='czk', plot=self.graphicsView_30d)
        print("")
        self.print_move_stats(timedelta(days=365), fiat='czk', plot=self.graphicsView_1y)
        print("")
        self.print_move_stats(self.get_current_time() - self.sol_launch, fiat='czk', plot=self.graphicsView_at)

        # default page (24 Hours)
        self.tabWidget.setCurrentIndex(0)

    def connect_slots(self):
        """ Connect gui actions with functions """

        self.tabWidget.currentChanged.connect(self.time_tab_changed)

    @staticmethod
    def add_new_records(prices: list, timestamps: list, prices_new: list, timestamps_new: list) -> (list, list):
        """ Add just new price records to existing lists """

        for i in range(len(timestamps_new)):
            if timestamps_new[i] not in timestamps:
                prices.append(prices_new[i])
                timestamps.append(timestamps_new[i])

        return prices, timestamps

    @staticmethod
    def get_current_time() -> datetime:
        """ Get current time """

        return datetime.now()

    @staticmethod
    def prepare_api_url(coin: str, fiat: str, start: str, end: str) -> str:
        """
        Prepare url for CoinGecko API request

        Args:
            coin:   name of cryptocurrency
            fiat:   name of currency to display price in
            start:  unix-like start timestamp
            end:    unix-like end timestamp

        Out:
            url:    request url
        """

        url = "https://api.coingecko.com/api/v3/coins/" + \
              f"{coin}/market_chart/range?vs_currency={fiat}&from={start}&to={end}"

        return url

    def download_hist_data(self, coin: str = 'solana', fiat: str = 'usd',
                           time_s: datetime = None, time_e: datetime = None) -> (list, list):
        """
        Download historical data of selected coin from given interval

        Args:
            coin:       name of cryptocurrency
            fiat:       name of currency to display price in
            time_s:     interval start datetime
            time_e:     interval end datetime

        Out:
            prices:         list of prices
            timestamps:     list of datetime times
        """

        delta = time_e - time_s

        print(f"Downloading data between {time_s} and {time_e}")

        time_s = time_s.replace(microsecond=0)
        time_e = time_e.replace(microsecond=0)

        # unix timestamp (eg 1392577232)
        start = int(time.mktime(time_s.timetuple()))
        end = int(time.mktime(time_e.timetuple()))

        url = self.prepare_api_url(coin, fiat, str(start), str(end))
        response = urlopen(url)
        prices = list(np.array(json.loads(response.read())['prices'])[:, 1])
        n_records = len(prices)
        delta = delta / n_records

        timestamps = []
        for i in range(1, n_records + 1):
            timestamps.append((time_s + i * delta).replace(microsecond=0))

        return prices, timestamps

    def plot_local_extremes(self, plot: pg.GraphicsView, times: list, prices: list):
        highest_price = np.max(prices)
        lowest_price = np.min(prices)
        highest_price_time = times[prices.index(highest_price)]
        lowest_price_time = times[prices.index(lowest_price)]

        scatter = pg.ScatterPlotItem(size=8, brush=(0, 0, 0), symbol='t')
        x_data = [lowest_price_time, highest_price_time]
        y_data = [lowest_price, highest_price]
        scatter.setData(x_data, y_data)
        plot.addItem(scatter)

    def plot_time_area(self, plot: pg.GraphicsView, df: pl.DataFrame):
        """ Plot given DataFrame to given PyQtGraph plot """

        prices = list(df['price_usd'])
        timestamps = df['time']

        fiat = self.display_fiat.upper()
        if fiat != 'USD':
            if self.exchange_rates is None:
                self.get_exchange_data()
            if fiat in self.exchange_rates.keys():
                exchange_rate = self.exchange_rates[fiat]
                for i in range(len(prices)):
                    prices[i] *= exchange_rate

        x_axis = []

        for i in range(len(timestamps)):
            x_axis.append(int(time.mktime(timestamps[i].timetuple())))

        min_time = np.min(x_axis)
        max_time = np.max(x_axis)

        min_price = np.min(prices)
        max_price = np.max(prices)
        price_diff = max_price - min_price
        plot_max = max_price + price_diff*0.05
        fill_lvl = min_price - price_diff*0.25

        bottom_axis = pg.DateAxisItem()

        plot.getPlotItem().showGrid(x=True, y=True, alpha=0.4)

        plot.clear()
        plot.setAxisItems({'bottom': bottom_axis})
        plot.plot(x=x_axis, y=prices, pen=pg.mkPen(width=2, color='#9945FF'), fillLevel=fill_lvl, brush=(20, 241, 149, 100))
        plot.getPlotItem().setLabel('left', text=f"Price [{fiat}]")

        self.plot_local_extremes(plot, x_axis, prices)

        plot.getPlotItem().setLimits(xMin=min_time, xMax=max_time, yMin=fill_lvl, yMax=plot_max)

    def get_time_area_extremes(self, start: datetime, end: datetime, cut_area: bool,
                               plot: pg.GraphicsView = None) -> (datetime, datetime):
        """ Get min and max timestamps in selected time area of DataFrame

        Args:
            start:      start of selected area
            end:        end of selected area
            cut_area:   True = cut DataFrame with start&end | False = use whole DataFrame
            plot:       Plot to visualize area in (None if no visualization)

        Out:
            oldest:     oldest record in selected dataframe
            latest:     latest record in selected dataframe
        """
        if cut_area:
            df_cut = self.df.filter(pl.col('time').is_between(start, end, closed="both"),)
        else:
            df_cut = self.df

        if plot is not None:
            self.plot_time_area(plot, df_cut)

        latest = df_cut['time'].max()
        oldest = df_cut['time'].min()

        return oldest, latest

    def get_time_extremes(self):
        """ Find oldest and latest record in whole DataFrame """

        self.oldest_record, self.latest_record = self.get_time_area_extremes(datetime.now(), datetime.now(), False,
                                                                             self.graphicsView_at)
        print(f"Got data between {self.oldest_record} and {self.latest_record}")

    def get_time_diff(self) -> timedelta:
        """ Compute time difference between current time and latest record in DataFrame

        Out:
            time_diff:  time difference between current time and latest record
        """

        current_time = self.get_current_time()
        self.get_time_extremes()
        time_diff = current_time - self.latest_record

        return time_diff

    def refresh_dataframe(self, min_diff: int):
        """
        Download the latest price data if time difference is greater than min_diff

        Args:
            min_diff:   minimal time difference threshold
        """

        timediff_minutes = int(self.get_time_diff().total_seconds()) // 60

        # update if time difference is greater than min_diff minutes
        if timediff_minutes > min_diff:
            current_time = self.get_current_time()
            start_time = current_time - timedelta(minutes=timediff_minutes)
            print(f"Fetching data from {start_time} to {current_time}")
            new_prices, new_times = self.download_hist_data('solana', 'usd', start_time, current_time)
            new_df = pl.DataFrame({'price_usd': new_prices,
                                   'time': new_times})
            self.df = pl.concat([self.df, new_df], how="vertical")
            self.save_dataframe()
        else:
            print(f"Data are up-to-date. {timediff_minutes} minutes differance")

    def load_dataframe(self):
        """ Load DataFrame from local file and update data if they are out-dated """

        self.df = pl.read_json(self.df_path)
        self.refresh_dataframe(5)

    def save_dataframe(self):
        """ Save current DataFrame to local file """

        self.df.write_json(self.df_path)

    def get_exchange_data(self):
        """ Load current exchange rates from ExchangeRate API """

        response = requests.get('https://api.exchangerate.host/latest?base=USD')
        data = response.json()
        self.exchange_rates = data['rates']
        self.exchange_rates_last_update = self.get_current_time().replace(microsecond=0)

    def load_exchange_data(self):
        """ Load exchange data from local file and update them if they are older than 1 hour """

        with open(self.ex_path, 'r') as outfile:
            data = json.load(outfile)
            data_timestamp: int = data['date']
            data_time = datetime(1970, 1, 1) + timedelta(seconds=data_timestamp)
            current_time = self.get_current_time()
            delta = current_time - data_time
            if delta > timedelta(hours=1):
                print(f"Exchange data are outdated ({delta}) -> Downloading new data")
                self.get_exchange_data()
                self.save_exchange_data()
            else:
                print(f"Exchange data are up-to-date ({delta}) -> Using local data")
                self.exchange_rates_last_update = data_time
                self.exchange_rates = data['rates']

    def save_exchange_data(self):
        """ Save exchange rates to local json file """

        ex_dict = {'date': int(time.mktime(self.exchange_rates_last_update.timetuple())),
                   'rates': self.exchange_rates}
        with open(self.ex_path, 'w') as outfile:
            json.dump(ex_dict, outfile)

    def time_tab_changed(self):
        """ change label_change displayed text when switched to different tabWidget page """

        current_idx = self.tabWidget.currentIndex()

        if current_idx == 0:  # 24 Hours
            self.label_change.setText("%.2f %%" % self.change_24h)
        elif current_idx == 1:  # 7 Days
            self.label_change.setText("%.2f %%" % self.change_7d)
        elif current_idx == 2:  # 30 Days
            self.label_change.setText("%.2f %%" % self.change_30d)
        elif current_idx == 3:  # 1 Year
            self.label_change.setText("%.2f %%" % self.change_1y)
        elif current_idx == 4:  # All-Time
            self.label_change.setText("%.2f %%" % self.change_at)

    def print_move_stats(self, delta: timedelta, end_time=None, fiat: str = 'usd', plot: pg.GraphicsView = None):
        """
        Print stats for given time delta (current_time - delta  :  current_time)

        Args:
            delta:      difference between current_time and start of examined interval
            end_time:   replace current_time with that if not None
            fiat:       currency to display price in
            plot:
        """
        fiat = fiat.upper()

        if end_time is not None:
            current_time = end_time
        else:
            current_time = self.get_current_time()
        back_time = current_time - delta
        old_time, latest_time = self.get_time_area_extremes(back_time, current_time, True, plot)
        old_price_row = self.df.filter(pl.col('time') == old_time,)
        new_price_row = self.df.filter(pl.col('time') == latest_time,)
        old_price = old_price_row['price_usd'][0]
        new_price = new_price_row['price_usd'][0]

        if end_time is None:
            self.current_usd_price = new_price

        # convert to different fiat currency
        if fiat != 'USD' and fiat in self.exchange_rates.keys():
            print(f"1 USD = {self.exchange_rates[fiat]} {fiat}")
            old_price *= self.exchange_rates[fiat]
            new_price *= self.exchange_rates[fiat]

        self.label_price.setText(f"%.2f {fiat}" % new_price)

        fiat = fiat.lower()

        print(f"Investigating last {delta}")
        print(f"  - current price: %.2f {fiat} -> %.2f {fiat}" % (old_price, new_price))
        price_diff = (new_price / old_price - 1) * 100
        if end_time is None:
            if delta == timedelta(days=1):
                self.change_24h = price_diff
            elif delta == timedelta(days=7):
                self.change_7d = price_diff
            elif delta == timedelta(days=30):
                self.change_30d = price_diff
            elif delta == timedelta(days=365):
                self.change_1y = price_diff
            elif delta > timedelta(days=400):
                self.change_at = price_diff
        print("Price change is: %.2f %%" % price_diff)


class AppWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.ui = SolWatcher()
        self.ui.setupUi(self)
        self.ui.init(self)
        self.ui.init_df()
        self.ui.init_exchanges()
        self.ui.init_gui()
        # TODO: set colors
        # self.setStyleSheet("background:#999999")


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('GTK')
    w = AppWindow()
    w.show()
    sys.exit(app.exec())
