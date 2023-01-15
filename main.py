import json
import os
import platform
import polars as pl
import numpy as np
from urllib.request import urlopen
from datetime import datetime, timedelta
import time
from os.path import expanduser


class SolWatcher:

    def __init__(self):
        """
        Prepare system paths and variables
        """

        """ Paths """

        self.user_path: str = expanduser("~")
        self.app_path: str = self.user_path
        self.platform = platform.system()
        if self.platform == "Linux":
            self.app_path = self.user_path + "/.local/share/SolWatcher"
            self.df_path = self.app_path + "/dataframe.json"
        elif self.platform == "Windows":
            self.app_path = self.user_path + "\AppData\Local\SolWatcher"
            self.df_path = self.app_path + "\dataframe.json"

        if not os.path.exists(self.app_path):
            os.makedirs(self.app_path)

        """ Dataframe """

        self.df = None
        self.oldest_record = None
        self.latest_record = None

    def init_df(self):
        """ Load existing data or create new DataFrame """

        if os.path.exists(self.df_path):
            print("Loading existing data...")
            self.load_dataframe()
        else:
            print("Creating new DataFrame (last 30 days data) ...")
            time_end = self.get_current_utc_time()
            time_start = time_end - timedelta(days=30)

            prices_list, timestamps_list = self.download_hist_data(coin='solana', fiat='czk', time_s=time_start, time_e=time_end)

            # create dataframe
            self.df = pl.DataFrame({'price': prices_list,
                                    'utc_time': timestamps_list})

            self.save_dataframe()

    @staticmethod
    def get_current_utc_time() -> datetime:
        return datetime.utcnow()

    @staticmethod
    def prepare_api_url(coin: str, fiat: str, start: str, end: str) -> str:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range?vs_currency={fiat}&from={start}&to={end}"

        return url

    def download_hist_data(self, coin: str = 'solana', fiat: str = 'czk', time_s: datetime = None, time_e: datetime = None) -> (list, list):
        """
        Download historical data of selected coin from given interval

        Args:
            coin:       name of cryptocurrency
            fiat:       name of currency to display price in
            time_s:     interval start datetime
            time_e:     interval end datetime

        Output:

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

    def get_time_area_extremes(self, start: datetime, end: datetime, cut_area: bool) -> (datetime, datetime):
        if cut_area:
            df_cut = self.df.filter(pl.col('utc_time').is_between(start, end),)
        else:
            df_cut = self.df

        latest = 0
        oldest = 0
        for i in range(len(df_cut['utc_time'])):
            record_datetime = df_cut['utc_time'][i]
            if i == 0:
                latest = record_datetime
                oldest = record_datetime
            else:
                if record_datetime > latest:
                    latest = record_datetime
                if record_datetime < oldest:
                    oldest = record_datetime
        return oldest, latest

    def get_time_extremes(self):
        self.oldest_record, self.latest_record = self.get_time_area_extremes(datetime.now(), datetime.now(), False)
        print(f"Got data between {self.oldest_record} and {self.latest_record}")

    def get_time_diff(self) -> timedelta:
        current_time = self.get_current_utc_time()
        self.get_time_extremes()
        time_diff = current_time - self.latest_record

        return time_diff

    def refresh_dataframe(self, min_diff: int):
        """ Download the latest price data if time difference is greater than min_diff """
        timediff_minutes = int(self.get_time_diff().total_seconds()) // 60

        # update if time difference is greater than min_diff minutes
        if timediff_minutes > min_diff:
            current_time = self.get_current_utc_time()
            start_time = current_time - timedelta(minutes=timediff_minutes)
            print(f"Fetching data from {start_time} to {current_time} [UTC]")
            new_prices, new_times = self.download_hist_data('solana', 'czk', start_time, current_time)
            new_df = pl.DataFrame({'price': new_prices,
                                   'utc_time': new_times})
            self.df = pl.concat([self.df, new_df], how="vertical")
            self.save_dataframe()
        else:
            print(f"Data are up-to-date. {timediff_minutes} minutes differance")

    def load_dataframe(self):
        self.df = pl.read_json(self.df_path)
        self.refresh_dataframe(5)

    def save_dataframe(self):
        self.df.write_json(self.df_path)

    def print_move_stats(self, delta: timedelta):
        current_time = self.get_current_utc_time()
        back_time = current_time - delta
        old_time, latest_time = self.get_time_area_extremes(back_time, current_time, True)
        old_price_row = self.df.filter(pl.col('utc_time') == old_time,)
        new_price_row = self.df.filter(pl.col('utc_time') == latest_time,)
        old_price = old_price_row['price'][0]
        new_price = new_price_row['price'][0]
        print(f"Investigating last {delta}")
        print("  - current price: %.2f czk" % new_price)
        print("  - back then price: %.2f czk" % old_price)
        price_diff = (new_price / old_price - 1) * 100
        print("Price change is: %.2f %%" % price_diff)


if __name__ == '__main__':
    watcher = SolWatcher()
    watcher.init_df()
    print("Description:")
    print(watcher.df.describe())
    print("")
    watcher.print_move_stats(timedelta(days=1))
    print("")
    watcher.print_move_stats(timedelta(days=7))
    print("")
    watcher.print_move_stats(timedelta(days=30))
