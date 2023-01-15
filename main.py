import json
import os
import platform
import polars as pl
import numpy as np
from urllib.request import urlopen
from datetime import datetime, timedelta
import time
from os.path import expanduser
import requests


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
            self.ex_path = self.app_path + "/exchanges.json"
        elif self.platform == "Windows":
            self.app_path = self.user_path + "\AppData\Local\SolWatcher"
            self.df_path = self.app_path + "\dataframe.json"
            self.df_path = self.app_path + "\exchanges.json"
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
        self.exchange_rates_last_update: datetime = datetime(1990, 1, 1)

    def init_df(self):
        """ Load existing data or create new DataFrame """

        if os.path.exists(self.df_path):
            print("Loading existing data...")
            self.load_dataframe()
        else:
            print("Creating new DataFrame (last 30 days data) ...")
            time_end = self.get_current_utc_time()
            time_start = time_end - timedelta(days=30)

            prices_list, timestamps_list = self.download_hist_data(coin='solana', fiat='usd', time_s=time_start, time_e=time_end)

            # create dataframe
            self.df = pl.DataFrame({'price_usd': prices_list,
                                    'utc_time': timestamps_list})

            self.save_dataframe()

    def init_exchanges(self):
        """ Load existing exchange data or load from internet """

        if os.path.exists(self.ex_path):
            self.load_exchange_data()
        else:
            self.get_exchange_data()
            self.save_exchange_data()

    @staticmethod
    def get_current_utc_time() -> datetime:
        """ Get current UTC time """

        return datetime.utcnow()

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

        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range?vs_currency={fiat}&from={start}&to={end}"

        return url

    def download_hist_data(self, coin: str = 'solana', fiat: str = 'usd', time_s: datetime = None, time_e: datetime = None) -> (list, list):
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

    def get_time_area_extremes(self, start: datetime, end: datetime, cut_area: bool) -> (datetime, datetime):
        """ Get min and max timestamps in selected time area of DataFrame

        Args:
            start:      start of selected area
            end:        end of selected area
            cut_area:   True = cut DataFrame with start&end | False = use whole DataFrame

        Out:
            oldest:     oldest record in selected dataframe
            latest:     latest record in selected dataframe
        """
        if cut_area:
            df_cut = self.df.filter(pl.col('utc_time').is_between(start, end, closed="both"),)
        else:
            df_cut = self.df

        latest = df_cut['utc_time'].max()
        oldest = df_cut['utc_time'].min()

        return oldest, latest

    def get_time_extremes(self):
        """ Find oldest and latest record in whole DataFrame """
        self.oldest_record, self.latest_record = self.get_time_area_extremes(datetime.now(), datetime.now(), False)
        print(f"Got data between {self.oldest_record} and {self.latest_record}")

    def get_time_diff(self) -> timedelta:
        """ Compute time difference between current time and latest record in DataFrame

        Out:
            time_diff:  time difference between current time and latest record
        """
        current_time = self.get_current_utc_time()
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
            current_time = self.get_current_utc_time()
            start_time = current_time - timedelta(minutes=timediff_minutes)
            print(f"Fetching data from {start_time} to {current_time} [UTC]")
            new_prices, new_times = self.download_hist_data('solana', 'usd', start_time, current_time)
            new_df = pl.DataFrame({'price_usd': new_prices,
                                   'utc_time': new_times})
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
        """ Load current exchange rates from exchangerate.host API """

        url = 'https://api.exchangerate.host/latest?base=USD'
        response = requests.get(url)
        data = response.json()
        self.exchange_rates = data['rates']
        self.exchange_rates_last_update = self.get_current_utc_time().replace(microsecond=0)

    def load_exchange_data(self):
        """ Load exchange data from local file and update them if they are older than 1 hour """

        with open(self.ex_path, 'r') as outfile:
            data = json.load(outfile)
            data_timestamp: int = data['date']
            data_time = datetime(1970, 1, 1) + timedelta(seconds=data_timestamp) + timedelta(hours=1)
            current_time = self.get_current_utc_time()
            delta = current_time - data_time
            if delta > timedelta(hours=1):
                self.get_exchange_data()
                self.save_exchange_data()
            else:
                self.exchange_rates_last_update = data_time
                self.exchange_rates = data['rates']

    def save_exchange_data(self):
        """ Save exchange rates to local json file """

        ex_dict = {'date': int(time.mktime(self.exchange_rates_last_update.timetuple())),
                   'rates': self.exchange_rates}
        with open(self.ex_path, 'w') as outfile:
            json.dump(ex_dict, outfile)

    def print_move_stats(self, delta: timedelta, end_time=None, fiat: str = 'usd'):
        """
        Print stats for given time delta (current_time - delta  :  current_time)

        Args:
            delta:      difference between current_time and start of examined interval
            end_time:   replace current_time with that if not None
            fiat:       currency to display price in
        """
        fiat = fiat.upper()

        if end_time is not None:
            current_time = end_time
        else:
            current_time = self.get_current_utc_time()
        back_time = current_time - delta
        old_time, latest_time = self.get_time_area_extremes(back_time, current_time, True)
        old_price_row = self.df.filter(pl.col('utc_time') == old_time,)
        new_price_row = self.df.filter(pl.col('utc_time') == latest_time,)
        old_price = old_price_row['price_usd'][0]
        new_price = new_price_row['price_usd'][0]

        # convert to different fiat currency
        if fiat != 'USD' and fiat in self.exchange_rates.keys():
            print(f"1 USD = {self.exchange_rates[fiat]} {fiat}")
            old_price *= self.exchange_rates[fiat]
            new_price *= self.exchange_rates[fiat]

        fiat = fiat.lower()

        print(f"Investigating last {delta}")
        print(f"  - current price: %.2f {fiat} -> %.2f {fiat}" % (old_price, new_price))
        price_diff = (new_price / old_price - 1) * 100
        print("Price change is: %.2f %%" % price_diff)


if __name__ == '__main__':
    watcher = SolWatcher()
    watcher.init_df()
    watcher.init_exchanges()
    print("Description:")
    print(watcher.df.describe())
    print("")
    watcher.print_move_stats(timedelta(days=1), fiat='czk')
    print("")
    watcher.print_move_stats(timedelta(days=7), fiat='czk')
    print("")
    watcher.print_move_stats(timedelta(days=30), fiat='czk')

    watcher.get_exchange_data()
