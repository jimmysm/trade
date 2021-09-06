import requests
import time
import csv
import ccxt
import pandas as pd


class Binance:
    __convert_interval_microsecond = {
        "1m": 60000,
        "3m": 180000,
        "5m": 300000,
        "15m": 900000,
        "30m": 1800000,
        "1h": 3600000,
        "2h": 7200000,
        "4h": 14400000,
        "6h": 21600000,
        "8h": 28800000,
        "12h": 43200000,
        "1d": 86400000,
        "3d": 259200000,
        "1w": 604800000,
        "1M": 2419200000  # 1M = 4w for calculate
    }

    __method = {
        "get_klines": "v3/klines"
    }

    def __init__(self, symbol, interval):
        self.url = r"https://api.binance.com/api/"
        self.symbol = symbol
        self.interval = interval

    def get_loop_parameter(self, start_time, end_time, limit):
        interval_time = self.__convert_interval_microsecond[self.interval]
        path = "v3/klines"
        par = {
            "symbol": self.symbol,
            "interval": self.interval,
            "startTime": str(start_time),
            "endTime": str(end_time),
            "limit": 1
        }
        res = requests.get(self.url + path, params=par)
        data = res.json()
        actual_start_time = data[0][0]
        loop = (end_time - actual_start_time)//(interval_time*limit) + 1
        return loop, actual_start_time

    def klines_write_csv(self, start_time, end_time, limit):
        """1499040000000,      // 开盘时间
          "0.01634790",       // 开盘价
          "0.80000000",       // 最高价
          "0.01575800",       // 最低价
          "0.01577100",       // 收盘价(当前K线未结束的即为最新价)
          "148976.11427815",  // 成交量
          1499644799999,      // 收盘时间
          "2434.19055334",    // 成交额
          308,                // 成交笔数
          "1756.87402397",    // 主动买入成交量
          "28.46694368",      // 主动买入成交额
          "17928899.62484339" // 请忽略该参数"""
        if limit > 1000:
            limit = 1000 # Max Limit in Binance is 1000
        path = "v3/klines"
        loop, actual_start_time = self.get_loop_parameter(start_time, end_time, limit)
        headers = ["Open_Time", "Open", "High", "Low", "Close", "Volume", "Close_Time", "Quote_asset_volume", "Number_of_trades", "Taker_buy_base_asset_volume", "Taker_buy_quote_asset_volume", "Ignored"]
        with open('test.csv', 'w+', encoding='UTF-8') as f:
            f_csv = csv.writer(f)
            f_csv.writerow(headers)
            for i in range(loop):
                start_time_value = actual_start_time + i*self.__convert_interval_microsecond[self.interval]*limit

                par = {
                    "symbol": self.symbol,
                    "interval": self.interval,
                    "startTime": str(start_time_value),
                    "endTime": str(end_time),
                    "limit": limit
                }
                res = requests.get(self.url + path, params=par)
                data = res.json()
                f_csv.writerows(data)
        df = pd.read_csv("test.csv")
        del df["Ignored"]
        df.to_csv("test.csv", index=None)


if __name__ == '__main__':
    bn = Binance('BTCUSDT', '1m')
    bn.klines_write_csv(0, int(time.time() * 1000), 1000)




