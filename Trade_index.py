import csv
#计算
import numpy as np
import pandas as pd
import talib
from pylab import mpl
from talib import MA_Type
#文本控制
import csv
#时间控制
import time, datetime
#线程池
from concurrent.futures import ThreadPoolExecutor



class createIndex:
    def __init__(self, file,interval):
        self.file = file
        self.interval = int(interval / 60000)  #毫秒转分钟
    def changeData(self,full_data,loop,mod=0):
        result_data = []
        for i in range(0, loop):
            Open_time = full_data[i * self.interval][0]
            Open = full_data[i * self.interval][1]
            High = full_data[i * self.interval][2]
            Low = full_data[i * self.interval][3]
            Close = full_data[(i + 1) * self.interval - 1][4]
            Volume = 0
            Close_Time = full_data[(i + 1) * self.interval - 1][6]
            Quote_asset_volume = 0
            Number_of_trades = 0
            Taker_buy_base_asset_volume = 0
            Taker_buy_quote_asset_volume = 0
            for j in range(self.interval):
                # print('j',j)
                # print('High',High)
                # print('Low',Low)
                High = max(High, full_data[i * self.interval + j][2]) #取时间范围内最高价格
                Low = min(Low, full_data[i * self.interval + j][3]) #取时间范围内最低价格
                Volume = Volume + float(full_data[i * self.interval + j][5]) #全量累加
                Quote_asset_volume = Quote_asset_volume + float(full_data[i * self.interval + j][7]) # 全量累加
                Number_of_trades = Number_of_trades + int(full_data[i * self.interval + j][8]) #全量累加
                Taker_buy_base_asset_volume = Taker_buy_base_asset_volume + float(full_data[i * self.interval + j][9]) #量累加
                Taker_buy_quote_asset_volume = Taker_buy_quote_asset_volume + float(
                    full_data[i * self.interval + j][10]) #量累加
            temp_data = [Open_time, Open, High, Low, Close, Volume, Close_Time, Quote_asset_volume, Number_of_trades,
                         Taker_buy_base_asset_volume, Taker_buy_quote_asset_volume]
            result_data.append(temp_data)
        if mod != 0:
            Open_time = full_data[-mod][0]
            Open = full_data[-mod][1]
            High = full_data[-mod][2]
            Low = full_data[-mod][3]
            Close = full_data[-1][4]
            Volume = 0
            Close_Time = full_data[-1][6]
            Quote_asset_volume = 0
            Number_of_trades = 0
            Taker_buy_base_asset_volume = 0
            Taker_buy_quote_asset_volume = 0
            for i in range(mod, 0, -1):
                High = max(High, full_data[-i][2])
                Low = min(Low, full_data[-i][3])
                Volume = Volume + float(full_data[-i][5])
                Quote_asset_volume = Quote_asset_volume + float(full_data[-i][7])
                Number_of_trades = Number_of_trades + int(full_data[-i][8])
                Taker_buy_base_asset_volume = Taker_buy_base_asset_volume + float(full_data[-i][9])
                Taker_buy_quote_asset_volume = Taker_buy_quote_asset_volume + float(full_data[-i][10])
            temp_data = [Open_time, Open, High, Low, Close, Volume, Close_Time, Quote_asset_volume, Number_of_trades,
                         Taker_buy_base_asset_volume, Taker_buy_quote_asset_volume]
            result_data.append(temp_data)
        return result_data


    def get_data(self,num=None):  # 获取当前interval下的各项数据 将1分钟级别按新级别整合成新表格 Sample 14400000 = 4H
        full_data = []
        with open(self.file) as f:
            f_csv = csv.reader(f)
            for i in f_csv:
                full_data.append(i)
        if isinstance(full_data[0][0],str):#判断头部如果是str 就去掉，不是就继续
            full_data = full_data[1:]   #去除header

        loop = int(len(full_data) / self.interval) #算有多少份数据
        mod = int(len(full_data) % self.interval) # 余下的数据
        #数据传递给计算数据函数
        data = self.changeData(full_data,loop,mod)
        return data[0:num]


    def get_boll(self, result_data, sd_parameter=20):  # sd_parameter=boll值几日均值 默认20
        boll_data = []
        for i in range(sd_parameter, len(result_data)):
            close_price_in_array = [x[4] for x in result_data]
            close_price_in_array = close_price_in_array[i - sd_parameter:i]
            close_price_in_array = [float(x) for x in close_price_in_array]
            total = (np.sum(close_price_in_array))
            average_price = total / sd_parameter
            sd = np.sqrt(np.sum((close_price_in_array - average_price) ** 2) / sd_parameter)  # 标准差
            boll_mid = average_price
            boll_up = average_price + 2 * sd
            boll_down = average_price - 2 * sd
            boll_data_temp = [result_data[i-1][0], result_data[i-1][4], boll_up, boll_mid, boll_down] #open_time close_prise boll_up boll_mid boll_down
            boll_data.append(boll_data_temp)
        return boll_data

    def get_macd(self,close=0):
        # close = np.random.random(2)
        # print(close)
        #计算收盘价的一个简单移动平均数SMA:
        # output = talib.SMA(close)
        #
        # #计算布林线，三指数移动平均：
        # upper, middle, lower = talib.BBANDS(close, matype=MA_Type.T3)
        # print('upper:',upper, 'middle',middle, 'lower',lower)
        # # #MACD利用收盘价的短期（常用为12日）指数移动平均线与长期（常用为26日）指数移动平均线之间的聚合与分离状况，对买进、卖出时
        # dif, dem, histogram = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        # print('macd:',dif, dem, histogram )

        #计算收盘价的动量，时间为5：
        # output = talib.MOM(close, timeperiod=5)
        EMA = talib.EMA(np.array(close), timeperiod=7)
        print('11',EMA)
    def analyze_boll(self, result_data, full_data):
        time = 0
        for i in range(len(result_data)):
            for j in range(len(full_data)):
                if result_data[i][0] == full_data[j][0]: #open time 相等
                    for k in range(self.interval):
                        if j+k < len(full_data) and full_data[j+k][4] > result_data[i][2]: #一个interval里收盘价大于或小于boll
                            # print(full_data[0][j] + "is higher than boll")
                            time = time + 1
                        if j+k < len(full_data) and full_data[j+k][4] < result_data[i][4]:
                            # print(full_data[0][j] + "is lower than boll")
                            time = time + 1
        print(time)
        print(time/len(full_data))
    # def readCsv(self):
    #     with open('rdata.csv','r') as csvfile:
    #         files = csv.reader(csvfile)
    #         for row in files:
    #             print(row[0])
    #             timeArray = time.localtime(int(row[0])/1000)
    #             otherStyleTime = time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(int(x[0])/1000))
    #             print(otherStyleTime)


if __name__ == '__main__':
    bn = createIndex('test123.csv',86400000)
    closeList = [float(x[4]) for x in bn.get_data(100)]
    # date  = [time.strftime("%Y--%m--%d %H:%M:%S", time.localtime(int(x[0])/1000)) for x in bn.get_data(100)]
    # print(len(date))
    close = np.array(closeList)
    N = [5, 30, 120, 250]
    for i in N:
        pass
    re = bn.get_macd(close)

    # result_data, full_data = bn.get_data()
    # print(result_data,'\n',full_data)
    # boll_data = bn.get_boll(result_data)
    # with open('test3.csv', 'w+', encoding='UTF-8') as f:
    #     f_csv = csv.writer(f)
    #     f_csv.writerows(boll_data)
    # bn.analyze_boll(result_data, full_data)
